"""
Модуль для эффективного извлечения объявлений из БД пакетами
"""
import logging
import signal
from datetime import datetime
from typing import AsyncIterator, Dict, Any, List, Optional
from contextlib import asynccontextmanager

import asyncpg
import psutil
from tqdm.asyncio import tqdm

from ..config import (
    DATABASE_HOST,
    DATABASE_PORT,
    DATABASE_NAME,
    DATABASE_USER,
    DATABASE_PASSWORD,
    BATCH_SIZE,
)

logger = logging.getLogger(__name__)


class DataRetriever:
    """
    Класс для батч-загрузки данных из БД
    """
    
    def __init__(self, batch_size: int = BATCH_SIZE):
        """
        Инициализация загрузчика данных
        
        Args:
            batch_size: Размер пакета для загрузки
        """
        self.batch_size = batch_size
        self.connection = None
        self.total_processed = 0
        self._process = psutil.Process()  # Для мониторинга памяти
        self.memory_checks = []  # История проверок памяти
        self.is_running = True
        
        # Регистрация обработчиков сигналов для graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
    def _handle_shutdown(self, signum, frame):
        """
        Обработчик сигналов для корректного завершения
        """
        logger.info(f"Получен сигнал {signum}, начинаем graceful shutdown...")
        self.is_running = False
        
    def _log_memory_usage(self) -> None:
        """
        Логирование использования памяти процессом
        """
        memory_info = self._process.memory_info()
        memory_mb = memory_info.rss / 1024 / 1024  # RSS в мегабайтах  
        memory_percent = self._process.memory_percent()
        
        self.memory_checks.append({
            'processed': self.total_processed,
            'memory_mb': memory_mb,
            'memory_percent': memory_percent,
            'timestamp': datetime.now()
        })
        
        logger.info(
            f"Память: {memory_mb:.2f} MB ({memory_percent:.1f}%), "
            f"обработано: {self.total_processed} записей"
        )
        
    async def connect(self) -> None:
        """
        Создание соединения с БД
        """
        try:
            logger.info(f"Подключение к БД {DATABASE_NAME}@{DATABASE_HOST}:{DATABASE_PORT}")
            
            # Создаём соединение
            self.connection = await asyncpg.connect(
                host=DATABASE_HOST,
                port=DATABASE_PORT,
                database=DATABASE_NAME,
                user=DATABASE_USER,
                password=DATABASE_PASSWORD,
            )
            
            # Проверка соединения
            version = await self.connection.fetchval('SELECT version()')
            logger.info(f"Подключено к PostgreSQL")
                
        except Exception as e:
            logger.error(f"Ошибка подключения к БД: {e}")
            raise
            
    async def disconnect(self) -> None:
        """
        Закрытие соединения
        """
        if self.connection:
            await self.connection.close()
            logger.info("Соединение закрыто")
            
    async def get_total_count(
        self,
        filter_date: Optional[datetime] = None,
        processed_status: Optional[bool] = None
    ) -> int:
        """
        Получение общего количества записей для обработки
        
        Args:
            filter_date: Фильтр по дате создания (записи после этой даты)
            processed_status: Фильтр по статусу обработки
            
        Returns:
            Количество записей
        """
        if not self.connection:
            await self.connect()
            
        query = """
            SELECT COUNT(*) 
            FROM public.special_model_data s
            INNER JOIN public.text_model_data t ON s.id = t.id
            WHERE 1=1
        """
        
        params = []
        param_num = 1
        
        # Добавляем фильтры если нужно  
        if filter_date:
            query += f" AND s.created_at > ${param_num}"
            params.append(filter_date)
            param_num += 1
            
        if processed_status is not None:
            # Проверяем существование записи в таблице результатов
            if processed_status:
                query += " AND EXISTS (SELECT 1 FROM public.avito_parts_resolved r WHERE r.ad_id = s.id)"
            else:
                query += " AND NOT EXISTS (SELECT 1 FROM public.avito_parts_resolved r WHERE r.ad_id = s.id)"
        
        count = await self.connection.fetchval(query, *params)
        logger.info(f"Найдено {count} записей для обработки")
        return count
        
    async def fetch_batch_data(
        self,
        filter_date: Optional[datetime] = None,
        processed_status: Optional[bool] = None,
        show_progress: bool = True
    ) -> AsyncIterator[List[Dict[str, Any]]]:
        """
        Асинхронный генератор для потоковой загрузки данных пакетами
        
        Args:
            filter_date: Фильтр по дате создания
            processed_status: Фильтр по статусу обработки
            show_progress: Показывать прогресс-бар
        
        Yields:
            Пакеты записей для обработки
        """
        if not self.connection:
            await self.connect()
            
        # Получаем общее количество для прогресс-бара  
        progress_bar = None
        if show_progress:
            total_count = await self.get_total_count(filter_date, processed_status)
            progress_bar = tqdm(
                total=total_count,
                desc="Загрузка данных",
                unit="записей"
            )
        
        # SQL запрос с JOIN таблиц и склейкой полей
        query = """
            SELECT 
                s.id as ad_id,
                s.title,
                t.description,
                t.characteristic,
                CONCAT(
                    COALESCE(s.title, ''), ' ',
                    COALESCE(t.description, ''), ' ',
                    COALESCE(t.characteristic, '')
                ) as text_raw
            FROM public.special_model_data s
            INNER JOIN public.text_model_data t ON s.id = t.id
            WHERE 1=1
        """
        
        params = []
        param_num = 1
        
        # Добавляем фильтры
        if filter_date:
            query += f" AND s.created_at > ${param_num}"
            params.append(filter_date)
            param_num += 1
            
        if processed_status is not None:
            if processed_status:
                query += " AND EXISTS (SELECT 1 FROM public.avito_parts_resolved r WHERE r.ad_id = s.id)"
            else:
                query += " AND NOT EXISTS (SELECT 1 FROM public.avito_parts_resolved r WHERE r.ad_id = s.id)"
        
        query += " ORDER BY s.id"
        
        try:
            # Что: начинаем транзакцию для курсора
            # Зачем: asyncpg требует транзакцию для работы с курсором
            async with self.connection.transaction():
                # Используем курсор для потоковой загрузки данных
                cursor = self.connection.cursor(query, *params)
                batch = []
                async for row in cursor:
                    if not self.is_running:
                        logger.info("Получен сигнал остановки, прерываем загрузку...")
                        break
                        
                    # Преобразуем Record в словарь
                    record = dict(row)
                    batch.append(record)
                    self.total_processed += 1
                    
                    # Когда набрали полный батч - отдаём его
                    if len(batch) >= self.batch_size:
                        yield batch
                        # Обновляем прогресс
                        if progress_bar:
                            progress_bar.update(len(batch))
                        # Проверяем память каждые 10000 записей
                        if self.total_processed % 10000 == 0:
                            self._log_memory_usage()
                        batch = []
                
                # Отдаём последний неполный батч
                if batch and self.is_running:
                    yield batch
                    if progress_bar:
                        progress_bar.update(len(batch))
                
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных: {e}")
            raise
        finally:
            if progress_bar:
                progress_bar.close()
            logger.info(f"Загружено {self.total_processed} записей")
            
    @asynccontextmanager
    async def batch_processor(
        self,
        filter_date: Optional[datetime] = None,
        processed_status: Optional[bool] = None,
        show_progress: bool = True
    ):
        """
        Контекстный менеджер для безопасной обработки батчей
        
        Args:
            filter_date: Фильтр по дате
            processed_status: Фильтр по статусу
            show_progress: Показывать прогресс-бар
            
        Yields:
            Асинхронный генератор батчей
        """
        try:
            await self.connect()
            yield self.fetch_batch_data(filter_date, processed_status, show_progress)
        finally:
            await self.disconnect()