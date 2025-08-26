"""
Модуль асинхронного подключения к PostgreSQL через asyncpg
"""
import asyncio
import asyncpg
import logging
from typing import Optional, List, Any, Dict
from functools import wraps

from ..config import (
    DATABASE_HOST,
    DATABASE_PORT,
    DATABASE_NAME,
    DATABASE_USER,
    DATABASE_PASSWORD,
    DATABASE_MIN_CONNECTIONS,
    DATABASE_MAX_CONNECTIONS,
)

logger = logging.getLogger(__name__)


def with_retry(max_attempts: int = 3, initial_delay: float = 1.0, backoff_factor: float = 2.0):
    """
    Декоратор для retry логики с экспоненциальной задержкой
    
    Args:
        max_attempts: максимальное количество попыток
        initial_delay: начальная задержка в секундах  
        backoff_factor: множитель для увеличения задержки
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            delay = initial_delay
            last_exception = None
            
            # Пытаемся выполнить функцию max_attempts раз
            for attempt in range(max_attempts):
                try:
                    # Логируем попытку если это не первая
                    if attempt > 0:
                        logger.info(f"Попытка {attempt + 1}/{max_attempts} для {func.__name__}")
                    
                    # Выполняем функцию
                    result = await func(*args, **kwargs)
                    
                    # Успешно - возвращаем результат
                    if attempt > 0:
                        logger.info(f"Успешное выполнение {func.__name__} с попытки {attempt + 1}")
                    return result
                    
                except (asyncpg.PostgresError, asyncio.TimeoutError, ConnectionError) as e:
                    last_exception = e
                    
                    # Логируем ошибку
                    logger.warning(
                        f"Ошибка в {func.__name__} (попытка {attempt + 1}/{max_attempts}): {e}"
                    )
                    
                    # Если последняя попытка - пробрасываем исключение
                    if attempt == max_attempts - 1:
                        logger.error(f"Все попытки исчерпаны для {func.__name__}")
                        raise
                    
                    # Ждем перед следующей попыткой
                    logger.info(f"Ожидание {delay:.1f} сек перед следующей попыткой...")
                    await asyncio.sleep(delay)
                    
                    # Увеличиваем задержку
                    delay *= backoff_factor
                    
            # На всякий случай
            if last_exception:
                raise last_exception
                
        return wrapper
    return decorator


class DatabaseConnection:
    """
    Класс для управления асинхронным подключением к PostgreSQL
    """
    
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None
        
    @with_retry(max_attempts=3, initial_delay=1.0, backoff_factor=2.0)
    async def connect(self) -> None:
        """
        Создает connection pool для работы с БД
        """
        # Проверяем что pool еще не создан
        if self.pool:
            logger.warning("Connection pool уже существует")
            return
            
        # Логируем попытку создания pool
        logger.info(
            f"Создание connection pool к {DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}"
        )
        
        # Формируем строку подключения
        connection_string = (
            f"postgresql://{DATABASE_USER}:{DATABASE_PASSWORD}@"
            f"{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}"
        )
        
        # Создаем pool соединений с оптимальными настройками
        try:
            self.pool = await asyncpg.create_pool(
                connection_string,
                min_size=DATABASE_MIN_CONNECTIONS,  # min_size=10
                max_size=DATABASE_MAX_CONNECTIONS,  # max_size=20
                max_queries=50000,
                max_inactive_connection_lifetime=300.0,
                timeout=60.0,
                command_timeout=60.0
            )
            
            # Проверяем подключение через pool
            async with self.pool.acquire() as connection:
                version = await connection.fetchval('SELECT version()')
                logger.info(f"Успешное подключение к PostgreSQL: {version}")
            
        except Exception as e:
            logger.error(f"Ошибка создания connection pool: {e}")
            raise
            
    async def disconnect(self) -> None:
        """
        Закрывает подключение к БД
        """
        if self.pool:
            logger.info("Закрытие подключения")
            await self.pool.close()
            self.pool = None
            
    async def execute(self, query: str, *args) -> str:
        """
        Выполняет SQL команду без возврата результата
        
        Args:
            query: SQL запрос
            *args: параметры запроса
            
        Returns:
            Статус выполнения команды
        """
        # Проверяем что pool инициализирован
        if not self.pool:
            raise ConnectionError("Connection pool не инициализирован. Вызовите connect() сначала.")
            
        # Получаем соединение из pool и выполняем запрос
        async with self.pool.acquire() as connection:
            logger.debug(f"Выполнение запроса: {query[:100]}...")
            result = await connection.execute(query, *args)
            logger.debug(f"Результат выполнения: {result}")
            return result
            
    async def fetch(self, query: str, *args) -> List[asyncpg.Record]:
        """
        Выполняет SELECT запрос и возвращает все строки
        
        Args:
            query: SQL запрос
            *args: параметры запроса
            
        Returns:
            Список записей
        """
        # Проверяем что pool инициализирован
        if not self.pool:
            raise ConnectionError("Connection pool не инициализирован. Вызовите connect() сначала.")
            
        # Выполняем запрос и возвращаем все записи
        async with self.pool.acquire() as connection:
            logger.debug(f"Выполнение запроса: {query[:100]}...")
            result = await connection.fetch(query, *args)
            logger.debug(f"Получено {len(result)} записей")
            return result
            
    async def fetchrow(self, query: str, *args) -> Optional[asyncpg.Record]:
        """
        Выполняет SELECT запрос и возвращает первую строку
        
        Args:
            query: SQL запрос
            *args: параметры запроса
            
        Returns:
            Первая запись или None
        """
        # Проверяем что pool инициализирован
        if not self.pool:
            raise ConnectionError("Connection pool не инициализирован. Вызовите connect() сначала.")
            
        # Выполняем запрос и возвращаем первую запись
        async with self.pool.acquire() as connection:
            logger.debug(f"Выполнение запроса: {query[:100]}...")
            result = await connection.fetchrow(query, *args)
            logger.debug(f"Получена {'запись' if result else 'пустой результат'}")
            return result
            
    async def fetchval(self, query: str, *args) -> Any:
        """
        Выполняет SELECT запрос и возвращает первое значение первой строки
        
        Args:
            query: SQL запрос
            *args: параметры запроса
            
        Returns:
            Значение или None
        """
        # Проверяем что pool инициализирован
        if not self.pool:
            raise ConnectionError("Connection pool не инициализирован. Вызовите connect() сначала.")
            
        # Выполняем запрос и возвращаем первое значение
        async with self.pool.acquire() as connection:
            logger.debug(f"Выполнение запроса: {query[:100]}...")
            result = await connection.fetchval(query, *args)
            logger.debug(f"Получено значение: {result}")
            return result
            
    async def get_ads_data(self, limit: Optional[int] = None, offset: int = 0) -> List[Dict[str, Any]]:
        """
        Получает объединенные данные из special_model_data и text_model_data
        
        Args:
            limit: ограничение количества записей
            offset: смещение для пагинации
            
        Returns:
            Список объявлений с объединенными данными
        """
        logger.info(f"Чтение данных объявлений (limit={limit}, offset={offset})")
        
        # SQL запрос с JOIN таблиц и склеиванием текстов
        query = """
        SELECT 
            s.id as ad_id,
            s.title,
            t.description,
            t.characteristic,
            -- Склеиваем все текстовые поля в одно
            COALESCE(s.title, '') || ' ' || 
            COALESCE(t.description, '') || ' ' || 
            COALESCE(t.characteristic, '') as text_raw
        FROM public.special_model_data s
        LEFT JOIN public.text_model_data t ON s.id = t.id
        WHERE s.title IS NOT NULL OR t.description IS NOT NULL
        """
        
        # Добавляем LIMIT и OFFSET если указаны
        if limit:
            query += f" LIMIT {limit}"
        if offset:
            query += f" OFFSET {offset}"
            
        try:
            # Выполняем запрос
            records = await self.fetch(query)
            
            # Преобразуем Record в словари для удобства
            result = []
            for record in records:
                result.append({
                    'ad_id': record['ad_id'],
                    'title': record['title'],
                    'description': record['description'],
                    'characteristic': record['characteristic'],
                    'text_raw': record['text_raw']
                })
                
            logger.info(f"Получено {len(result)} записей из БД")
            return result
            
        except asyncpg.PostgresError as e:
            logger.error(f"Ошибка чтения данных: {e}")
            raise
            
    async def create_table_avito_parts_resolved(self) -> None:
        """
        Создает таблицу avito_parts_resolved если она не существует
        """
        logger.info("Проверка и создание таблицы avito_parts_resolved")
        
        # SQL для создания таблицы и индексов
        create_table_query = """
        CREATE TABLE IF NOT EXISTS public.avito_parts_resolved (
            ad_id BIGINT PRIMARY KEY,
            text_clean TEXT,
            first_article VARCHAR(255),
            brand_near_first_article VARCHAR(255),
            all_articles TEXT[],
            all_brands TEXT[],
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Создаем индексы для ускорения поиска
        CREATE INDEX IF NOT EXISTS idx_avito_parts_first_article 
            ON public.avito_parts_resolved(first_article);
        CREATE INDEX IF NOT EXISTS idx_avito_parts_brand 
            ON public.avito_parts_resolved(brand_near_first_article);
        CREATE INDEX IF NOT EXISTS idx_avito_parts_processed_at 
            ON public.avito_parts_resolved(processed_at);
        """
        
        try:
            # Выполняем создание таблицы и индексов
            await self.execute(create_table_query)
            logger.info("Таблица avito_parts_resolved готова к использованию")
        except asyncpg.PostgresError as e:
            logger.error(f"Ошибка создания таблицы: {e}")
            raise
            
    async def save_results(self, results: List[Dict[str, Any]]) -> int:
        """
        Сохраняет результаты обработки в таблицу avito_parts_resolved
        
        Args:
            results: список результатов для сохранения
            
        Returns:
            Количество сохраненных записей
        """
        # Проверяем что есть данные для сохранения
        if not results:
            logger.warning("Нет результатов для сохранения")
            return 0
            
        logger.info(f"Сохранение {len(results)} результатов в БД")
        
        # Подготавливаем данные для вставки
        values = []
        for r in results:
            values.append((
                r['ad_id'],
                r.get('text_clean', ''),
                r.get('first_article'),
                r.get('brand_near_first_article'),
                r.get('all_articles', []),
                r.get('all_brands', [])
            ))
            
        # SQL запрос с ON CONFLICT для обновления существующих записей
        insert_query = """
        INSERT INTO public.avito_parts_resolved 
            (ad_id, text_clean, first_article, brand_near_first_article, all_articles, all_brands)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (ad_id) DO UPDATE SET
            text_clean = EXCLUDED.text_clean,
            first_article = EXCLUDED.first_article,
            brand_near_first_article = EXCLUDED.brand_near_first_article,
            all_articles = EXCLUDED.all_articles,
            all_brands = EXCLUDED.all_brands,
            processed_at = CURRENT_TIMESTAMP
        """
        
        try:
            # Используем executemany для batch insert
            async with self.pool.acquire() as connection:
                await connection.executemany(insert_query, values)
                
            logger.info(f"Успешно сохранено {len(values)} записей")
            return len(values)
            
        except asyncpg.PostgresError as e:
            logger.error(f"Ошибка сохранения результатов: {e}")
            raise


# Глобальный экземпляр для использования в приложении
db_connection = DatabaseConnection()