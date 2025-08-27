"""
Модуль для эффективного сохранения результатов извлечения в БД
"""
import logging
import json
from typing import List, Dict, Any, Optional
from datetime import datetime

import asyncpg
import aiofiles
from tqdm.asyncio import tqdm

from .connection import db_connection

logger = logging.getLogger(__name__)


class ResultPersistence:
    """
    Класс для батч-сохранения результатов в БД с прогрессом и обработкой ошибок
    """
    
    def __init__(self, batch_size: int = 500):
        """
        Инициализация модуля сохранения результатов
        
        Args:
            batch_size: размер батча для вставки (по умолчанию 500)
        """
        # Что: размер батча для вставки в БД
        # Зачем: оптимальный баланс между скоростью и нагрузкой на БД
        self.batch_size = batch_size
        
        # Что: счетчики для статистики
        # Зачем: мониторинг процесса сохранения
        self.total_saved = 0
        self.total_errors = 0
        self.problematic_records = []
        
        logger.info(f"Инициализирован ResultPersistence с batch_size={batch_size}")
    
    async def save_batch(self, results: List[Dict[str, Any]]) -> int:
        """
        Сохраняет батч результатов в БД с обработкой конфликтов
        
        Args:
            results: список результатов для сохранения
            
        Returns:
            количество успешно сохраненных записей
        """
        # Что: проверка на пустой список
        # Зачем: избежать лишних операций с БД
        if not results:
            return 0
            
        # Что: подготовка данных для вставки
        # Зачем: преобразование в формат для executemany
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
        
        # Что: SQL запрос с ON CONFLICT для обновления дубликатов
        # Зачем: обеспечить идемпотентность операции
        query = """
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
        
        # Что: получение соединения и выполнение вставки
        # Зачем: использование pool для эффективной работы с БД
        try:
            async with db_connection.pool.acquire() as connection:
                await connection.executemany(query, values)
                logger.debug(f"Батч из {len(values)} записей сохранен")
                return len(values)
                
        except Exception as e:
            logger.error(f"Ошибка при сохранении батча: {e}")
            # Что: добавление проблемного батча в список
            # Зачем: для последующего анализа и повторной попытки
            self.problematic_records.extend([r['ad_id'] for r in results])
            self.total_errors += len(results)
            raise
    
    async def save_results_with_progress(
        self, 
        results: List[Dict[str, Any]],
        description: str = "Сохранение результатов"
    ) -> Dict[str, Any]:
        """
        Сохраняет результаты батчами с отображением прогресса
        
        Args:
            results: полный список результатов для сохранения
            description: описание для прогресс-бара
            
        Returns:
            словарь со статистикой сохранения
        """
        # Что: проверка на пустой список
        # Зачем: избежать лишних операций
        if not results:
            logger.warning("Нет результатов для сохранения")
            return {
                'total_saved': 0,
                'total_errors': 0,
                'problematic_records': []
            }
        
        logger.info(f"Начало сохранения {len(results)} результатов батчами по {self.batch_size}")
        
        # Что: сброс счетчиков перед началом
        # Зачем: обеспечить корректную статистику для текущей операции
        self.total_saved = 0
        self.total_errors = 0
        self.problematic_records = []
        
        # Что: создание прогресс-бара через tqdm
        # Зачем: визуализация процесса для пользователя
        progress_bar = tqdm(
            total=len(results),
            desc=description,
            unit="записей"
        )
        
        try:
            # Что: разбиение на батчи и сохранение
            # Зачем: контролируемая нагрузка на БД и память
            for i in range(0, len(results), self.batch_size):
                batch = results[i:i + self.batch_size]
                
                try:
                    # Что: сохранение отдельного батча
                    # Зачем: атомарность операций на уровне батча
                    saved_count = await self.save_batch(batch)
                    self.total_saved += saved_count
                    progress_bar.update(len(batch))
                    
                except Exception as e:
                    logger.error(f"Ошибка при сохранении батча {i//self.batch_size + 1}: {e}")
                    # Что: продолжаем работу с другими батчами
                    # Зачем: частичное сохранение лучше полного отказа
                    progress_bar.update(len(batch))
                    continue
                    
        finally:
            progress_bar.close()
        
        # Что: логирование финальной статистики
        # Зачем: информирование о результатах операции
        logger.info(
            f"Сохранение завершено: успешно {self.total_saved}, "
            f"ошибок {self.total_errors}"
        )
        
        # Что: сохранение проблемных записей в лог если есть
        # Зачем: возможность повторной обработки или анализа
        if self.problematic_records:
            await self._save_problematic_records_to_log()
        
        return {
            'total_saved': self.total_saved,
            'total_errors': self.total_errors,
            'problematic_records': self.problematic_records
        }
    
    async def _save_problematic_records_to_log(self) -> None:
        """
        Сохраняет проблемные записи в отдельный лог-файл для анализа
        """
        # Что: проверка наличия проблемных записей
        # Зачем: избежать создания пустого файла
        if not self.problematic_records:
            return
            
        # Что: формирование имени файла с временной меткой
        # Зачем: уникальность и хронология логов
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_filename = f'logs/problematic_records_{timestamp}.json'
        
        # Что: подготовка данных для сохранения
        # Зачем: структурированный формат для последующего анализа
        log_data = {
            'timestamp': timestamp,
            'total_errors': self.total_errors,
            'problematic_ad_ids': self.problematic_records,
            'batch_size': self.batch_size
        }
        
        try:
            # Что: сохранение в JSON файл
            # Зачем: удобный формат для чтения и обработки
            async with aiofiles.open(log_filename, 'w') as f:
                await f.write(json.dumps(log_data, indent=2, ensure_ascii=False))
            
            logger.warning(
                f"Проблемные записи сохранены в {log_filename}: "
                f"{len(self.problematic_records)} ad_id"
            )
            
        except Exception as e:
            logger.error(f"Не удалось сохранить проблемные записи в файл: {e}")