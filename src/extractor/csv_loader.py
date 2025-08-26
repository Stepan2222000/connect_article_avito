"""
Модуль для асинхронной загрузки CSV-словаря артикулов
"""
import logging
import time
from pathlib import Path
from typing import Dict, Set, Optional
import aiofiles
from tqdm.asyncio import tqdm

from ..config import (
    CSV_DICTIONARY_PATH,
    BRAND_GROUPS_PATH,
    MIN_ARTICLE_LEN_DIGITS,
    MIN_ARTICLE_LEN_ALPHANUM,
)
from ..normalizer.brand_groups import BrandGroupMapper

logger = logging.getLogger(__name__)


class CSVDictionaryLoader:
    """
    Загрузчик CSV-словаря артикулов и брендов
    """
    
    def __init__(self, csv_path: Optional[str] = None):
        """
        Инициализация загрузчика
        
        Args:
            csv_path: Путь к CSV файлу
        """
        self.csv_path = Path(csv_path or CSV_DICTIONARY_PATH)
        # Структура: бренд -> множество артикулов  
        self.brand_articles: Dict[str, Set[str]] = {}
        # Что: система группировки брендов
        # Зачем: замена синонимов на канонические названия
        self.brand_mapper = BrandGroupMapper(BRAND_GROUPS_PATH)
        # Статистика
        self.stats = {
            'total_lines': 0,
            'valid_articles': 0,
            'skipped_empty': 0,
            'skipped_short': 0
        }
        
    async def load_dictionary(self) -> Dict[str, Set[str]]:
        """
        Загрузка CSV словаря
        
        Returns:
            Словарь бренд -> артикулы
        """
        start_time = time.time()
        logger.info(f"Начало загрузки CSV из {self.csv_path}")
        
        # Что: загружаем конфигурацию групп брендов
        # Зачем: для замены синонимов на канонические названия
        try:
            self.brand_mapper.load_groups()
        except Exception as e:
            logger.warning(f"Не удалось загрузить группы брендов: {e}")
        
        # Проверка файла
        if not self.csv_path.exists():
            raise FileNotFoundError(f"CSV файл не найден: {self.csv_path}")
        
        try:
            # Сначала считаем строки для прогресс-бара
            total_lines = await self._count_lines()
            
            # Загружаем файл асинхронно
            async with aiofiles.open(self.csv_path, mode='r', encoding='utf-8') as file:
                # Пропускаем заголовок
                await file.readline()
                
                # Создаем прогресс-бар
                progress_bar = tqdm(
                    total=total_lines - 1,  # минус заголовок
                    desc="Загрузка CSV",
                    unit="строк"
                )
                
                # Читаем построчно
                async for line in file:
                    await self._process_line(line)
                    progress_bar.update(1)
                    self.stats['total_lines'] += 1
                
                progress_bar.close()
            
            # Логируем статистику
            load_time = time.time() - start_time
            logger.info(
                f"Загрузка завершена за {load_time:.2f} сек: "
                f"строк={self.stats['total_lines']}, "
                f"валидных={self.stats['valid_articles']}, "
                f"брендов={len(self.brand_articles)}, "
                f"пропущено_пустых={self.stats['skipped_empty']}, "
                f"пропущено_коротких={self.stats['skipped_short']}"
            )
            
        except UnicodeDecodeError as e:
            logger.error(f"Ошибка кодировки при чтении CSV: {e}")
            raise ValueError(f"CSV файл имеет неправильную кодировку: {e}")
        except IOError as e:
            logger.error(f"Ошибка ввода/вывода при чтении CSV: {e}")
            raise IOError(f"Не удалось прочитать CSV файл: {e}")
        except Exception as e:
            logger.error(f"Неожиданная ошибка при загрузке CSV: {e}")
            raise
        
        return self.brand_articles
    
    async def _count_lines(self) -> int:
        """
        Подсчет количества строк в файле
        """
        line_count = 0
        async with aiofiles.open(self.csv_path, mode='r', encoding='utf-8') as file:
            async for _ in file:
                line_count += 1
        logger.info(f"Найдено {line_count} строк в CSV")
        return line_count
    
    async def _process_line(self, line: str) -> None:
        """
        Обработка одной строки CSV
        """
        # Парсим строку (формат: id,article,brand)
        parts = line.strip().split(',')
        
        if len(parts) < 3:
            return
        
        article = parts[1].strip()
        brand = parts[2].strip().upper()
        
        # Что: применяем группировку брендов (замена синонимов)
        # Зачем: LYNX -> BRP, CANAM -> BRP и т.д.
        canonical_brand = self.brand_mapper.map_brand(brand)
        
        # Валидация артикула
        if not self._validate_article(article):
            return
        
        # Добавляем в словарь с каноническим брендом
        if canonical_brand not in self.brand_articles:
            self.brand_articles[canonical_brand] = set()
        
        self.brand_articles[canonical_brand].add(article)
        self.stats['valid_articles'] += 1
    
    def _validate_article(self, article: str) -> bool:
        """
        Валидация артикула по минимальной длине
        """
        # Пропускаем пустые
        if not article:
            self.stats['skipped_empty'] += 1
            return False
        
        # Проверяем минимальную длину
        has_letters = any(c.isalpha() for c in article)
        
        if has_letters:
            # Для буквенно-цифровых
            min_length = MIN_ARTICLE_LEN_ALPHANUM
        else:
            # Для цифровых
            min_length = MIN_ARTICLE_LEN_DIGITS
        
        if len(article) < min_length:
            self.stats['skipped_short'] += 1
            return False
            
        return True