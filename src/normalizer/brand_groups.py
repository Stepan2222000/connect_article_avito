"""
Модуль группировки и замены брендов-синонимов
"""
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


def normalize_brand_for_comparison(brand: str) -> str:
    """
    Нормализует бренд для сравнения: UPPER + удаление дефисов и пробелов
    
    Args:
        brand: исходный бренд
        
    Returns:
        Нормализованный бренд для сравнения
    """
    if not brand:
        return ""
    # Что: приводим к UPPER, убираем дефисы и лишние пробелы
    # Зачем: ski-doo = skidoo = SKI DOO для поиска
    return brand.upper().replace("-", "").replace(" ", "").strip()


class BrandGroupMapper:
    """
    Класс для группировки и замены брендов-синонимов на канонические названия
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Инициализация маппера брендов
        
        Args:
            config_path: путь к файлу конфигурации brand_groups.json
        """
        self.config_path = Path(config_path) if config_path else Path("data/brand_groups.json")
        # Что: словарь canonical_brand -> [synonyms]
        self.brand_groups: Dict[str, List[str]] = {}
        # Что: обратный словарь synonym -> canonical_brand  
        self.synonym_to_canonical: Dict[str, str] = {}
        
    def load_groups(self) -> None:
        """
        Загрузка конфигурации групп брендов из JSON файла
        """
        logger.info(f"Загрузка конфигурации групп брендов из {self.config_path}")
        
        # Что: проверяем существование файла
        # Зачем: избегаем ошибки при отсутствии файла
        if not self.config_path.exists():
            raise FileNotFoundError(f"Файл конфигурации не найден: {self.config_path}")
        
        # Что: загружаем JSON конфигурацию
        with open(self.config_path, 'r', encoding='utf-8') as f:
            self.brand_groups = json.load(f)
        
        # Что: строим обратный словарь synonym -> canonical
        # Зачем: быстрый поиск канонического бренда по синониму
        self._build_reverse_mapping()
        
        logger.info(f"Загружено {len(self.brand_groups)} групп брендов")
    
    def _build_reverse_mapping(self) -> None:
        """
        Создание обратного маппинга synonym -> canonical_brand
        """
        self.synonym_to_canonical.clear()
        
        # Что: итерируем по каждой группе брендов
        for canonical_brand, synonyms in self.brand_groups.items():
            for synonym in synonyms:
                # Что: нормализуем синоним для поиска (убираем дефисы и пробелы)
                # Зачем: ski-doo = skidoo = SKI DOO при поиске
                normalized_synonym = normalize_brand_for_comparison(synonym)
                self.synonym_to_canonical[normalized_synonym] = canonical_brand.upper()
    
    def map_brand(self, brand: str) -> str:
        """
        Возвращает канонический бренд для переданного бренда
        
        Args:
            brand: исходный бренд
            
        Returns:
            Канонический бренд или исходный бренд если замена не найдена
        """
        # Что: проверяем на пустую строку
        if not brand:
            return brand
            
        # Что: нормализуем бренд для поиска (убираем дефисы и пробелы)
        # Зачем: ski-doo найдет SKIDOO из конфига
        normalized_brand = normalize_brand_for_comparison(brand)
        
        # Что: ищем канонический бренд в обратном маппинге
        canonical = self.synonym_to_canonical.get(normalized_brand, brand.upper().strip())
        return canonical
    
    def reload_groups(self) -> None:
        """
        Перезагрузка конфигурации групп брендов
        """
        logger.info("Перезагрузка конфигурации групп брендов")
        self.load_groups()