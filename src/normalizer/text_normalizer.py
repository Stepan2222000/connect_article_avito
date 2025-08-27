"""
Модуль нормализации текстов объявлений для поиска артикулов
"""
import logging
import re
from functools import lru_cache

logger = logging.getLogger(__name__)

# Что: таблица замены кириллицы на латиницу
# Зачем: артикулы могут быть написаны смешанными алфавитами
CYRILLIC_TO_LATIN = {
    'А': 'A', 'В': 'B', 'Е': 'E', 'К': 'K', 'М': 'M', 'Н': 'H', 
    'О': 'O', 'Р': 'P', 'С': 'C', 'Т': 'T', 'У': 'Y', 'Х': 'X',
    'Я': 'Y', 'И': 'I', 'Й': 'I', 'Ю': 'U', 'Ё': 'E', 'Ч': 'C',
    'Ш': 'S', 'Щ': 'S', 'Ж': 'Z', 'З': 'Z', 'Ц': 'C', 'Ь': '', 'Ъ': '',
    'Г': 'G', 'Д': 'D', 'Л': 'L', 'П': 'P', 'Ф': 'F', 'Б': 'B',
    # Строчные буквы для полноты
    'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e',
    'ё': 'e', 'ж': 'z', 'з': 'z', 'и': 'i', 'й': 'i', 'к': 'k',
    'л': 'l', 'м': 'm', 'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r',
    'с': 's', 'т': 't', 'у': 'u', 'ф': 'f', 'х': 'x', 'ц': 'c',
    'ч': 'c', 'ш': 's', 'щ': 's', 'ъ': '', 'ы': 'y', 'ь': '',
    'э': 'e', 'ю': 'u', 'я': 'y'
}


@lru_cache(maxsize=10000)
def normalize_text_for_search(text: str) -> str:
    """
    Нормализует текст для поиска артикулов по правилам проекта
    
    Args:
        text: исходный текст
        
    Returns:
        Нормализованный текст
    """
    # Что: проверяем на пустую строку
    if not text:
        return ""
    
    # Что: заменяем кириллицу на латиницу ПЕРЕД приведением к UPPER
    # Зачем: артикулы могут быть написаны смешанными алфавитами
    normalized = text
    for cyrillic, latin in CYRILLIC_TO_LATIN.items():
        normalized = normalized.replace(cyrillic, latin)
    
    # Что: приводим к UPPERCASE ПОСЛЕ замены кириллицы
    # Зачем: унификация регистра для поиска
    normalized = normalized.upper()
    
    # Что: заменяем тире на пробелы
    # Зачем: артикулы могут писаться с тире и без (ABC-123 = ABC 123)
    normalized = normalized.replace('-', ' ')
    
    # Что: заменяем спецсимволы на пробелы, сохраняя цифры и буквы
    # Зачем: очистка от мусорных символов, но сохранение структуры текста
    normalized = re.sub(r'[^A-Za-z0-9\s]', ' ', normalized)
    
    # Что: нормализуем множественные пробелы в одиночные
    # Зачем: согласно требованиям "пробелы не схлопываем" - делаем одиночными
    normalized = re.sub(r'\s+', ' ', normalized)
    
    # Что: убираем пробелы с краев
    normalized = normalized.strip()
    
    return normalized


@lru_cache(maxsize=5000)  
def normalize_article_for_search(article: str) -> str:
    """
    Нормализует артикул для поиска (отдельная логика для артикулов)
    
    Args:
        article: исходный артикул
        
    Returns:
        Нормализованный артикул для поиска (без дефисов)
    """
    # Что: проверяем на пустую строку
    if not article:
        return ""
    
    # Что: применяем те же правила что и для текста
    normalized = normalize_text_for_search(article)
    
    return normalized


@lru_cache(maxsize=5000)
def normalize_article_for_storage(article: str) -> str:
    """
    Нормализует артикул для записи в БД (с сохранением дефисов)
    
    Args:
        article: исходный артикул
        
    Returns:
        Нормализованный артикул для записи в БД (с дефисами)
    """
    # Что: проверяем на пустую строку
    if not article:
        return ""
    
    # Что: заменяем кириллицу на латиницу ПЕРЕД приведением к UPPER
    # Зачем: артикулы могут быть написаны смешанными алфавитами
    normalized = article
    for cyrillic, latin in CYRILLIC_TO_LATIN.items():
        normalized = normalized.replace(cyrillic, latin)
    
    # Что: приводим к UPPERCASE ПОСЛЕ замены кириллицы
    # Зачем: унификация регистра
    normalized = normalized.upper()
    
    # Что: НЕ удаляем дефисы (оставляем исходные)
    # Зачем: для записи в БД нужен вариант с исходными дефисами
    
    # Что: заменяем спецсимволы на пробелы, сохраняя цифры, буквы и дефисы
    # Зачем: очистка от мусора но сохранение структуры артикула
    normalized = re.sub(r'[^A-Z0-9\s\-]', ' ', normalized)
    
    # Что: нормализуем множественные пробелы в одиночные
    normalized = re.sub(r'\s+', ' ', normalized)
    
    # Что: убираем пробелы с краев
    normalized = normalized.strip()
    
    return normalized


def clear_normalization_cache() -> None:
    """
    Очищает кеш нормализации (для тестов и отладки)
    """
    # Что: очищаем LRU кеши всех функций нормализации
    # Зачем: для тестов и сброса состояния при необходимости
    normalize_text_for_search.cache_clear()
    normalize_article_for_search.cache_clear()
    normalize_article_for_storage.cache_clear()
    logger.info("Кеш нормализации очищен")