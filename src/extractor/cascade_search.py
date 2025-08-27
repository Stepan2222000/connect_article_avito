"""
Модуль каскадного поиска артикулов через бренды
"""
import logging
from typing import Dict, Set, List, Optional, Tuple
from dataclasses import dataclass
import time

logger = logging.getLogger(__name__)


@dataclass
class SearchResult:
    """
    Результат каскадного поиска
    """
    # Что: первый найденный артикул по позиции в тексте
    # Зачем: главный результат поиска для отчета
    first_article: Optional[str] = None
    
    # Что: бренд первого артикула из словаря
    # Зачем: ассоциация артикула с брендом по словарю, не по позиции
    brand_near_first_article: Optional[str] = None
    
    # Что: все найденные артикулы
    # Зачем: полная статистика находок
    all_articles: List[str] = None
    
    # Что: все найденные бренды
    # Зачем: для статистики и отладки
    all_brands: List[str] = None
    
    # Что: статистика поиска
    # Зачем: для логирования и оптимизации
    stats: Dict[str, int] = None
    
    def __post_init__(self):
        if self.all_articles is None:
            self.all_articles = []
        if self.all_brands is None:
            self.all_brands = []
        if self.stats is None:
            self.stats = {
                'brands_found': 0,
                'articles_found': 0,
                'search_time_ms': 0
            }


class CascadeSearchEngine:
    """
    Движок каскадного поиска: сначала бренды, затем их артикулы
    """
    
    def __init__(self):
        """
        Инициализация движка каскадного поиска
        """
        # Что: автомат для поиска брендов (из AutomatonBuilder)
        # Зачем: первый этап каскада
        self.brands_automaton = None
        
        # Что: словарь автоматов артикулов по брендам
        # Зачем: второй этап - поиск артикулов только найденных брендов
        self.brand_articles_automatons: Dict[str, any] = {}
        
        # Что: общая статистика работы движка
        # Зачем: мониторинг производительности
        self.total_stats = {
            'total_searches': 0,
            'total_brands_found': 0,
            'total_articles_found': 0,
            'total_time_ms': 0
        }
        
        logger.info("Инициализирован движок каскадного поиска")
    
    def set_automatons(self, brands_automaton, brand_articles_automatons: Dict[str, any]):
        """
        Установка готовых автоматов из AutomatonBuilder
        
        Args:
            brands_automaton: автомат для поиска брендов
            brand_articles_automatons: словарь автоматов артикулов по брендам
        """
        # Что: сохраняем автоматы для поиска
        # Зачем: используем готовые автоматы из AutomatonBuilder
        self.brands_automaton = brands_automaton
        self.brand_articles_automatons = brand_articles_automatons
        
        logger.info(
            f"Установлены автоматы: {len(brand_articles_automatons)} брендов"
        )
    
    def _search_brands(self, text: str) -> Dict[str, List[int]]:
        """
        Поиск брендов в тексте (первый этап каскада)
        
        Args:
            text: нормализованный текст для поиска
            
        Returns:
            Словарь {бренд: [позиции в тексте]}
        """
        # Что: результат - бренды и их позиции
        # Зачем: нужны позиции для определения первого артикула
        brands_positions = {}
        
        if not self.brands_automaton:
            logger.warning("Автомат брендов не инициализирован")
            return brands_positions
        
        # Что: ищем все вхождения брендов через Aho-Corasick
        # Зачем: эффективный множественный поиск O(n)
        try:
            for end_pos, brand in self.brands_automaton.iter(text):
                if brand not in brands_positions:
                    brands_positions[brand] = []
                # end_pos - конец найденного паттерна
                start_pos = end_pos - len(brand) + 1
                brands_positions[brand].append(start_pos)
        except Exception as e:
            logger.error(f"Ошибка при поиске брендов: {e}")
            
        logger.debug(f"Найдено брендов: {len(brands_positions)}")
        return brands_positions
    
    def _search_articles_for_brands(
        self, 
        text: str, 
        found_brands: Set[str]
    ) -> List[Tuple[int, str, str]]:
        """
        Поиск артикулов только для найденных брендов (второй этап)
        
        Args:
            text: нормализованный текст
            found_brands: множество найденных брендов
            
        Returns:
            Список кортежей (позиция, артикул, бренд)
        """
        # Что: собираем все найденные артикулы с позициями
        # Зачем: нужны позиции для определения первого артикула
        articles_with_positions = []
        
        # Что: ищем артикулы только найденных брендов
        # Зачем: минимизация ложных срабатываний
        for brand in found_brands:
            if brand not in self.brand_articles_automatons:
                logger.debug(f"Нет автомата артикулов для бренда {brand}")
                continue
                
            automaton = self.brand_articles_automatons[brand]
            
            try:
                # Что: поиск артикулов бренда через его автомат
                # Зачем: каскадный поиск - только релевантные артикулы
                for end_pos, (article, article_brand) in automaton.iter(text):
                    start_pos = end_pos - len(article) + 1
                    articles_with_positions.append((start_pos, article, article_brand))
            except Exception as e:
                logger.error(f"Ошибка при поиске артикулов бренда {brand}: {e}")
        
        # Что: сортируем по позиции для определения первого
        # Зачем: первый артикул определяется по позиции в тексте
        articles_with_positions.sort(key=lambda x: x[0])
        
        logger.debug(f"Найдено артикулов: {len(articles_with_positions)}")
        return articles_with_positions
    
    def search(self, text: str) -> SearchResult:
        """
        Основной метод каскадного поиска
        
        Args:
            text: нормализованный текст для поиска
            
        Returns:
            SearchResult с найденными артикулами и брендами
        """
        start_time = time.time()
        result = SearchResult()
        
        # Что: проверяем готовность автоматов
        # Зачем: без автоматов поиск невозможен
        if not self.brands_automaton or not self.brand_articles_automatons:
            logger.warning("Автоматы не инициализированы")
            return result
        
        # Этап 1: Поиск брендов
        # Что: находим все бренды в тексте
        # Зачем: первый этап каскада для минимизации ложных срабатываний
        brands_positions = self._search_brands(text)
        found_brands = set(brands_positions.keys())
        
        # Что: сохраняем найденные бренды
        # Зачем: для статистики и отладки
        result.all_brands = list(found_brands)
        result.stats['brands_found'] = len(found_brands)
        
        if not found_brands:
            logger.debug("Бренды не найдены, пропускаем поиск артикулов")
            result.stats['search_time_ms'] = int((time.time() - start_time) * 1000)
            return result
        
        # Этап 2: Поиск артикулов найденных брендов
        # Что: ищем только артикулы найденных брендов
        # Зачем: каскадный подход минимизирует ложные срабатывания
        articles_with_positions = self._search_articles_for_brands(text, found_brands)
        
        # Что: определяем первый артикул и его бренд
        # Зачем: главный результат поиска по требованиям
        if articles_with_positions:
            # Первый артикул - с минимальной позицией
            first_position, first_article, first_brand = articles_with_positions[0]
            result.first_article = first_article
            result.brand_near_first_article = first_brand  # Бренд из словаря, не по позиции!
            
            # Все артикулы для статистики
            result.all_articles = [art for _, art, _ in articles_with_positions]
            result.stats['articles_found'] = len(articles_with_positions)
        
        # Что: фиксируем время поиска
        # Зачем: мониторинг производительности
        search_time_ms = int((time.time() - start_time) * 1000)
        result.stats['search_time_ms'] = search_time_ms
        
        # Обновляем общую статистику
        self.total_stats['total_searches'] += 1
        self.total_stats['total_brands_found'] += len(found_brands)
        self.total_stats['total_articles_found'] += len(articles_with_positions)
        self.total_stats['total_time_ms'] += search_time_ms
        
        logger.debug(
            f"Поиск завершен за {search_time_ms}мс: "
            f"брендов={len(found_brands)}, артикулов={len(articles_with_positions)}"
        )
        
        return result