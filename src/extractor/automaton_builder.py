"""
Модуль построения автоматов Aho-Corasick для поиска паттернов
"""
import logging
import time
from typing import Dict, Set, Optional
import ahocorasick  # Что: импорт библиотеки Aho-Corasick
                   # Зачем: для построения автоматов множественного поиска

logger = logging.getLogger(__name__)


class AutomatonBuilder:
    """
    Построитель автоматов Aho-Corasick для эффективного поиска паттернов
    """
    
    def __init__(self):
        """
        Инициализация построителя автоматов
        """
        # Что: автомат для поиска брендов
        # Зачем: первый этап каскадного поиска
        self.brands_automaton: Optional[ahocorasick.Automaton] = None
        
        # Что: словарь автоматов артикулов по брендам
        # Зачем: второй этап - поиск только артикулов найденных брендов
        self.brand_articles_automatons: Dict[str, ahocorasick.Automaton] = {}
        
        # Что: статистика построения
        self.stats = {
            'brands_count': 0,
            'articles_count': 0,
            'build_time': 0.0
        }
    
    def build_brands_automaton(self, brands: Set[str]) -> ahocorasick.Automaton:
        """
        Построение автомата для поиска брендов
        
        Args:
            brands: множество брендов для поиска
            
        Returns:
            Построенный автомат Aho-Corasick
        """
        start_time = time.time()
        logger.info(f"Начало построения автомата брендов: {len(brands)} паттернов")
        
        # Что: создаем новый автомат
        # Зачем: для эффективного множественного поиска
        automaton = ahocorasick.Automaton()
        
        # Что: добавляем бренды в автомат
        # Зачем: каждый бренд становится искомым паттерном
        for brand in brands:
            if brand:  # пропускаем пустые
                # add_word(паттерн, значение)
                automaton.add_word(brand, brand)
        
        # Что: финализируем автомат
        # Зачем: построение триев и переходов для быстрого поиска
        automaton.make_automaton()
        
        # Что: сохраняем автомат и статистику
        self.brands_automaton = automaton
        self.stats['brands_count'] = len(brands)
        build_time = time.time() - start_time
        
        logger.info(f"Автомат брендов построен за {build_time:.2f} сек")
        return automaton
    
    def build_brand_articles_automaton(
        self, 
        brand: str, 
        articles: Set[str]
    ) -> ahocorasick.Automaton:
        """
        Построение автомата для артикулов конкретного бренда
        
        Args:
            brand: название бренда
            articles: множество артикулов этого бренда
            
        Returns:
            Построенный автомат для артикулов бренда
        """
        logger.debug(f"Построение автомата для {brand}: {len(articles)} артикулов")
        
        # Что: создаем автомат для артикулов бренда
        automaton = ahocorasick.Automaton()
        
        # Что: добавляем артикулы с удалением дубликатов
        # Зачем: избегаем коллизий при добавлении
        unique_articles = set()
        for article in articles:
            if article and article not in unique_articles:
                # Сохраняем кортеж (артикул, бренд)
                automaton.add_word(article, (article, brand))
                unique_articles.add(article)
        
        # Что: финализируем автомат
        automaton.make_automaton()
        
        # Что: сохраняем в словарь автоматов
        self.brand_articles_automatons[brand] = automaton
        self.stats['articles_count'] += len(unique_articles)
        
        return automaton
    
    def build_all_articles_automatons(
        self,
        brand_articles: Dict[str, Set[str]]
    ) -> Dict[str, ahocorasick.Automaton]:
        """
        Построение автоматов для всех брендов и их артикулов
        
        Args:
            brand_articles: словарь бренд -> множество артикулов
            
        Returns:
            Словарь бренд -> автомат артикулов
        """
        start_time = time.time()
        total_brands = len(brand_articles)
        logger.info(f"Начало построения {total_brands} автоматов артикулов")
        
        # Что: строим автомат для каждого бренда
        # Зачем: каскадный поиск - ищем только артикулы найденных брендов
        for brand, articles in brand_articles.items():
            if articles:  # только для брендов с артикулами
                self.build_brand_articles_automaton(brand, articles)
        
        # Что: логируем общую статистику
        build_time = time.time() - start_time
        self.stats['build_time'] = build_time
        
        logger.info(
            f"Построено {len(self.brand_articles_automatons)} автоматов за {build_time:.2f} сек. "
            f"Всего артикулов: {self.stats['articles_count']}"
        )
        
        return self.brand_articles_automatons
