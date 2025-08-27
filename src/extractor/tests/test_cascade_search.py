"""
Тесты для модуля каскадного поиска
"""
import pytest
import ahocorasick
from ..cascade_search import CascadeSearchEngine, SearchResult


class TestCascadeSearch:
    """
    Тесты для каскадного поиска артикулов
    """
    
    def setup_method(self):
        """
        Подготовка тестовых данных
        """
        self.engine = CascadeSearchEngine()
        
        # Что: создаем тестовый автомат брендов
        # Зачем: для тестирования первого этапа каскада
        self.brands_automaton = ahocorasick.Automaton()
        self.brands_automaton.add_word("YAMAHA", "YAMAHA")
        self.brands_automaton.add_word("HONDA", "HONDA")
        self.brands_automaton.add_word("SUZUKI", "SUZUKI")
        self.brands_automaton.make_automaton()
        
        # Что: создаем тестовые автоматы артикулов для каждого бренда
        # Зачем: для тестирования второго этапа каскада
        self.brand_articles = {}
        
        # Артикулы YAMAHA
        yamaha_automaton = ahocorasick.Automaton()
        yamaha_automaton.add_word("YA123", ("YA123", "YAMAHA"))
        yamaha_automaton.add_word("YA456", ("YA456", "YAMAHA"))
        yamaha_automaton.make_automaton()
        self.brand_articles["YAMAHA"] = yamaha_automaton
        
        # Артикулы HONDA
        honda_automaton = ahocorasick.Automaton()
        honda_automaton.add_word("HO789", ("HO789", "HONDA"))
        honda_automaton.add_word("HO321", ("HO321", "HONDA"))
        honda_automaton.make_automaton()
        self.brand_articles["HONDA"] = honda_automaton
        
        # Устанавливаем автоматы в движок
        self.engine.set_automatons(self.brands_automaton, self.brand_articles)
    
    def test_search_no_brands(self):
        """
        Тест: текст без брендов не должен находить артикулы
        """
        # Что: текст без упоминания брендов
        # Зачем: проверяем что каскад работает - без бренда нет поиска артикулов
        text = "PRODAU FILTR MASLYANYI ARTIKUL YA123 NOVYI"
        result = self.engine.search(text)
        
        assert result.first_article is None
        assert result.brand_near_first_article is None
        assert len(result.all_articles) == 0
        assert len(result.all_brands) == 0
        assert result.stats['brands_found'] == 0
        assert result.stats['articles_found'] == 0
    
    def test_search_brand_with_articles(self):
        """
        Тест: находим бренд и его артикулы
        """
        # Что: текст с брендом YAMAHA и его артикулом
        # Зачем: проверяем полный каскадный поиск
        text = "PRODAU FILTR YAMAHA ARTIKUL YA123 ORIGINAL"
        result = self.engine.search(text)
        
        assert result.first_article == "YA123"
        assert result.brand_near_first_article == "YAMAHA"
        assert "YA123" in result.all_articles
        assert "YAMAHA" in result.all_brands
        assert result.stats['brands_found'] == 1
        assert result.stats['articles_found'] == 1
    
    def test_search_multiple_brands_and_articles(self):
        """
        Тест: несколько брендов и артикулов, первый артикул определяется по позиции
        """
        # Что: текст с двумя брендами и их артикулами
        # Зачем: проверяем определение первого артикула по позиции
        text = "ZAPCHASTI HONDA HO789 I YAMAHA YA456"
        result = self.engine.search(text)
        
        # HO789 идет раньше в тексте, поэтому он первый
        assert result.first_article == "HO789"
        assert result.brand_near_first_article == "HONDA"
        assert set(result.all_articles) == {"HO789", "YA456"}
        assert set(result.all_brands) == {"HONDA", "YAMAHA"}
        assert result.stats['brands_found'] == 2
        assert result.stats['articles_found'] == 2
    
    def test_search_brand_without_articles(self):
        """
        Тест: бренд найден, но его артикулов в тексте нет
        """
        # Что: текст с брендом но без артикулов
        # Зачем: проверяем корректность обработки случая "бренд есть, артикулов нет"
        text = "MOTOCIKL SUZUKI NA PRODAJU"
        result = self.engine.search(text)
        
        assert result.first_article is None
        assert result.brand_near_first_article is None
        assert len(result.all_articles) == 0
        assert result.all_brands == ["SUZUKI"]
        assert result.stats['brands_found'] == 1
        assert result.stats['articles_found'] == 0
    
    def test_search_result_initialization(self):
        """
        Тест: проверка инициализации SearchResult
        """
        # Что: создаем пустой результат
        # Зачем: проверяем корректность инициализации по умолчанию
        result = SearchResult()
        
        assert result.first_article is None
        assert result.brand_near_first_article is None
        assert result.all_articles == []
        assert result.all_brands == []
        assert 'brands_found' in result.stats
        assert 'articles_found' in result.stats
        assert 'search_time_ms' in result.stats