"""
Тесты для модуля automaton_builder
"""
import pytest
from unittest.mock import MagicMock
import sys
from pathlib import Path

# Добавляем путь к src
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Мокаем pyahocorasick так как он может быть недоступен в тестах
sys.modules['pyahocorasick'] = MagicMock()

from src.extractor.automaton_builder import AutomatonBuilder


class TestAutomatonBuilder:
    """Тесты для AutomatonBuilder"""
    
    def test_init(self):
        """Тест инициализации"""
        builder = AutomatonBuilder()
        
        # Что: проверяем начальное состояние
        assert builder.brands_automaton is None
        assert builder.brand_articles_automatons == {}
        assert builder.stats['brands_count'] == 0
        assert builder.stats['articles_count'] == 0
        assert builder.stats['build_time'] == 0.0
    
    def test_build_brands_automaton(self):
        """Тест построения автомата брендов"""
        builder = AutomatonBuilder()
        brands = {'YAMAHA', 'HONDA', 'KAWASAKI', 'BRP'}
        
        # Что: мокаем Automaton
        mock_automaton = MagicMock()
        sys.modules['pyahocorasick'].Automaton.return_value = mock_automaton
        
        # Что: строим автомат
        result = builder.build_brands_automaton(brands)
        
        # Что: проверяем вызовы
        assert mock_automaton.add_word.call_count == 4
        assert mock_automaton.make_automaton.called
        assert builder.brands_automaton is not None
        assert builder.stats['brands_count'] == 4
    
    def test_build_brands_automaton_empty(self):
        """Тест с пустым множеством брендов"""
        builder = AutomatonBuilder()
        brands = set()
        
        mock_automaton = MagicMock()
        sys.modules['pyahocorasick'].Automaton.return_value = mock_automaton
        
        result = builder.build_brands_automaton(brands)
        
        # Что: проверяем что пустые не добавлены
        assert mock_automaton.add_word.call_count == 0
        assert mock_automaton.make_automaton.called
        assert builder.stats['brands_count'] == 0
    
    def test_build_brand_articles_automaton(self):
        """Тест построения автомата артикулов для бренда"""
        builder = AutomatonBuilder()
        brand = 'YAMAHA'
        articles = {'ABC123', 'XYZ456', 'DEF789'}
        
        mock_automaton = MagicMock()
        sys.modules['pyahocorasick'].Automaton.return_value = mock_automaton
        
        result = builder.build_brand_articles_automaton(brand, articles)
        
        # Что: проверяем вызовы и состояние
        assert mock_automaton.add_word.call_count == 3
        assert mock_automaton.make_automaton.called
        assert brand in builder.brand_articles_automatons
        assert builder.stats['articles_count'] == 3
    
    def test_build_brand_articles_with_duplicates(self):
        """Тест обработки дубликатов артикулов"""
        builder = AutomatonBuilder()
        brand = 'HONDA'
        # Что: артикулы с дубликатами
        articles = {'ABC123', 'ABC123', 'XYZ456', ''}
        
        mock_automaton = MagicMock()
        sys.modules['pyahocorasick'].Automaton.return_value = mock_automaton
        
        result = builder.build_brand_articles_automaton(brand, articles)
        
        # Что: проверяем что дубликаты не добавлены
        # Зачем: избегаем коллизий в автомате
        assert mock_automaton.add_word.call_count == 2  # только ABC123 и XYZ456
        assert builder.stats['articles_count'] == 2
    
    def test_build_all_articles_automatons(self):
        """Тест построения всех автоматов"""
        builder = AutomatonBuilder()
        brand_articles = {
            'YAMAHA': {'A1', 'A2', 'A3'},
            'HONDA': {'B1', 'B2'},
            'KAWASAKI': set(),  # пустой набор
            'BRP': {'C1'}
        }
        
        mock_automaton = MagicMock()
        sys.modules['pyahocorasick'].Automaton.return_value = mock_automaton
        
        result = builder.build_all_articles_automatons(brand_articles)
        
        # Что: проверяем что построены только непустые
        assert len(result) == 3  # YAMAHA, HONDA, BRP (не KAWASAKI)
        assert 'YAMAHA' in result
        assert 'HONDA' in result
        assert 'BRP' in result
        assert 'KAWASAKI' not in result  # пустой не добавлен
        assert builder.stats['articles_count'] == 6  # 3+2+1
        assert builder.stats['build_time'] > 0
    
    def test_stats_accumulation(self):
        """Тест накопления статистики"""
        builder = AutomatonBuilder()
        
        mock_automaton = MagicMock()
        sys.modules['pyahocorasick'].Automaton.return_value = mock_automaton
        
        # Что: добавляем несколько брендов
        builder.build_brand_articles_automaton('BRAND1', {'A1', 'A2'})
        assert builder.stats['articles_count'] == 2
        
        builder.build_brand_articles_automaton('BRAND2', {'B1', 'B2', 'B3'})
        assert builder.stats['articles_count'] == 5  # накопительно
        
        builder.build_brand_articles_automaton('BRAND3', {'C1'})
        assert builder.stats['articles_count'] == 6  # накопительно