"""
Тесты для модуля result_persistence
"""
import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch

from ..result_persistence import ResultPersistence


@pytest.fixture
def sample_results():
    """Тестовые данные результатов"""
    return [
        {
            'ad_id': 1,
            'text_clean': 'TEST TEXT 1',
            'first_article': 'ART001',
            'brand_near_first_article': 'YAMAHA',
            'all_articles': ['ART001', 'ART002'],
            'all_brands': ['YAMAHA']
        },
        {
            'ad_id': 2,
            'text_clean': 'TEST TEXT 2',
            'first_article': 'ART003',
            'brand_near_first_article': 'HONDA',
            'all_articles': ['ART003'],
            'all_brands': ['HONDA', 'SUZUKI']
        }
    ]


@pytest.mark.asyncio
async def test_result_persistence_init():
    """Тест инициализации ResultPersistence"""
    # Что: создание экземпляра с параметрами
    # Зачем: проверка корректной инициализации
    persistence = ResultPersistence(batch_size=100)
    
    assert persistence.batch_size == 100
    assert persistence.total_saved == 0
    assert persistence.total_errors == 0
    assert persistence.problematic_records == []


@pytest.mark.asyncio
async def test_save_batch_empty():
    """Тест сохранения пустого батча"""
    # Что: проверка обработки пустого списка
    # Зачем: защита от ошибок при пустых данных
    persistence = ResultPersistence()
    result = await persistence.save_batch([])
    
    assert result == 0


@pytest.mark.asyncio
async def test_save_results_with_progress_empty():
    """Тест сохранения пустого списка результатов"""
    # Что: проверка обработки пустых данных
    # Зачем: корректное поведение при отсутствии данных
    persistence = ResultPersistence()
    
    stats = await persistence.save_results_with_progress([])
    
    assert stats['total_saved'] == 0
    assert stats['total_errors'] == 0
    assert stats['problematic_records'] == []


@pytest.mark.asyncio
async def test_batch_splitting(sample_results):
    """Тест разбиения на батчи"""
    # Что: проверка корректного разбиения данных на батчи
    # Зачем: убедиться в правильной работе батчирования
    
    # Создаем мок для db_connection.pool
    with patch('src.database.result_persistence.db_connection') as mock_db:
        mock_pool = AsyncMock()
        mock_connection = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_connection
        mock_connection.executemany = AsyncMock()
        mock_db.pool = mock_pool
        
        # Что: создание persistence с маленьким размером батча
        # Зачем: проверка разбиения на несколько батчей
        persistence = ResultPersistence(batch_size=1)
        
        # Генерируем 5 тестовых записей
        test_data = [
            {
                'ad_id': i,
                'text_clean': f'TEXT {i}',
                'first_article': f'ART{i:03d}',
                'brand_near_first_article': 'TEST',
                'all_articles': [f'ART{i:03d}'],
                'all_brands': ['TEST']
            }
            for i in range(5)
        ]
        
        # Что: сохранение с батчами размера 1
        # Зачем: каждая запись должна быть в отдельном батче
        stats = await persistence.save_results_with_progress(test_data)
        
        # Проверяем что executemany вызывался 5 раз (по разу на батч)
        assert mock_connection.executemany.call_count == 5