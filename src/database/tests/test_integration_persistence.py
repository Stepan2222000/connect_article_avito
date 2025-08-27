"""
Интеграционный тест для проверки работы ResultPersistence с реальной БД
"""
import asyncio
import logging
from datetime import datetime

from ..connection import db_connection
from ..result_persistence import ResultPersistence

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_integration():
    """
    Интеграционный тест с реальным подключением к БД
    """
    try:
        # Что: подключение к БД
        # Зачем: проверка работы с реальной БД
        await db_connection.connect()
        logger.info("Подключение к БД успешно")
        
        # Что: проверка/создание таблицы
        # Зачем: убедиться что таблица существует
        await db_connection.create_table_avito_parts_resolved()
        logger.info("Таблица avito_parts_resolved готова")
        
        # Что: создание тестовых данных
        # Зачем: симуляция реальных результатов извлечения
        test_results = [
            {
                'ad_id': 10000000 + i,
                'text_clean': f'NORMALIZED TEXT FOR AD {i}',
                'first_article': f'TEST{i:05d}',
                'brand_near_first_article': ['YAMAHA', 'HONDA', 'SUZUKI'][i % 3],
                'all_articles': [f'TEST{i:05d}', f'ALT{i:05d}'],
                'all_brands': [['YAMAHA'], ['HONDA'], ['SUZUKI']][i % 3]
            }
            for i in range(1500)  # Создаем 1500 записей для проверки батчирования
        ]
        
        # Что: создание экземпляра ResultPersistence
        # Зачем: тестирование модуля сохранения
        persistence = ResultPersistence(batch_size=500)
        
        # Что: сохранение результатов с прогрессом
        # Зачем: проверка полного цикла сохранения
        stats = await persistence.save_results_with_progress(
            test_results,
            description="Тестовое сохранение"
        )
        
        logger.info(f"Статистика сохранения: {stats}")
        
        # Что: проверка сохраненных данных
        # Зачем: убедиться что данные действительно в БД
        count = await db_connection.fetchval(
            "SELECT COUNT(*) FROM public.avito_parts_resolved WHERE ad_id >= 10000000"
        )
        logger.info(f"Количество записей в БД: {count}")
        
        # Что: очистка тестовых данных
        # Зачем: не оставлять тестовые данные в БД
        await db_connection.execute(
            "DELETE FROM public.avito_parts_resolved WHERE ad_id >= 10000000"
        )
        logger.info("Тестовые данные очищены")
        
    except Exception as e:
        logger.error(f"Ошибка в интеграционном тесте: {e}")
        raise
        
    finally:
        # Что: закрытие соединения
        # Зачем: освобождение ресурсов
        await db_connection.disconnect()
        logger.info("Соединение с БД закрыто")


if __name__ == "__main__":
    # Запуск интеграционного теста
    asyncio.run(test_integration())