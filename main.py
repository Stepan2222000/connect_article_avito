#!/usr/bin/env python3
"""
Главная точка входа в систему извлечения артикулов автозапчастей
"""
import asyncio
import argparse
import sys
from pathlib import Path
import time

# Что: добавляем корневую директорию в путь
# Зачем: для корректного импорта модулей
sys.path.insert(0, str(Path(__file__).parent))

from src.utils.logging_config import setup_logging
from src.extractor.engine import run_extraction
from src.database.connection import db_connection

# Что: настраиваем логирование при импорте модуля
# Зачем: логи должны работать сразу
logger = setup_logging()


async def main():
    """
    Главная функция приложения
    """
    # Что: парсер аргументов командной строки
    # Зачем: гибкое управление параметрами запуска
    parser = argparse.ArgumentParser(
        description="Система извлечения артикулов автозапчастей из объявлений Avito"
    )
    
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Ограничение количества обрабатываемых объявлений (по умолчанию: все)"
    )
    
    parser.add_argument(
        "--test-connection",
        action="store_true",
        help="Только проверить подключение к БД и выйти"
    )
    
    # Параметр --no-cache убран, так как кеширование отключено
    
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Размер батча для обработки (по умолчанию: 1000)"
    )
    
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("СИСТЕМА ИЗВЛЕЧЕНИЯ АРТИКУЛОВ АВТОЗАПЧАСТЕЙ")
    logger.info("=" * 60)
    
    # Что: режим проверки подключения
    # Зачем: тестирование конфигурации БД без запуска обработки
    if args.test_connection:
        logger.info("Режим проверки подключения к БД...")
        try:
            await db_connection.connect()
            logger.info("✓ Подключение к БД успешно установлено")
            
            # Что: проверяем доступность таблиц
            # Зачем: убедиться что схема БД корректна
            count = await db_connection.fetchval(
                "SELECT COUNT(*) FROM public.special_model_data"
            )
            logger.info(f"✓ Таблица special_model_data доступна ({count} записей)")
            
            count = await db_connection.fetchval(
                "SELECT COUNT(*) FROM public.text_model_data"
            )
            logger.info(f"✓ Таблица text_model_data доступна ({count} записей)")
            
            # Что: проверяем таблицу результатов
            # Зачем: готовность к сохранению результатов
            table_exists = await db_connection.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'avito_parts_resolved'
                )
            """)
            
            if table_exists:
                count = await db_connection.fetchval(
                    "SELECT COUNT(*) FROM public.avito_parts_resolved"
                )
                logger.info(f"✓ Таблица avito_parts_resolved существует ({count} записей)")
            else:
                logger.info("○ Таблица avito_parts_resolved будет создана при первом запуске")
            
            await db_connection.disconnect()
            logger.info("✓ Тест подключения завершен успешно")
            return 0
            
        except Exception as e:
            logger.error(f"✗ Ошибка подключения к БД: {e}")
            return 1
    
    # Что: основной режим работы
    # Зачем: запуск полного цикла извлечения артикулов
    try:
        logger.info("Параметры запуска:")
        logger.info(f"  - Лимит объявлений: {args.limit or 'без ограничений'}")
        logger.info(f"  - Кеширование: отключено (всегда строим автоматы заново)")
        logger.info(f"  - Размер батча: {args.batch_size}")
        logger.info("")
        
        start_time = time.time()
        
        # Что: запускаем основную обработку
        # Зачем: извлечение артикулов из объявлений
        stats = await run_extraction(limit=args.limit)
        
        elapsed = time.time() - start_time
        
        # Что: итоговая статистика
        # Зачем: отчет о проделанной работе
        logger.info("")
        logger.info("=" * 60)
        logger.info("ИТОГОВАЯ СТАТИСТИКА")
        logger.info("=" * 60)
        logger.info(f"Общее время работы: {elapsed:.1f} сек")
        logger.info(f"Обработано объявлений: {stats['total_processed']}")
        
        if stats['total_processed'] > 0:
            logger.info(f"Средняя скорость: {stats['total_processed'] / elapsed:.1f} объявлений/сек")
            logger.info(f"Найдено артикулов: {stats['articles_found']} ({stats['articles_found'] * 100 / stats['total_processed']:.1f}%)")
            logger.info(f"Найдено брендов: {stats['brands_found']} ({stats['brands_found'] * 100 / stats['total_processed']:.1f}%)")
            
            if 'save_stats' in stats:
                save_stats = stats['save_stats']
                logger.info(f"Сохранено в БД: {save_stats['total_saved']}")
                if save_stats['total_errors'] > 0:
                    logger.warning(f"Ошибок при сохранении: {save_stats['total_errors']}")
        
        logger.info("=" * 60)
        logger.info("✓ РАБОТА ЗАВЕРШЕНА УСПЕШНО")
        logger.info("=" * 60)
        return 0
        
    except KeyboardInterrupt:
        logger.warning("\n⚠ Работа прервана пользователем")
        return 130
        
    except Exception as e:
        logger.error(f"\n✗ КРИТИЧЕСКАЯ ОШИБКА: {e}", exc_info=True)
        return 1


def run():
    """
    Точка входа для запуска через командную строку
    """
    try:
        # Что: запускаем асинхронную главную функцию
        # Зачем: вся система построена на asyncio
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
        
    except KeyboardInterrupt:
        print("\n⚠ Работа прервана пользователем")
        sys.exit(130)
        
    except Exception as e:
        print(f"\n✗ Критическая ошибка: {e}")
        sys.exit(1)


if __name__ == "__main__":
    run()