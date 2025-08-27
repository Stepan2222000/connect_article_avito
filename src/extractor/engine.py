"""
Главный движок системы извлечения артикулов
"""
import asyncio
import logging
import time
from typing import List, Dict, Any, Optional
from pathlib import Path

from ..database.connection import db_connection
from ..database.data_retrieval import DataRetriever
from ..database.result_persistence import ResultPersistence
from ..normalizer.text_normalizer import normalize_text_for_search
# Кеширование отключено - работаем только с данными текущего запуска
from .csv_loader import CSVDictionaryLoader
from tqdm.asyncio import tqdm
from .automaton_builder import AutomatonBuilder
from .cascade_search import CascadeSearchEngine
from ..config import (
    CSV_DICTIONARY_PATH,
    BATCH_SIZE,
    MAX_WORKERS,
)

logger = logging.getLogger(__name__)


class ExtractionEngine:
    """
    Главный движок системы извлечения артикулов из объявлений
    """
    
    def __init__(self):
        """
        Инициализация движка
        """
        # Что: загрузчик CSV словаря
        # Зачем: чтение артикулов и брендов из файла
        self.csv_loader = CSVDictionaryLoader(CSV_DICTIONARY_PATH)
        
        # Что: построитель автоматов Aho-Corasick
        # Зачем: создание структур для быстрого поиска
        self.automaton_builder = AutomatonBuilder()
        
        # Что: движок каскадного поиска
        # Зачем: поиск брендов → артикулов
        self.cascade_engine = CascadeSearchEngine()
        
        # Что: система кеширования автоматов
        # Зачем: ускорение повторных запусков
        # Кеширование отключено
        
        # Что: модуль сохранения результатов
        # Зачем: батчевая запись в БД с обработкой ошибок
        self.persistence = ResultPersistence(batch_size=BATCH_SIZE)
        
        # Что: словарь бренд → артикулы
        # Зачем: хранение загруженного словаря
        self.brand_articles: Dict[str, set] = {}
        
        # Что: статистика обработки
        # Зачем: мониторинг производительности
        self.stats = {
            'total_processed': 0,
            'articles_found': 0,
            'brands_found': 0,
            'processing_time': 0.0
        }
        
        logger.info("Инициализирован главный движок извлечения артикулов")
    
    async def load_dictionary(self) -> None:
        """
        Загрузка словаря артикулов из CSV
        """
        logger.info("Загрузка словаря артикулов...")
        
        # Что: загружаем словарь из CSV
        # Зачем: получаем структуру бренд → артикулы
        self.brand_articles = await self.csv_loader.load_dictionary()
        
        logger.info(
            f"Словарь загружен: {len(self.brand_articles)} брендов, "
            f"{self.csv_loader.stats['valid_articles']} артикулов"
        )
    
    async def build_automatons(self) -> None:
        """
        Построение автоматов Aho-Corasick
        """
        logger.info("Построение автоматов для поиска...")
        
        # Что: всегда строим автоматы заново
        # Зачем: работаем только с данными текущего запуска
        logger.info("Построение новых автоматов...")
        
        # Что: строим автомат брендов
        # Зачем: первый этап каскадного поиска
        brands = set(self.brand_articles.keys())
        brands_automaton = self.automaton_builder.build_brands_automaton(brands)
        
        # Что: строим автоматы артикулов для каждого бренда
        # Зачем: второй этап каскадного поиска
        brand_articles_automatons = {}
        for brand, articles in self.brand_articles.items():
            if articles:  # только для брендов с артикулами
                automaton = self.automaton_builder.build_brand_articles_automaton(brand, articles)
                brand_articles_automatons[brand] = automaton
        
        logger.info(f"Построено {len(brand_articles_automatons)} автоматов артикулов")
        
        # Что: передаем автоматы в движок поиска
        # Зачем: готовность к каскадному поиску
        self.cascade_engine.set_automatons(brands_automaton, brand_articles_automatons)
        
        # Что: кеширование отключено
        # Зачем: работаем только с данными текущего запуска
    
    async def process_batch(self, ads: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Обработка батча объявлений
        
        Args:
            ads: список объявлений для обработки
            
        Returns:
            Список результатов с найденными артикулами
        """
        results = []
        
        for ad in ads:
            # Что: извлекаем текст и нормализуем
            # Зачем: подготовка к поиску артикулов
            text_raw = ad.get('text_raw', '')
            text_clean = normalize_text_for_search(text_raw)
            
            # Что: выполняем каскадный поиск
            # Зачем: находим бренды → их артикулы
            search_result = await self.cascade_engine.search_articles(text_clean)
            
            # Что: формируем результат для сохранения
            # Зачем: структурированные данные для БД
            result = {
                'ad_id': ad['ad_id'],
                'text_clean': text_clean,
                'first_article': search_result.first_article,
                'brand_near_first_article': search_result.brand_near_first_article,
                'all_articles': search_result.all_articles,
                'all_brands': search_result.all_brands
            }
            
            results.append(result)
            
            # Что: обновляем статистику
            # Зачем: мониторинг эффективности
            if search_result.first_article:
                self.stats['articles_found'] += 1
            if search_result.all_brands:
                self.stats['brands_found'] += 1
        
        return results
    
    async def process_advertisements(
        self, 
        limit: Optional[int] = None,
        batch_size: int = BATCH_SIZE
    ) -> Dict[str, Any]:
        """
        Основной метод обработки объявлений из БД с батчированной загрузкой
        
        Args:
            limit: ограничение количества объявлений
            batch_size: размер батча для обработки
            
        Returns:
            Статистика обработки
        """
        start_time = time.time()
        logger.info(f"Начало обработки объявлений (limit={limit}, batch_size={batch_size})")
        
        # Что: проверяем готовность движка поиска
        # Зачем: автоматы должны быть построены
        if not self.cascade_engine.brands_automaton:
            logger.error("Автоматы не построены. Вызовите build_automatons() сначала")
            raise RuntimeError("Автоматы не построены")
        
        # Что: создаем загрузчик данных с батчированием
        # Зачем: избегаем зависания при загрузке больших объемов
        data_retriever = DataRetriever(batch_size=batch_size)
        
        try:
            await data_retriever.connect()
            
            # Что: получаем общее количество для прогресса
            # Зачем: показываем прогресс обработки
            total_count = await data_retriever.get_total_count(processed_status=False)
            
            if limit:
                total_count = min(total_count, limit)
            
            if total_count == 0:
                logger.warning("Нет объявлений для обработки")
                return self.stats
            
            logger.info(f"Найдено {total_count} объявлений для обработки")
            
            # Что: создаем прогресс-бар для обработки
            # Зачем: визуализация прогресса для пользователя
            processing_progress = tqdm(
                total=total_count,
                desc="Обработка объявлений",
                unit="объявлений"
            )
            
            # Что: загружаем и обрабатываем данные батчами
            # Зачем: контролируемое использование памяти
            all_results = []
            batch_num = 0
            processed = 0
            
            # Что: используем асинхронный генератор для потоковой обработки
            # Зачем: данные загружаются по мере обработки, не все сразу
            async for batch in data_retriever.fetch_batch_data(
                processed_status=False,
                show_progress=False  # У нас свой прогресс-бар
            ):
                batch_num += 1
                
                # Что: ограничиваем количество если задан limit
                # Зачем: соблюдаем заданное ограничение
                if limit and processed >= limit:
                    break
                
                if limit and processed + len(batch) > limit:
                    batch = batch[:limit - processed]
                
                logger.debug(f"Обработка батча {batch_num} ({len(batch)} объявлений)")
                
                # Что: обрабатываем батч
                # Зачем: поиск артикулов в объявлениях
                batch_results = await self.process_batch(batch)
                all_results.extend(batch_results)
                
                processed += len(batch)
                self.stats['total_processed'] = processed
                
                # Что: обновляем прогресс-бар
                # Зачем: показываем текущий прогресс
                processing_progress.update(len(batch))
                
                # Что: логируем промежуточный прогресс каждые 10 батчей
                # Зачем: мониторинг скорости обработки
                if batch_num % 10 == 0:
                    elapsed = time.time() - start_time
                    speed = processed / elapsed if elapsed > 0 else 0
                    logger.info(
                        f"Прогресс: {processed}/{total_count} "
                        f"({speed:.1f} объявлений/сек)"
                    )
            
            processing_progress.close()
            
            # Что: сохраняем результаты в БД
            # Зачем: персистентное хранение найденных артикулов
            logger.info(f"Сохранение {len(all_results)} результатов в БД...")
            save_stats = await self.persistence.save_results_with_progress(
                all_results,
                description="Сохранение результатов"
            )
            
            # Что: финальная статистика
            # Зачем: отчет о проделанной работе
            self.stats['processing_time'] = time.time() - start_time
            self.stats['save_stats'] = save_stats
            
            logger.info(
                f"Обработка завершена за {self.stats['processing_time']:.1f} сек:\n"
                f"  - Обработано: {self.stats['total_processed']} объявлений\n"
                f"  - Найдено артикулов: {self.stats['articles_found']}\n"
                f"  - Найдено брендов: {self.stats['brands_found']}\n"
                f"  - Сохранено в БД: {save_stats['total_saved']}\n"
                f"  - Ошибок сохранения: {save_stats['total_errors']}"
            )
            
        finally:
            # Что: закрываем соединение с БД
            # Зачем: освобождаем ресурсы
            await data_retriever.disconnect()
        
        return self.stats


async def run_extraction(limit: Optional[int] = None) -> Dict[str, Any]:
    """
    Запуск полного цикла извлечения артикулов
    
    Args:
        limit: ограничение количества объявлений
        
    Returns:
        Статистика обработки
    """
    logger.info("=" * 60)
    logger.info("ЗАПУСК СИСТЕМЫ ИЗВЛЕЧЕНИЯ АРТИКУЛОВ")
    logger.info("=" * 60)
    
    try:
        # Что: подключаемся к БД
        # Зачем: доступ к данным объявлений
        logger.info("Подключение к БД...")
        await db_connection.connect()
        
        # Что: создаем таблицу результатов если не существует
        # Зачем: подготовка хранилища результатов
        await db_connection.create_table_avito_parts_resolved()
        
        # Что: создаем и инициализируем движок
        # Зачем: основной компонент обработки
        engine = ExtractionEngine()
        
        # Что: загружаем словарь
        # Зачем: данные для поиска артикулов
        await engine.load_dictionary()
        
        # Что: строим автоматы для поиска
        # Зачем: структуры для эффективного поиска
        await engine.build_automatons()
        
        # Что: обрабатываем объявления
        # Зачем: основная работа - извлечение артикулов
        stats = await engine.process_advertisements(limit=limit)
        
        return stats
        
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
        raise
        
    finally:
        # Что: закрываем подключение к БД
        # Зачем: освобождение ресурсов
        await db_connection.disconnect()
        logger.info("Подключение к БД закрыто")