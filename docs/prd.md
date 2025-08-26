# Система извлечения артикулов автозапчастей Product Requirements Document (PRD)

## Goals and Background Context

### Goals
• Автоматизировать процесс извлечения артикулов из объявлений Avito со 100% автоматизацией
• Достичь точности извлечения артикулов без ложных срабатываний
• Обеспечить обработку 300000+ объявлений за сеанс с высокой производительностью
• Реализовать каскадный алгоритм поиска (бренд→артикул) для повышения точности
• Создать эффективную систему кеширования для работы с ~1 млн записей словаря

### Background Context
Система предназначена для решения проблемы трудоёмкого ручного извлечения артикулов автозапчастей из тысяч неструктурированных объявлений на Avito. Артикулы в объявлениях могут быть написаны с использованием как кириллицы, так и латиницы, с различными вариациями форматирования (дефисы, пробелы, спецсимволы), что делает ручной поиск крайне неэффективным и подверженным ошибкам.

Решение основано на использовании алгоритма Aho-Corasick для эффективного множественного поиска паттернов с каскадным подходом: сначала система находит бренды в тексте, затем ищет только артикулы соответствующих брендов. Это значительно повышает точность и снижает количество ложных срабатываний при работе с базой из ~2 миллионов артикулов.

### Change Log
| Date | Version | Description | Author |
|------|---------|-------------|---------|
| 2025-01-26 | 1.0 | Initial PRD creation | John (PM) |

## Requirements

### Functional
• FR1: Система должна подключаться к PostgreSQL БД и читать данные из таблиц special_model_data и text_model_data
• FR2: Система должна выполнять нормализацию текста: приведение к UPPERCASE, замена кириллицы на латиницу, очистка спецсимволов
• FR3: Система должна загружать и обрабатывать CSV-словарь с ~1 млн записей артикулов и брендов
• FR4: Система должна реализовывать каскадный поиск: сначала находить бренды через Aho-Corasick, затем искать артикулы только найденных брендов
• FR5: Система должна сохранять результаты в таблицу avito_parts_resolved с полями: first_article, brand_near_first_article, all_articles, all_brands
• FR6: Система должна кешировать построенные автоматы Aho-Corasick в pickle и хранить json формате для повторного использования
• FR7: Система должна поддерживать конфигурируемые параметры MIN_ARTICLE_LEN_DIGITS и MIN_ARTICLE_LEN_ALPHANUM через .env файл
• FR8: Система должна вести детальное логирование процесса обработки для отладки и мониторинга
• FR9: Система должна проходить валидацию на эталонном наборе данных Excel с полным совпадением результатов по 30 контрольным записям

### Non Functional
• NFR1: Производительность обработки одного объявления должна быть максимально высокой
• NFR2: Система должна достигать максимальной точности извлечения артикулов без ложных срабатываний
• NFR3: Система должна работать на Python 3.13+ с использованием библиотеки pyahocorasick (последняя версия)
• NFR4: Система должна использовать asyncpg для асинхронной работы с PostgreSQL
• NFR5: Система должна максимально использовать асинхронную обработку там, где это повышает эффективность
• NFR6: Система должна быть модульной с разделением на компоненты: нормализация, поиск, работа с БД
• NFR7: Система должна обрабатывать 300000+ объявлений за один сеанс работы без сбоев
• NFR8: Система должна эффективно использовать 8-ядерный процессор и 16GB RAM

## Technical Assumptions

### Repository Structure: Monorepo
Проект будет организован как монорепозиторий, содержащий все модули системы в единой структуре для упрощения управления зависимостями и версионирования.

### Service Architecture
CRITICAL DECISION - Монолитная архитектура с модульной структурой - все компоненты (нормализация, поиск, БД) работают в рамках единого приложения, но организованы в отдельные модули для изоляции логики и тестирования.

### Testing Requirements
CRITICAL DECISION - Unit тесты для критических функций нормализации и поиска, Integration тесты для проверки работы с БД через asyncpg, Validation тесты на эталонном наборе из 30 записей Excel (структура: id, first_article, brand), Performance тесты для проверки обработки 300000+ записей.

### Additional Technical Assumptions and Requests
• **Python 3.13+** - последняя версия для максимальной производительности asyncio
• **pyahocorasick** - максимальная версия для оптимальной работы алгоритма Aho-Corasick
• **asyncpg** - максимальная версия для высокопроизводительной асинхронной работы с PostgreSQL
• **tqdm** - максимальная версия с поддержкой асинхронного прогресс-бара для отображения процесса обработки
• **PostgreSQL 14+** - для оптимальной производительности и современных возможностей
• **Кеширование** - pickle для автоматов, JSON для метаданных
• **Конфигурация** - python-dotenv максимальной версии для управления .env файлами
• **Логирование** - стандартная библиотека logging для детального логирования процесса
• **Все зависимости** - использовать максимально доступные стабильные версии всех библиотек
• **Параллелизация** - максимальное использование asyncio для утилизации 8 ядер процессора
• **Батч-обработка** - обработка данных пакетами для оптимизации работы с БД
• **Memory management** - контроль использования памяти при работе с большими автоматами
• **Progress tracking** - асинхронный tqdm для визуализации прогресса обработки больших объемов данных

## Epic List

**Epic 1: Foundation & Data Pipeline:** Настройка базовой инфраструктуры проекта, подключение к БД, загрузка и подготовка данных

**Epic 2: Core Extraction Engine:** Реализация основного движка извлечения артикулов с каскадным поиском и нормализацией

**Epic 3: Performance & Validation:** Оптимизация производительности, кеширование и валидация на эталонных данных

## Epic 1 Foundation & Data Pipeline

**Цель эпика:** Создать базовую инфраструктуру проекта с подключением к PostgreSQL, загрузкой CSV-словаря и подготовкой данных для обработки. Этот эпик закладывает фундамент для всей системы, обеспечивая надёжную работу с данными и базовую функциональность.

### Story 1.1 Project Setup and Configuration
As a developer,
I want to set up the project structure with all necessary dependencies,
so that I have a working development environment with latest library versions.

#### Acceptance Criteria
1: Python 3.13+ virtual environment созданы с pip
2: Все зависимости максимальных версий установлены (asyncpg, pyahocorasick, tqdm, python-dotenv)
3: Структура проекта создана с модулями: database/, extractor/, normalizer/, cache/, utils/
4: .env файл настроен с параметрами БД и конфигурации (MIN_ARTICLE_LEN_DIGITS, MIN_ARTICLE_LEN_ALPHANUM)
5: Базовое логирование через logging настроено с ротацией файлов
6: Git репозиторий инициализирован с .gitignore для Python проекта

### Story 1.2 Database Connection Module
As a system,
I want to establish async connection to PostgreSQL database,
so that I can read source data and write results efficiently.

#### Acceptance Criteria
1: Асинхронное подключение к PostgreSQL через asyncpg реализовано
2: Connection pool настроен для оптимальной производительности (min_size=10, max_size=20)
3: Модуль может читать из таблиц special_model_data и text_model_data
4: Модуль может создавать таблицу avito_parts_resolved если она не существует
5: Обработка ошибок подключения с retry логикой (3 попытки с экспоненциальной задержкой)
6: Логирование всех операций с БД включено

### Story 1.3 CSV Dictionary Loader
As a system,
I want to load and parse the CSV dictionary with articles and brands,
so that I can prepare data structures for efficient searching.

#### Acceptance Criteria
1: CSV файл с ~1 млн записей загружается асинхронно с использованием aiofiles
2: Данные парсятся в структуры: dict[brand] -> set[articles]
3: Прогресс загрузки отображается через асинхронный tqdm
4: Валидация данных: пропуск пустых артикулов, проверка минимальной длины
5: Статистика загрузки логируется (количество брендов, артикулов, время загрузки)
6: Обработка ошибок при чтении файла с информативными сообщениями

### Story 1.4 Data Retrieval Pipeline
As a system,
I want to efficiently retrieve advertisements from database in batches,
so that I can process large volumes without memory overflow.

#### Acceptance Criteria
1: Батч-загрузка данных из БД реализована (размер батча = 1000 записей)
2: Асинхронный генератор для потоковой обработки реализован
3: Прогресс отображается через tqdm с общим количеством записей
4: Поддержка фильтрации по дате/статусу обработки добавлена
5: Memory usage мониторится и логируется каждые 10000 записей
6: Graceful shutdown при Ctrl+C с сохранением текущего прогресса

## Epic 2 Core Extraction Engine

**Цель эпика:** Реализовать основной движок извлечения артикулов с алгоритмом Aho-Corasick, каскадным поиском и системой нормализации текста. Это ядро системы, которое выполняет основную бизнес-логику.

### Story 2.1 Text Normalization Module
As a extraction engine,
I want to normalize advertisement texts to standard format,
so that I can reliably match articles regardless of formatting variations.

#### Acceptance Criteria
1: Приведение текста к UPPERCASE реализовано
2: Транслитерация кириллицы в латиницу по таблице соответствий
3: Очистка спецсимволов с сохранением цифр и букв
4: Нормализация пробелов и дефисов (множественные → одиночные)
5: Кеширование результатов нормализации для повторяющихся текстов
6: Unit тесты покрывают все варианты нормализации

### Story 2.2 Aho-Corasick Automaton Builder
As a system,
I want to build efficient Aho-Corasick automatons for pattern matching,
so that I can perform fast multi-pattern search.

#### Acceptance Criteria
1: Отдельные автоматы для брендов и артикулов по брендам созданы
2: Автоматы строятся асинхронно с отображением прогресса через tqdm
3: Использование памяти при построении оптимизировано (инкрементальное добавление)
4: Время построения автоматов логируется
5: Проверка корректности автоматов на тестовых данных
6: Обработка коллизий и дубликатов паттернов

### Story 2.3 Cascade Search Implementation
As a extraction engine,
I want to implement cascade search (brand first, then articles),
so that I can minimize false positives in article detection.

#### Acceptance Criteria
1: Поиск брендов в нормализованном тексте через Aho-Corasick
2: Для каждого найденного бренда - поиск только его артикулов
3: Определение "первого" артикула по позиции в тексте
4: Сбор всех найденных артикулов и брендов в структурированном виде
5: Ассоциация первого артикула с брендом, по которому этот артикул был найден (не по позиции, а по принадлежности к бренду в словаре)
6: Логирование статистики поиска (найдено брендов/артикулов на запись)

### Story 2.4 Result Persistence Module
As a system,
I want to save extraction results to database efficiently,
so that results are available for further processing.

#### Acceptance Criteria
1: Батч-вставка результатов в avito_parts_resolved (batch size = 500)
2: Поля заполняются: first_article, brand_near_first_article, all_articles (JSON), all_brands (JSON)
3: Асинхронная запись с использованием asyncpg
4: Обработка конфликтов при дублировании записей (ON CONFLICT DO UPDATE)
5: Прогресс сохранения отображается через tqdm
6: Rollback при ошибках с сохранением проблемных записей в отдельный лог

## Epic 3 Performance & Validation

**Цель эпика:** Оптимизировать производительность системы через кеширование, параллельную обработку и провести полную валидацию на эталонных данных для подтверждения корректности работы.

### Story 3.1 Automaton Caching System
As a system,
I want to cache built automatons to disk,
so that I can avoid rebuilding them on every run.

#### Acceptance Criteria
1: Сериализация автоматов в pickle формат реализована
2: Метаданные автоматов сохраняются в JSON (версия, дата, статистика)
3: Проверка актуальности хэша по отдельным брендам
4: Автоматическая инвалидация при изменении словаря
5: Загрузка из кеша < 5 секунд для автомата на 1 млн паттернов
6: Размер кеша не превышает 10GB с автоматической очисткой старых версий

### Story 3.2 Parallel Processing Optimization
As a system,
I want to process advertisements in parallel,
so that I can fully utilize available CPU cores.

#### Acceptance Criteria
1: Обработка батчей распараллелена через asyncio (8 воркеров для 8 ядер)
2: Балансировка нагрузки между воркерами реализована
3: Shared memory для автоматов между воркерами
4: Мониторинг CPU utilization и коррекция количества воркеров
5: Graceful degradation при высокой нагрузке
6: Прогресс отображается корректно для параллельной обработки

### Story 3.3 Excel Validation Suite
As a quality assurance,
I want to validate system against reference Excel dataset,
so that I can confirm 100% accuracy on known data.

#### Acceptance Criteria
1: Загрузка эталонного Excel файла с 30 контрольными записями (структура: id как ключ из special_model_data, first_article, brand)
2: Запуск системы на контрольных данных в специальном режиме
3: Сравнение результатов по всем полям (первый артикул и его бренд)
4: Детальный отчёт о расхождениях с указанием конкретных различий
5: Система считается рабочей только при 100% совпадении
6: Автоматический тест валидации в CI/CD pipeline

### Story 3.4 Performance Testing & Monitoring
As a system administrator,
I want comprehensive performance metrics,
so that I can ensure system meets requirements for 300000+ records.

#### Acceptance Criteria
1: Performance тест на 300000+ записей успешно проходит
2: Метрики собираются: записей/сек, RAM usage, CPU usage
3: Bottleneck analysis с профилированием критических секций
4: Мониторинг использования памяти через стандартные метрики системы
5: Детальный performance report генерируется после каждого запуска
6: Автоматические алерты при деградации производительности

## Checklist Results Report

### Executive Summary
- **Overall PRD completeness:** 92%
- **MVP scope appropriateness:** Just Right
- **Readiness for architecture phase:** Ready
- **Most critical gaps:** Отсутствует UI/UX секция (не применимо для CLI-системы)

### Category Analysis Table

| Category                         | Status  | Critical Issues |
| -------------------------------- | ------- | --------------- |
| 1. Problem Definition & Context  | PASS    | Нет             |
| 2. MVP Scope Definition          | PASS    | Нет             |
| 3. User Experience Requirements  | N/A     | CLI-система без UI |
| 4. Functional Requirements       | PASS    | Нет             |
| 5. Non-Functional Requirements   | PASS    | Нет             |
| 6. Epic & Story Structure        | PASS    | Нет             |
| 7. Technical Guidance            | PASS    | Нет             |
| 8. Cross-Functional Requirements | PASS    | Excel формат уточнён |
| 9. Clarity & Communication       | PASS    | Нет             |

### Critical Deficiencies
Нет критических недостатков. Все требования детализированы, структура эталонного Excel файла определена.

### Recommendations
1. Создать architecture document на основе этого PRD
2. Начать с Epic 1 для установки базовой инфраструктуры
3. Подготовить тестовые данные для раннего тестирования

### Final Decision
**READY FOR ARCHITECT:** PRD полностью готов для передачи архитектору.

## Next Steps

### UX Expert Prompt
*Не применимо* - система является CLI-инструментом без пользовательского интерфейса.

### Architect Prompt
Пожалуйста, создайте архитектуру для системы извлечения артикулов автозапчастей на основе этого PRD. Фокусируйтесь на: модульной структуре проекта, асинхронной обработке с asyncpg, эффективном использовании памяти при работе с Aho-Corasick автоматами на 1млн+ паттернов, стратегии кеширования и оптимизации для обработки 300000+ записей. Используйте Python 3.13+ с максимальными версиями всех библиотек.