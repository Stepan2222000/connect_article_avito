"""
Простая конфигурация приложения из .env файла
"""
import os
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Конфигурация БД
DATABASE_HOST = os.getenv("DATABASE_HOST", "localhost")
DATABASE_PORT = int(os.getenv("DATABASE_PORT", "5432"))
DATABASE_NAME = os.getenv("DATABASE_NAME", "avito")
DATABASE_USER = os.getenv("DATABASE_USER", "postgres")
DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD", "postgres")

# Пул соединений
DATABASE_MIN_CONNECTIONS = int(os.getenv("DATABASE_MIN_CONNECTIONS", "10"))
DATABASE_MAX_CONNECTIONS = int(os.getenv("DATABASE_MAX_CONNECTIONS", "20"))

# Обработка артикулов
MIN_ARTICLE_LEN_DIGITS = int(os.getenv("MIN_ARTICLE_LEN_DIGITS", "3"))  # Мин длина для цифр
MIN_ARTICLE_LEN_ALPHANUM = int(os.getenv("MIN_ARTICLE_LEN_ALPHANUM", "4"))  # Мин длина с буквами

# Пути
CSV_DICTIONARY_PATH = os.getenv("CSV_DICTIONARY_PATH", "./data/articles_dictionary.csv")
BRAND_GROUPS_PATH = os.getenv("BRAND_GROUPS_PATH", "./data/brand_groups.json")
CACHE_DIR = os.getenv("CACHE_DIR", "./cache")
LOG_FILE = os.getenv("LOG_FILE", "./logs/app.log")

# Настройки обработки
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "8"))

# Логирование
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_MAX_BYTES = int(os.getenv("LOG_MAX_BYTES", "10485760"))  # 10MB
LOG_BACKUP_COUNT = int(os.getenv("LOG_BACKUP_COUNT", "5"))