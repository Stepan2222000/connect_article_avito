"""
Конфигурация логирования с ротацией файлов
"""
import logging
import logging.handlers
import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()


def setup_logging(
    log_level: Optional[str] = None,
    log_file: Optional[str] = None,
    max_bytes: Optional[int] = None,
    backup_count: Optional[int] = None,
) -> logging.Logger:
    """
    Настройка логирования с ротацией файлов
    
    Args:
        log_level: Уровень логирования (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Путь к файлу логов
        max_bytes: Максимальный размер файла логов в байтах
        backup_count: Количество резервных копий логов
    
    Returns:
        Настроенный логгер
    """
    # Получаем параметры из .env или используем значения по умолчанию
    log_level = log_level or os.getenv("LOG_LEVEL", "INFO")
    log_file = log_file or os.getenv("LOG_FILE", "./logs/app.log")
    max_bytes = max_bytes or int(os.getenv("LOG_MAX_BYTES", "10485760"))  # 10MB
    backup_count = backup_count or int(os.getenv("LOG_BACKUP_COUNT", "5"))
    
    # Создаём директорию для логов если она не существует
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Настраиваем формат логов
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Настраиваем корневой логгер
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Очищаем существующие обработчики
    logger.handlers.clear()
    
    # Консольный обработчик
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Файловый обработчик с ротацией
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger


def get_logger(name: str) -> logging.Logger:
    """
    Получить логгер для модуля
    
    Args:
        name: Имя модуля
    
    Returns:
        Логгер для модуля
    """
    return logging.getLogger(name)