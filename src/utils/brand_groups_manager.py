"""
Утилита для управления группами брендов
"""
import logging
from pathlib import Path

from ..normalizer.brand_groups import BrandGroupMapper
from ..config import BRAND_GROUPS_PATH

logger = logging.getLogger(__name__)


class BrandGroupsManager:
    """
    Менеджер для управления группами брендов
    """
    
    def __init__(self):
        self.mapper = BrandGroupMapper(BRAND_GROUPS_PATH)
    
    def reload_groups(self) -> None:
        """
        Перезагружает конфигурацию групп брендов
        """
        try:
            self.mapper.reload_groups()
            print("✅ Конфигурация групп брендов успешно перезагружена")
        except Exception as e:
            print(f"❌ Ошибка перезагрузки: {e}")
            logger.error(f"Ошибка перезагрузки групп брендов: {e}")
    
    def test_mapping(self, brand: str) -> None:
        """
        Тестирует маппинг для конкретного бренда
        
        Args:
            brand: бренд для тестирования
        """
        try:
            if not self.mapper.synonym_to_canonical:
                self.mapper.load_groups()
            
            result = self.mapper.map_brand(brand)
            if result != brand.upper():
                print(f"🔄 {brand} -> {result}")
            else:
                print(f"✋ {brand} остается без изменений")
                
        except Exception as e:
            print(f"❌ Ошибка тестирования: {e}")
    
    def show_info(self) -> None:
        """
        Показывает информацию о загруженных группах
        """
        try:
            if not self.mapper.brand_groups:
                self.mapper.load_groups()
            
            print(f"📊 Загружено групп брендов: {len(self.mapper.brand_groups)}")
            print(f"📊 Всего синонимов: {len(self.mapper.synonym_to_canonical)}")
            
            print("\n🏷️  Группы брендов:")
            for canonical, synonyms in self.mapper.brand_groups.items():
                print(f"  {canonical}: {synonyms}")
                
        except Exception as e:
            print(f"❌ Ошибка получения информации: {e}")


def main():
    """
    Главная функция для командной строки
    """
    import sys
    
    manager = BrandGroupsManager()
    
    if len(sys.argv) < 2:
        print("Использование:")
        print("  python -m src.utils.brand_groups_manager reload")
        print("  python -m src.utils.brand_groups_manager test LYNX")
        print("  python -m src.utils.brand_groups_manager info")
        return
    
    command = sys.argv[1].lower()
    
    if command == "reload":
        manager.reload_groups()
    elif command == "test" and len(sys.argv) > 2:
        brand = sys.argv[2]
        manager.test_mapping(brand)
    elif command == "info":
        manager.show_info()
    else:
        print("❌ Неизвестная команда")


if __name__ == "__main__":
    main()