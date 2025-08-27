"""
Unit тесты для модуля нормализации текста
"""
import pytest
from ..text_normalizer import (
    normalize_text_for_search,
    normalize_article_for_search, 
    normalize_article_for_storage,
    clear_normalization_cache
)


class TestTextNormalizer:
    """Тесты нормализации текста для поиска"""
    
    def test_empty_text(self):
        """Тест пустого текста"""
        assert normalize_text_for_search("") == ""
        assert normalize_text_for_search(None) == ""
    
    def test_uppercase_conversion(self):
        """Тест приведения к UPPERCASE"""
        assert normalize_text_for_search("abc 123") == "ABC 123"
        assert normalize_text_for_search("Test Article") == "TEST ARTICLE"
    
    def test_cyrillic_to_latin(self):
        """Тест замены кириллицы на латиницу"""
        assert normalize_text_for_search("АВС") == "ABC"
        assert normalize_text_for_search("РОКЕТ") == "POKET"  
        assert normalize_text_for_search("КАМАЗ") == "KAMAZ"
        assert normalize_text_for_search("АВС-123") == "ABC 123"  # дефис заменен на пробел
    
    def test_dash_removal(self):
        """Тест удаления дефисов"""
        assert normalize_text_for_search("ABC-123") == "ABC 123"
        assert normalize_text_for_search("TEST-PART-456") == "TEST PART 456"
    
    def test_special_chars_cleanup(self):
        """Тест очистки спецсимволов с сохранением цифр и букв"""
        assert normalize_text_for_search("ABC@123#DEF") == "ABC 123 DEF"
        assert normalize_text_for_search("TEST!@#$%456") == "TEST 456"
        assert normalize_text_for_search("PART(123)") == "PART 123"
    
    def test_spaces_normalization(self):
        """Тест нормализации пробелов"""
        assert normalize_text_for_search("ABC   123") == "ABC 123"
        assert normalize_text_for_search("  TEST  PART  ") == "TEST PART"
        assert normalize_text_for_search("A\t\nB\r\nC") == "A B C"
    
    def test_complex_normalization(self):
        """Тест комплексной нормализации"""
        input_text = "тест-АВС@123  дефолт##456"
        expected = "TEST ABC 123 DEFOLT 456"
        assert normalize_text_for_search(input_text) == expected


class TestArticleForSearch:
    """Тесты нормализации артикулов для поиска"""
    
    def test_empty_article(self):
        """Тест пустого артикула"""
        assert normalize_article_for_search("") == ""
        assert normalize_article_for_search(None) == ""
    
    def test_dash_removal_in_search(self):
        """Тест удаления дефисов в поиске"""
        assert normalize_article_for_search("ABC-123") == "ABC 123"
        assert normalize_article_for_search("TEST-PART-456") == "TEST PART 456"
    
    def test_cyrillic_in_articles(self):
        """Тест кириллицы в артикулах"""
        assert normalize_article_for_search("АВС-123") == "ABC 123"
        assert normalize_article_for_search("РОКЕТ456") == "POKET456"


class TestArticleForStorage:
    """Тесты нормализации артикулов для хранения"""
    
    def test_empty_article(self):
        """Тест пустого артикула"""
        assert normalize_article_for_storage("") == ""
        assert normalize_article_for_storage(None) == ""
    
    def test_dash_preservation(self):
        """Тест сохранения дефисов при хранении"""
        assert normalize_article_for_storage("ABC-123") == "ABC-123"
        assert normalize_article_for_storage("TEST-PART-456") == "TEST-PART-456"
    
    def test_cyrillic_conversion_with_dashes(self):
        """Тест замены кириллицы с сохранением дефисов"""
        assert normalize_article_for_storage("АВС-123") == "ABC-123"
        assert normalize_article_for_storage("РОКЕТ-456") == "POKET-456"
    
    def test_special_chars_cleanup_preserve_dashes(self):
        """Тест очистки спецсимволов с сохранением дефисов"""
        assert normalize_article_for_storage("ABC@123-DEF") == "ABC 123-DEF"
        assert normalize_article_for_storage("TEST!@#-456") == "TEST -456"
    
    def test_spaces_normalization_with_dashes(self):
        """Тест нормализации пробелов с сохранением дефисов"""
        assert normalize_article_for_storage("ABC  -  123") == "ABC - 123"
        assert normalize_article_for_storage("  TEST-PART  ") == "TEST-PART"


class TestCaching:
    """Тесты кеширования"""
    
    def test_cache_consistency(self):
        """Тест консистентности кеша"""
        # Что: проверяем что кеш возвращает те же результаты
        text = "TEST-АВС@123"
        result1 = normalize_text_for_search(text)
        result2 = normalize_text_for_search(text)
        assert result1 == result2
    
    def test_cache_clear(self):
        """Тест очистки кеша"""
        # Что: проверяем что функция очистки не падает
        clear_normalization_cache()
        # Зачем: после очистки функции должны работать как обычно
        assert normalize_text_for_search("test") == "TEST"


class TestEdgeCases:
    """Тесты граничных случаев"""
    
    def test_only_special_chars(self):
        """Тест только спецсимволов"""
        assert normalize_text_for_search("!@#$%^&*()") == ""
        assert normalize_article_for_storage("!@#-$%^") == "-"
    
    def test_only_dashes(self):
        """Тест только дефисов"""
        assert normalize_text_for_search("---") == ""
        assert normalize_article_for_storage("---") == "---"
    
    def test_mixed_alphabets(self):
        """Тест смешанных алфавитов"""
        assert normalize_text_for_search("ABCабв123") == "ABCABV123"
        assert normalize_article_for_storage("ABC-абв-123") == "ABC-ABV-123"