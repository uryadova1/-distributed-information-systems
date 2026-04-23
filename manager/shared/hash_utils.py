"""Общие утилиты для работы с хэшами."""
import hashlib


def compute_hash(word: str, algorithm: str) -> str:
    """Вычисление хэша слова."""
    if algorithm == "MD5":
        return hashlib.md5(word.encode()).hexdigest()
    raise ValueError(f"Unsupported algorithm: {algorithm}")