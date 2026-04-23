import hashlib
import itertools
from typing import Generator, List


def generate_words(alphabet: str, max_length: int) -> Generator[str, None, None]:
    """Генератор слов заданной длины из алфавита."""
    for length in range(1, max_length + 1):
        for combo in itertools.product(alphabet, repeat=length):
            yield ''.join(combo)


def count_combinations(alphabet: str, max_length: int) -> int:
    """Подсчёт общего количества комбинаций."""
    total = 0
    for length in range(1, max_length + 1):
        total += len(alphabet) ** length
    return total


def compute_hash(word: str, algorithm: str) -> str:
    """Вычисление хэша слова."""
    if algorithm == "MD5":
        return hashlib.md5(word.encode()).hexdigest()
    raise ValueError(f"Unsupported algorithm: {algorithm}")


def get_word_at_index(alphabet: str, max_length: int, index: int) -> str:
    """Получение слова по индексу в лексикографическом порядке."""
    if index < 0:
        raise IndexError("Index must be non-negative")

    current_index = 0
    for length in range(1, max_length + 1):
        combinations_in_length = len(alphabet) ** length
        if current_index + combinations_in_length > index:
            # Слово находится в текущей длине
            relative_index = index - current_index
            word = []
            for pos in range(length):
                pos_combinations = len(alphabet) ** (length - pos - 1)
                char_index = relative_index // pos_combinations
                word.append(alphabet[char_index])
                relative_index %= pos_combinations
            return ''.join(word)
        current_index += combinations_in_length

    raise IndexError("Index out of range")


def get_range_for_part(
        alphabet: str,
        max_length: int,
        part_number: int,
        part_count: int
) -> tuple[int, int]:
    """Вычисление диапазона индексов для части задачи."""
    total = count_combinations(alphabet, max_length)
    part_size = total // part_count
    remainder = total % part_count

    # Распределяем остаток по первым частям
    if part_number < remainder:
        start = part_number * (part_size + 1)
        end = start + part_size + 1
    else:
        start = remainder * (part_size + 1) + (part_number - remainder) * part_size
        end = start + part_size

    return start, min(end, total)