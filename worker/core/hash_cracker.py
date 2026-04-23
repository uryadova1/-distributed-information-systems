"""Воркер: выполнение части задачи по перебору."""
import asyncio
import time
from typing import List, Optional


from shared.hash_utils import compute_hash
from shared.word_generator import get_word_at_index, get_range_for_part


async def crack_part(
        hash_target: str,
        algorithm: str,
        alphabet: str,
        max_length: int,
        part_number: int,
        part_count: int,
        cancel_flag: Optional[asyncio.Event] = None
) -> tuple[List[str], int, float]:
    """
    Выполнение части задачи по перебору.

    Returns:
        (found_words, checked_count, execution_time)
    """
    start_idx, end_idx = get_range_for_part(alphabet, max_length, part_number, part_count)
    found = []
    checked = 0
    start_time = time.time()

    for idx in range(start_idx, end_idx):
        if cancel_flag and cancel_flag.is_set():
            break

        try:
            word = get_word_at_index(alphabet, max_length, idx)
            word_hash = compute_hash(word, algorithm)
            checked += 1

            if word_hash == hash_target:
                found.append(word)
        except Exception:
            continue  # Пропускаем ошибки, продолжаем перебор

    execution_time = time.time() - start_time
    return found, checked, execution_time