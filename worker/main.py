import hashlib
import time
import asyncio
from typing import Set
from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
import httpx


MANAGER_URL  = "http://manager:8080/internal/api/manager/hash/crack/request"
RETRY_COUNT  = 5
RETRY_DELAY  = 2   # секунд между попытками

app = FastAPI()


class Task(BaseModel):
    requestId:  str
    hash:       str
    partNumber: int
    partCount:  int
    algorithm:  str
    alphabet:   str
    maxLength:  int


class CancelRequest(BaseModel):
    requestId: str



# requestId, которые нужно прервать
_cancelled: Set[str] = set()
_cancelled_lock = asyncio.Lock()



def _estimate_combinations(alphabet_len: int, max_length: int) -> int:
    return sum(alphabet_len ** i for i in range(1, max_length + 1))


def _index_to_word(index: int, alphabet: str, max_length: int) -> str:
    """Переводит глобальный индекс (по всем длинам) в слово."""
    base = len(alphabet)
    for length in range(1, max_length + 1):
        count = base ** length
        if index < count:
            return _number_to_word(index, alphabet, length)
        index -= count
    return ""   # не должно случиться при корректном диапазоне


def _number_to_word(number: int, alphabet: str, length: int) -> str:
    base  = len(alphabet)
    chars = []
    for _ in range(length):
        chars.append(alphabet[number % base])
        number //= base
    return "".join(reversed(chars))


def _hash_word(word: str, algorithm: str) -> str:
    algo = algorithm.upper()
    if algo == "MD5":
        return hashlib.md5(word.encode()).hexdigest()
    if algo == "SHA1":
        return hashlib.sha1(word.encode()).hexdigest()
    if algo == "SHA256":
        return hashlib.sha256(word.encode()).hexdigest()
    raise ValueError(f"Unsupported algorithm: {algorithm}")



def _run_crack(task: Task) -> dict:
    """
    Синхронная функция — выполняется в asyncio.to_thread(),
    чтобы не блокировать event loop.
    """
    alphabet   = task.alphabet
    total      = _estimate_combinations(len(alphabet), task.maxLength)
    start_idx  = total * task.partNumber  // task.partCount
    end_idx    = total * (task.partNumber + 1) // task.partCount

    target_hash = task.hash.lower()
    results     = []
    checked     = 0
    start_time  = time.time()

    for i in range(start_idx, end_idx):
        # проверяем отмену каждые 1000 слов (без lock — читаем set напрямую)
        if checked % 1000 == 0 and task.requestId in _cancelled:
            break

        word = _index_to_word(i, alphabet, task.maxLength)
        if word is None:
            continue

        checked += 1
        try:
            h = _hash_word(word, task.algorithm)
        except ValueError:
            break

        if h == target_hash:
            results.append(word)

    return {
        "requestId":    task.requestId,
        "partNumber":   task.partNumber,
        "results":      results,
        "checked":      checked,
        "executionTime": time.time() - start_time,
    }


async def _report_to_manager(payload: dict) -> None:
    """Отправляет результат менеджеру; повторяет RETRY_COUNT раз при ошибке."""
    async with httpx.AsyncClient(timeout=10) as client:
        for attempt in range(RETRY_COUNT):
            try:
                r = await client.patch(MANAGER_URL, json=payload)
                r.raise_for_status()
                return
            except Exception as e:
                if attempt < RETRY_COUNT - 1:
                    await asyncio.sleep(RETRY_DELAY)
                # Если все попытки провалились — молча завершаем.
                # Менеджер-watchdog переназначит задачу.




async def _process_and_report(task: Task) -> None:
    """Запускает вычисление в потоке и отправляет результат менеджеру."""
    payload = await asyncio.to_thread(_run_crack, task)
    await _report_to_manager(payload)



@app.post("/internal/api/worker/hash/crack/task")
async def receive_task(task: Task, background_tasks: BackgroundTasks):
    """
    Принимает задачу от менеджера.
    Возвращает 202 немедленно; вычисление идёт в фоне.
    """
    # Убираем из отменённых (воркер может получить ту же задачу повторно)
    _cancelled.discard(task.requestId)

    background_tasks.add_task(_process_and_report, task)
    return {"status": "ACCEPTED"}


@app.post("/internal/api/worker/hash/crack/cancel")
async def cancel_task(body: CancelRequest):
    """Менеджер сообщает, что запрос отменён — прерываем вычисление."""
    async with _cancelled_lock:
        _cancelled.add(body.requestId)
    return {"status": "CANCELLED"}


@app.get("/health")
def health():
    """Health-check для docker-compose и менеджера."""
    return {"status": "ok"}