import hashlib
import time
import asyncio
from fastapi import FastAPI
from pydantic import BaseModel
import httpx

MANAGER_URL = "http://manager:8080/internal/api/manager/hash/crack/request"
RETRY_COUNT = 5

app = FastAPI()


class Task(BaseModel):
    requestId: str
    hash: str
    partNumber: int
    partCount: int
    algorithm: str
    alphabet: str
    maxLength: int


def estimate_combinations(alphabet_len: int, max_length: int):
    return sum(alphabet_len ** i for i in range(1, max_length + 1))


def index_to_word(index, alphabet, max_length):
    base = len(alphabet)

    for length in range(1, max_length + 1):
        count = base ** length
        if index < count:
            return number_to_word(index, alphabet, length)
        index -= count


def number_to_word(number, alphabet, length):
    base = len(alphabet)
    chars = []
    for _ in range(length):
        chars.append(alphabet[number % base])
        number //= base
    return ''.join(reversed(chars))


@app.post("/internal/api/worker/hash/crack/task")
async def process_task(task: Task):

    total = estimate_combinations(len(task.alphabet), task.maxLength)

    start = total * task.partNumber // task.partCount
    end = total * (task.partNumber + 1) // task.partCount

    results = []
    checked = 0
    start_time = time.time()

    for i in range(start, end):
        word = index_to_word(i, task.alphabet, task.maxLength)
        checked += 1

        if hashlib.md5(word.encode()).hexdigest() == task.hash:
            results.append(word)

    exec_time = time.time() - start_time

    payload = {
        "requestId": task.requestId,
        "partNumber": task.partNumber,
        "results": results,
        "checked": checked,
        "executionTime": exec_time
    }

    async with httpx.AsyncClient() as client:
        for _ in range(RETRY_COUNT):
            try:
                await client.patch(MANAGER_URL, json=payload)
                break
            except Exception:
                await asyncio.sleep(1)

    return {"status": "OK"}