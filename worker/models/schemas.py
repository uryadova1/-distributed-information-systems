# Можно импортировать из manager или дублировать
# Для автономности дублируем минимально необходимые модели

from enum import Enum
from typing import List, Optional
from pydantic import BaseModel, Field


class HashAlgorithm(str, Enum):
    MD5 = "MD5"


class WorkerTask(BaseModel):
    requestId: str
    hash: str
    partNumber: int
    partCount: int
    algorithm: HashAlgorithm
    alphabet: str
    maxLength: int


class WorkerResult(BaseModel):
    requestId: str
    partNumber: int
    found: List[str]
    checked: int
    executionTime: float