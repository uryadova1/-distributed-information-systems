"""Общие Pydantic-модели для API."""
from enum import Enum
from typing import Optional, List
from pydantic import BaseModel, Field, field_validator


class HashAlgorithm(str, Enum):
    MD5 = "MD5"


class TaskStatus(str, Enum):
    IN_PROGRESS = "IN_PROGRESS"
    READY = "READY"
    ERROR = "ERROR"
    CANCELLED = "CANCELLED"


class CrackRequest(BaseModel):
    hash: str = Field(..., min_length=32, max_length=128)
    maxLength: int = Field(..., ge=1, le=10)
    algorithm: HashAlgorithm = HashAlgorithm.MD5
    alphabet: str = Field(default="abcdefghijklmnopqrstuvwxyz0123456789", min_length=1)

    @field_validator('hash')
    @classmethod
    def validate_hash(cls, v: str) -> str:
        try:
            bytes.fromhex(v)
        except ValueError:
            raise ValueError('Hash must be a valid hexadecimal string')
        return v.lower()


class CrackResponse(BaseModel):
    requestId: str
    estimatedCombinations: int


class StatusResponse(BaseModel):
    status: TaskStatus
    data: Optional[List[str]] = None
    error: Optional[str] = None


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


class MetricsResponse(BaseModel):
    totalTasks: int
    activeTasks: int
    completedTasks: int
    avgExecutionTime: float