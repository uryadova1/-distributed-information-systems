import os
import sys
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from shared.models import CrackRequest, CrackResponse, StatusResponse, WorkerResult, MetricsResponse, TaskStatus
from core.manager import Manager

# Глобальный экземпляр менеджера
manager: Manager = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle manager для инициализации/очистки."""
    global manager

    # Получение списка воркеров из переменных окружения
    worker_urls = os.getenv('WORKER_URLS', 'http://worker:8000').split(',')
    task_timeout = float(os.getenv('TASK_TIMEOUT', '5'))

    manager = Manager(worker_urls=worker_urls, task_timeout=task_timeout)
    await manager.initialize()

    logger.info("Manager started")
    yield

    await manager.shutdown()
    logger.info("Manager stopped")


app = FastAPI(
    title="CrackHash Manager",
    description="Distributed hash cracking system - Manager node",
    version="1.0.0",
    lifespan=lifespan
)


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}


@app.post("/api/hash/crack", response_model=CrackResponse)
async def crack_hash(request: CrackRequest):
    """Запрос на взлом хэша."""
    try:
        return await manager.crack_hash(request)
    except Exception as e:
        logger.error(f"Error processing crack request: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/hash/status", response_model=StatusResponse)
async def get_status(requestId: str):
    """Получение статуса запроса."""
    return manager.get_status(requestId)


@app.delete("/api/hash/crack")
async def cancel_crack(requestId: str):
    """Отмена запроса на взлом."""
    if manager.cancel_request(requestId):
        return {"message": "Request cancelled"}
    raise HTTPException(status_code=404, detail="Request not found")


@app.get("/api/metrics", response_model=MetricsResponse)
async def get_metrics():
    """Получение метрик системы."""
    return manager.get_metrics()


# Internal API для воркеров
@app.patch("/internal/api/manager/hash/crack/request")
async def worker_result(result: WorkerResult):
    """Приём результатов от воркера."""
    # Обработка результата (упрощённо)
    if result.found:
        # Если найдено совпадение - завершаем запрос
        manager.store.update_status(
            result.requestId,
            TaskStatus.READY,
            results=result.found
        )
    return {"received": True}


@app.on_event("startup")
def setup_logging():
    logger.add("logs/manager.log", rotation="10 MB", level="INFO")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
