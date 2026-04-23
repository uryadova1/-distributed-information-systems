import os
import sys
from contextlib import asynccontextmanager
from fastapi import FastAPI, BackgroundTasks
from loguru import logger

# ✅ Добавляем shared в PYTHON_PATH
sys.path.insert(0, '/app/shared')

from shared.models import WorkerTask
from core.worker import Worker

worker: Worker = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle для воркера."""
    global worker

    manager_url = os.getenv('MANAGER_URL', 'http://manager:8000')
    worker = Worker(manager_url=manager_url)
    await worker.__aenter__()

    logger.info("Worker started")
    yield

    await worker.__aexit__(None, None, None)
    logger.info("Worker stopped")


app = FastAPI(
    title="CrackHash Worker",
    description="Distributed hash cracking system - Worker node",
    version="1.0.0",
    lifespan=lifespan
)


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


@app.post("/internal/api/worker/hash/crack/task")
async def receive_task(task: WorkerTask, background_tasks: BackgroundTasks):
    """Получение задачи от менеджера."""
    # Запускаем выполнение в фоне, не блокируя ответ
    background_tasks.add_task(worker.execute_task, task)
    return {"status": "accepted", "partNumber": task.partNumber}


@app.delete("/internal/api/worker/hash/crack/task/{request_id}")
async def cancel_task(request_id: str):
    """Отмена задачи (опционально)."""
    worker.cancel_task(request_id)
    return {"status": "cancellation_requested"}


@app.on_event("startup")
def setup_logging():
    logger.add("logs/worker.log", rotation="10 MB", level="INFO")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000)