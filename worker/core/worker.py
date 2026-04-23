import asyncio
import sys

import httpx
from loguru import logger
sys.path.insert(0, '/app')
# from schemas import WorkerTask, WorkerResult
from models.schemas import WorkerTask, WorkerResult
from core.hash_cracker import crack_part


class Worker:
    """Воркер для выполнения задач перебора."""

    def __init__(self, manager_url: str, max_retries: int = 3):
        self.manager_url = manager_url.rstrip('/')
        self.max_retries = max_retries
        self._client: httpx.AsyncClient = None
        self._cancel_events: dict[str, asyncio.Event] = {}

    async def __aenter__(self):
        self._client = httpx.AsyncClient(timeout=30.0)
        return self

    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()

    async def execute_task(self, task: WorkerTask) -> bool:
        """Выполнение полученной задачи."""
        request_id = task.requestId
        part_number = task.partNumber

        logger.info(f"Starting task {request_id}:{part_number}")

        # Создание флага отмены для этой задачи
        cancel_event = asyncio.Event()
        self._cancel_events[request_id] = cancel_event

        try:
            found, checked, exec_time = await crack_part(
                hash_target=task.hash,
                algorithm=task.algorithm.value,
                alphabet=task.alphabet,
                max_length=task.maxLength,
                part_number=task.partNumber,
                part_count=task.partCount,
                cancel_flag=cancel_event
            )

            result = WorkerResult(
                requestId=request_id,
                partNumber=part_number,
                found=found,
                checked=checked,
                executionTime=exec_time
            )

            # Отправка результата менеджеру с повторами
            await self._send_result_with_retry(result)
            logger.info(f"Task {request_id}:{part_number} completed")
            return True

        except Exception as e:
            logger.error(f"Task {request_id}:{part_number} failed: {e}")
            return False
        finally:
            self._cancel_events.pop(request_id, None)

    async def _send_result_with_retry(self, result: WorkerResult):
        """Отправка результата с обработкой повторов."""
        last_error = None

        for attempt in range(self.max_retries):
            try:
                response = await self._client.patch(
                    f"{self.manager_url}/internal/api/manager/hash/crack/request",
                    json=result.model_dump()
                )
                response.raise_for_status()
                return
            except (httpx.TimeoutException, httpx.ConnectError) as e:
                last_error = e
                logger.warning(f"Retry {attempt + 1}/{self.max_retries} failed: {e}")
                await asyncio.sleep(1 * (attempt + 1))

        logger.error(f"Failed to send result after {self.max_retries} retries: {last_error}")

    def cancel_task(self, request_id: str):
        """Сигнал отмены для задачи."""
        if request_id in self._cancel_events:
            self._cancel_events[request_id].set()
            logger.info(f"Task {request_id} cancellation requested")