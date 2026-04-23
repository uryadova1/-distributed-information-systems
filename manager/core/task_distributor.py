import asyncio
import time
import httpx
from typing import Dict, List, Optional, Set
from loguru import logger

from models.schemas import WorkerTask, WorkerResult, TaskStatus


class TaskDistributor:
    """Распределитель задач между воркерами (Push-модель)."""

    def __init__(
            self,
            worker_urls: List[str],
            timeout: float = 5.0,
            max_retries: int = 3
    ):
        self.worker_urls = worker_urls
        self.timeout = timeout
        self.max_retries = max_retries
        self.available_workers: Set[str] = set(worker_urls)
        self.assigned_tasks: Dict[str, Dict] = {}  # requestId: {partNumber: worker_url}
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self):
        self._client = httpx.AsyncClient(timeout=self.timeout)
        return self

    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()

    def _calculate_parts(self, max_length: int, alphabet: str) -> int:
        """Определение количества частей для оптимального выполнения."""
        from core.hash_cracker import count_combinations
        total = count_combinations(alphabet, max_length)
        # Цель: каждая часть выполняется ~3-5 секунд
        # Приблизительно 100K комбинаций на часть
        part_count = max(1, total // 100_000)
        # Ограничиваем количеством воркеров * 2 для балансировки
        return min(part_count, len(self.worker_urls) * 2)

    async def distribute_task(
            self,
            request_id: str,
            task_data: dict,
            on_result_callback,
            on_error_callback
    ) -> bool:
        """Распределение задачи на части и назначение воркерам."""
        from core.hash_cracker import count_combinations

        alphabet = task_data['alphabet']
        max_length = task_data['maxLength']
        part_count = self._calculate_parts(max_length, alphabet)

        if not self.available_workers:
            logger.error("No available workers")
            return False

        self.assigned_tasks[request_id] = {
            'partCount': part_count,
            'completed': set(),
            'results': [],
            'start_time': time.time()
        }

        # Назначаем части воркерам
        for part_number in range(part_count):
            worker_url = self._select_worker()
            if not worker_url:
                logger.warning(f"No worker available for part {part_number}")
                continue

            worker_task = WorkerTask(
                requestId=request_id,
                hash=task_data['hash'],
                partNumber=part_number,
                partCount=part_count,
                algorithm=task_data['algorithm'],
                alphabet=alphabet,
                maxLength=max_length
            )

            # Асинхронный запуск без ожидания
            asyncio.create_task(
                self._send_to_worker(
                    worker_url, worker_task, request_id, part_number,
                    on_result_callback, on_error_callback
                )
            )

        return True

    def _select_worker(self) -> Optional[str]:
        """Выбор доступного воркера (round-robin)."""
        if not self.available_workers:
            return None
        return next(iter(self.available_workers))

    async def _send_to_worker(
            self,
            worker_url: str,
            task: WorkerTask,
            request_id: str,
            part_number: int,
            on_result_callback,
            on_error_callback,
            retry_count: int = 0
    ):
        """Отправка задачи воркеру с обработкой повторов."""
        try:
            response = await self._client.post(
                f"{worker_url}/internal/api/worker/hash/crack/task",
                json=task.model_dump()
            )
            response.raise_for_status()
            logger.debug(f"Task {request_id}:{part_number} sent to {worker_url}")

        except (httpx.TimeoutException, httpx.ConnectError, httpx.HTTPStatusError) as e:
            logger.warning(f"Failed to send task {request_id}:{part_number}: {e}")

            if retry_count < self.max_retries:
                # Повторная отправка тому же или другому воркеру
                await asyncio.sleep(0.5 * (retry_count + 1))
                new_worker = self._select_worker()
                if new_worker:
                    await self._send_to_worker(
                        new_worker, task, request_id, part_number,
                        on_result_callback, on_error_callback,
                        retry_count + 1
                    )
            else:
                await on_error_callback(request_id, part_number, str(e))

    async def handle_worker_result(self, result: WorkerResult):
        """Обработка результата от воркера."""
        task_info = self.assigned_tasks.get(result.requestId)
        if not task_info:
            logger.warning(f"Unknown request: {result.requestId}")
            return

        task_info['completed'].add(result.partNumber)
        task_info['results'].extend(result.found)

        # Проверка завершения всех частей
        if len(task_info['completed']) >= task_info['partCount']:
            execution_time = time.time() - task_info['start_time']
            total_checked = sum(r.checked for r in [result] if r.requestId == result.requestId)
            # Здесь должна быть агрегация всех результатов
            await self._finalize_request(result.requestId, task_info['results'], execution_time)

    async def _finalize_request(self, request_id: str, results: List[str], execution_time: float):
        """Завершение обработки запроса."""
        logger.info(f"Request {request_id} completed in {execution_time:.2f}s")
        # Уведомление менеджера о завершении
        # (реализуется через callback или shared state)

    def cancel_request(self, request_id: str):
        """Отмена запроса и уведомление воркеров."""
        if request_id in self.assigned_tasks:
            del self.assigned_tasks[request_id]
        logger.info(f"Request {request_id} cancelled")

    def register_worker(self, worker_url: str):
        """Регистрация воркера в пуле."""
        self.available_workers.add(worker_url)
        self.worker_urls.append(worker_url)

    def unregister_worker(self, worker_url: str):
        """Удаление воркера из пула."""
        self.available_workers.discard(worker_url)