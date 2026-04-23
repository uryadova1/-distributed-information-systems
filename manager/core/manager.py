import os
import sys
import uuid
import time
from typing import Dict, Optional, List
from loguru import logger
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from models.schemas import CrackRequest, TaskStatus, CrackResponse, StatusResponse, MetricsResponse

from shared.word_generator import count_combinations
from core.task_distributor import TaskDistributor


class RequestStore:
    """In-memory хранилище запросов."""

    def __init__(self):
        self._requests: Dict[str, dict] = {}
        self._hash_index: Dict[str, str] = {}  # hash+params -> requestId

    def _make_key(self, request: CrackRequest) -> str:
        """Создание уникального ключа для идемпотентности."""
        return f"{request.hash}:{request.maxLength}:{request.algorithm}:{request.alphabet}"

    def find_existing(self, request: CrackRequest) -> Optional[str]:
        """Поиск существующего запроса с такими же параметрами."""
        return self._hash_index.get(self._make_key(request))

    def create(self, request: CrackRequest) -> tuple[str, int]:
        """Создание нового запроса."""
        request_id = str(uuid.uuid4())
        estimated = count_combinations(request.alphabet, request.maxLength)

        self._requests[request_id] = {
            'request': request,
            'status': TaskStatus.IN_PROGRESS,
            'created_at': time.time(),
            'results': [],
            'error': None,
            'estimated': estimated
        }
        self._hash_index[self._make_key(request)] = request_id
        return request_id, estimated

    def get(self, request_id: str) -> Optional[dict]:
        return self._requests.get(request_id)

    def update_status(self, request_id: str, status: TaskStatus, results: List[str] = None, error: str = None):
        if request_id in self._requests:
            self._requests[request_id]['status'] = status
            if results is not None:
                self._requests[request_id]['results'] = results
            if error is not None:
                self._requests[request_id]['error'] = error

    def cancel(self, request_id: str):
        self.update_status(request_id, TaskStatus.CANCELLED)

    def get_metrics(self) -> MetricsResponse:
        total = len(self._requests)
        active = sum(1 for r in self._requests.values() if r['status'] == TaskStatus.IN_PROGRESS)
        completed = sum(1 for r in self._requests.values() if r['status'] == TaskStatus.READY)

        # Расчёт средней скорости (слов/сек)
        speeds = []
        for r in self._requests.values():
            if r['status'] == TaskStatus.READY and 'execution_time' in r:
                est = r.get('estimated', 0)
                exec_time = r['execution_time']
                if exec_time > 0:
                    speeds.append(est / exec_time)

        avg_speed = sum(speeds) / len(speeds) if speeds else 0

        return MetricsResponse(
            totalTasks=total,
            activeTasks=active,
            completedTasks=completed,
            avgExecutionTime=avg_speed
        )


class Manager:
    """Основной класс менеджера."""

    def __init__(self, worker_urls: List[str], task_timeout: float = 5.0):
        self.store = RequestStore()
        self.distributor: Optional[TaskDistributor] = None
        self.worker_urls = worker_urls
        self.task_timeout = task_timeout
        self._execution_times: List[float] = []

    async def initialize(self):
        """Инициализация распределителя задач."""
        self.distributor = TaskDistributor(
            worker_urls=self.worker_urls,
            timeout=self.task_timeout
        )
        await self.distributor.__aenter__()

    async def shutdown(self):
        """Очистка ресурсов."""
        if self.distributor:
            await self.distributor.__aexit__(None, None, None)

    async def crack_hash(self, request: CrackRequest) -> CrackResponse:
        """Обработка запроса на взлом хэша."""
        # Проверка идемпотентности
        existing_id = self.store.find_existing(request)
        if existing_id:
            existing = self.store.get(existing_id)
            if existing['status'] == TaskStatus.READY:
                # Уже готово - возвращаем результат
                return CrackResponse(
                    requestId=existing_id,
                    estimatedCombinations=existing['estimated']
                )
            elif existing['status'] == TaskStatus.IN_PROGRESS:
                # В процессе - возвращаем ID
                return CrackResponse(
                    requestId=existing_id,
                    estimatedCombinations=existing['estimated']
                )

        # Создание нового запроса
        request_id, estimated = self.store.create(request)

        # Запуск распределения задач
        task_data = {
            'hash': request.hash,
            'maxLength': request.maxLength,
            'algorithm': request.algorithm.value,
            'alphabet': request.alphabet
        }

        await self.distributor.distribute_task(
            request_id, task_data,
            on_result_callback=self._on_part_complete,
            on_error_callback=self._on_part_error
        )

        return CrackResponse(requestId=request_id, estimatedCombinations=estimated)

    def get_status(self, request_id: str) -> StatusResponse:
        """Получение статуса запроса."""
        req = self.store.get(request_id)
        if not req:
            return StatusResponse(
                status=TaskStatus.ERROR,
                error=f"Request {request_id} not found"
            )

        if req['status'] == TaskStatus.READY:
            return StatusResponse(
                status=TaskStatus.READY,
                data=req['results'] if req['results'] else None
            )
        elif req['status'] == TaskStatus.ERROR:
            return StatusResponse(
                status=TaskStatus.ERROR,
                error=req['error']
            )
        elif req['status'] == TaskStatus.CANCELLED:
            return StatusResponse(status=TaskStatus.CANCELLED)
        else:
            return StatusResponse(status=TaskStatus.IN_PROGRESS)

    def cancel_request(self, request_id: str) -> bool:
        """Отмена запроса."""
        req = self.store.get(request_id)
        if not req:
            return False

        self.store.cancel(request_id)
        if self.distributor:
            self.distributor.cancel_request(request_id)
        return True

    def get_metrics(self) -> MetricsResponse:
        return self.store.get_metrics()

    async def _on_part_complete(self, request_id: str, part_number: int, results: List[str]):
        """Callback при успешном выполнении части."""
        req = self.store.get(request_id)
        if not req or req['status'] != TaskStatus.IN_PROGRESS:
            return

        req['results'].extend(results)
        # Проверка: если найдено совпадение, можно завершить досрочно
        if results:
            self.store.update_status(request_id, TaskStatus.READY, req['results'])

    async def _on_part_error(self, request_id: str, part_number: int, error: str):
        """Callback при ошибке выполнения части."""
        logger.error(f"Part {part_number} of {request_id} failed: {error}")
        # Попытка переназначения уже реализована в TaskDistributor