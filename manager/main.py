import uuid
import asyncio
import time
from enum import Enum
from typing import Dict, List, Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx
import math

WORKER_URL = "http://worker:8000/internal/api/worker/hash/crack/task"
TIMEOUT = 10
TARGET_WORDS_PER_PART = 50000

app = FastAPI()

# ================================
# MODELS
# ================================

class Status(str, Enum):
    IN_PROGRESS = "IN_PROGRESS"
    READY = "READY"
    ERROR = "ERROR"
    CANCELLED = "CANCELLED"


class CrackRequest(BaseModel):
    hash: str
    maxLength: int
    algorithm: str = "MD5"
    alphabet: Optional[str] = "abcdefghijklmnopqrstuvwxyz0123456789"


class WorkerResult(BaseModel):
    requestId: str
    partNumber: int
    results: List[str]
    checked: int
    executionTime: float


# ================================
# IN-MEMORY STORAGE
# ================================

requests_store: Dict[str, dict] = {}
request_index: Dict[str, str] = {}

total_checked_words = 0
total_execution_time = 0
completed_parts = 0


# ================================
# UTILITIES
# ================================

def estimate_combinations(alphabet_len: int, max_length: int):
    return sum(alphabet_len ** i for i in range(1, max_length + 1))


def make_request_key(req: CrackRequest):
    return f"{req.hash}_{req.maxLength}_{req.algorithm}_{req.alphabet}"


def calculate_part_count(total_combinations):
    return max(1, math.ceil(total_combinations / TARGET_WORDS_PER_PART))


# ================================
# API
# ================================

@app.post("/api/hash/crack")
async def crack_hash(req: CrackRequest):

    key = make_request_key(req)

    # Идемпотентность
    if key in request_index:
        request_id = request_index[key]
        existing = requests_store[request_id]
        if existing["status"] == Status.READY:
            return {
                "requestId": request_id,
                "estimatedCombinations": existing["total"],
                "data": existing["results"]
            }
        return {
            "requestId": request_id,
            "estimatedCombinations": existing["total"]
        }

    request_id = str(uuid.uuid4())
    total = estimate_combinations(len(req.alphabet), req.maxLength)
    part_count = calculate_part_count(total)

    requests_store[request_id] = {
        "params": req,
        "status": Status.IN_PROGRESS,
        "results": [],
        "error": None,
        "created_at": time.time(),
        "total": total,
        "partCount": part_count,
        "partsDone": 0,
        "partsInProgress": {},
        "cancelled": False
    }

    request_index[key] = request_id

    asyncio.create_task(distribute_tasks(request_id))

    return {
        "requestId": request_id,
        "estimatedCombinations": total
    }


@app.get("/api/hash/status")
def get_status(requestId: str):
    if requestId not in requests_store:
        raise HTTPException(404)

    req = requests_store[requestId]

    response = {
        "status": req["status"],
        "data": req["results"] if req["status"] == Status.READY else None
    }

    if req["status"] == Status.ERROR:
        response["error"] = req["error"]

    return response


@app.delete("/api/hash/crack")
def cancel(requestId: str):
    if requestId not in requests_store:
        raise HTTPException(404)

    requests_store[requestId]["status"] = Status.CANCELLED
    requests_store[requestId]["cancelled"] = True

    return {"status": "CANCELLED"}


@app.get("/api/metrics")
def metrics():
    avg_speed = 0
    if total_execution_time > 0:
        avg_speed = total_checked_words / total_execution_time

    return {
        "totalTasks": len(requests_store),
        "activeTasks": sum(1 for r in requests_store.values() if r["status"] == Status.IN_PROGRESS),
        "completedTasks": sum(1 for r in requests_store.values() if r["status"] == Status.READY),
        "avgExecutionTime": int(avg_speed)
    }


# ================================
# TASK DISTRIBUTION
# ================================

async def distribute_tasks(request_id: str):

    req_data = requests_store[request_id]
    part_count = req_data["partCount"]

    async with httpx.AsyncClient() as client:
        for part in range(part_count):
            if req_data["cancelled"]:
                return

            req_data["partsInProgress"][part] = time.time()

            payload = {
                "requestId": request_id,
                "hash": req_data["params"].hash,
                "partNumber": part,
                "partCount": part_count,
                "algorithm": req_data["params"].algorithm,
                "alphabet": req_data["params"].alphabet,
                "maxLength": req_data["params"].maxLength
            }

            asyncio.create_task(send_task(client, payload))


async def send_task(client, payload):
    try:
        await client.post(WORKER_URL, json=payload, timeout=TIMEOUT)
    except Exception:
        pass


@app.patch("/internal/api/manager/hash/crack/request")
async def receive_result(result: WorkerResult):
    global total_checked_words, total_execution_time, completed_parts

    if result.requestId not in requests_store:
        return

    req = requests_store[result.requestId]

    if req["status"] != Status.IN_PROGRESS:
        return

    req["partsDone"] += 1
    req["results"].extend(result.results)

    total_checked_words += result.checked
    total_execution_time += result.executionTime
    completed_parts += 1

    if req["partsDone"] >= req["partCount"]:
        req["status"] = Status.READY


from fastapi.responses import HTMLResponse

@app.get("/", response_class=HTMLResponse)
def web():
    return """
    <html>
    <head>
        <title>CrackHash</title>
    </head>
    <body>
        <h2>CrackHash Web UI</h2>

        <label>Hash:</label><br>
        <input id="hash" size="40"><br><br>

        <label>Max Length:</label><br>
        <input id="maxLength" value="4"><br><br>

        <label>Alphabet:</label><br>
        <input id="alphabet" value="abcdefghijklmnopqrstuvwxyz"><br><br>

        <button onclick="start()">Start</button>

        <h3>Status:</h3>
        <pre id="status"></pre>

        <script>
        let requestId = null;

        async function start() {
            const hash = document.getElementById("hash").value;
            const maxLength = parseInt(document.getElementById("maxLength").value);
            const alphabet = document.getElementById("alphabet").value;

            const response = await fetch('/api/hash/crack', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({hash, maxLength, algorithm:"MD5", alphabet})
            });

            const data = await response.json();
            requestId = data.requestId;

            checkStatus();
        }

        async function checkStatus() {
            if (!requestId) return;

            const response = await fetch('/api/hash/status?requestId=' + requestId);
            const data = await response.json();

            document.getElementById("status").innerText =
                JSON.stringify(data, null, 2);

            if (data.status === "IN_PROGRESS") {
                setTimeout(checkStatus, 1000);
            }
        }
        </script>
    </body>
    </html>
    """