import uuid
import asyncio
import time
import math
import os
from enum import Enum
from typing import Dict, List, Optional
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import httpx


_raw_urls = os.getenv("WORKER_URLS", "http://worker:8000")
WORKER_URLS: List[str] = [u.strip() for u in _raw_urls.split(",") if u.strip()]

TASK_ENDPOINT = "/internal/api/worker/hash/crack/task"
TASK_TIMEOUT = 30          # секунд — таймаут на отправку задачи воркеру
PART_TIMEOUT = 60          # секунд — если воркер не ответил, перезапускаем часть
TARGET_WORDS_PER_PART = 50_000   # ~5 сек на воркер

app = FastAPI()


class Status(str, Enum):
    IN_PROGRESS = "IN_PROGRESS"
    READY       = "READY"
    ERROR       = "ERROR"
    CANCELLED   = "CANCELLED"


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



# requestId → state dict
requests_store: Dict[str, dict] = {}

# hash+maxLen+algo+alphabet → requestId  (идемпотентность)
request_index: Dict[str, str] = {}


_stats_lock = asyncio.Lock()
_total_checked_words: int = 0
_total_execution_time: float = 0.0
_completed_parts: int = 0



def _estimate_combinations(alphabet_len: int, max_length: int) -> int:
    return sum(alphabet_len ** i for i in range(1, max_length + 1))


def _make_key(req: CrackRequest) -> str:
    return f"{req.hash}|{req.maxLength}|{req.algorithm}|{req.alphabet}"


def _part_count(total: int) -> int:
    return max(1, math.ceil(total / TARGET_WORDS_PER_PART))


def _worker_url_for(part_number: int) -> str:
    """Round-robin по списку воркеров."""
    return WORKER_URLS[part_number % len(WORKER_URLS)] + TASK_ENDPOINT


def _build_payload(request_id: str, req_data: dict, part: int) -> dict:
    p = req_data["params"]
    return {
        "requestId":  request_id,
        "hash":       p.hash,
        "partNumber": part,
        "partCount":  req_data["partCount"],
        "algorithm":  p.algorithm,
        "alphabet":   p.alphabet,
        "maxLength":  p.maxLength,
    }



@app.post("/api/hash/crack")
async def crack_hash(req: CrackRequest):
    key = _make_key(req)

    # идемпотентность
    if key in request_index:
        rid = request_index[key]
        existing = requests_store[rid]
        if existing["status"] == Status.READY:
            return {
                "requestId": rid,
                "estimatedCombinations": existing["total"],
                "data": existing["results"],
            }
        # IN_PROGRESS или другой статус — возвращаем тот же id
        return {"requestId": rid, "estimatedCombinations": existing["total"]}

    # создаём новый запрос
    request_id = str(uuid.uuid4())
    total      = _estimate_combinations(len(req.alphabet), req.maxLength)
    part_count = _part_count(total)

    requests_store[request_id] = {
        "params":           req,
        "status":           Status.IN_PROGRESS,
        "results":          [],
        "error":            None,
        "created_at":       time.time(),
        "total":            total,
        "partCount":        part_count,
        "partsDone":        0,
        "partsInProgress":  {},   # part → timestamp последней отправки
        "partsFinished":    set(),
        "cancelled":        False,
        "lock":             asyncio.Lock(),
    }
    request_index[key] = request_id

    asyncio.create_task(distribute_tasks(request_id))

    return {"requestId": request_id, "estimatedCombinations": total}


@app.get("/api/hash/status")
def get_status(requestId: str):
    if requestId not in requests_store:
        raise HTTPException(status_code=404, detail="Request not found")

    req = requests_store[requestId]
    response: dict = {"status": req["status"], "data": None}

    if req["status"] == Status.READY:
        response["data"] = req["results"]
    elif req["status"] == Status.ERROR:
        response["error"] = req["error"]

    return response


@app.delete("/api/hash/crack")
async def cancel_request(requestId: str):
    if requestId not in requests_store:
        raise HTTPException(status_code=404, detail="Request not found")

    req = requests_store[requestId]
    if req["status"] not in (Status.IN_PROGRESS,):
        return {"status": req["status"]}  # уже завершён

    req["status"]    = Status.CANCELLED
    req["cancelled"] = True

    # уведомляем всех воркеров о прекращении
    asyncio.create_task(_notify_workers_cancel(requestId, req))

    return {"status": "CANCELLED"}


@app.get("/api/metrics")
def metrics():
    avg_speed = 0
    if _total_execution_time > 0:
        avg_speed = int(_total_checked_words / _total_execution_time)

    return {
        "totalTasks":       len(requests_store),
        "activeTasks":      sum(1 for r in requests_store.values() if r["status"] == Status.IN_PROGRESS),
        "completedTasks":   sum(1 for r in requests_store.values() if r["status"] == Status.READY),
        "avgExecutionTime": avg_speed,  # слов/сек
    }



@app.patch("/internal/api/manager/hash/crack/request")
async def receive_result(result: WorkerResult):
    global _total_checked_words, _total_execution_time, _completed_parts

    req = requests_store.get(result.requestId)
    if req is None:
        return {"status": "unknown request"}

    async with req["lock"]:
        # игнорируем дубликаты (воркер мог прислать дважды)
        if result.partNumber in req["partsFinished"]:
            return {"status": "duplicate"}

        if req["status"] != Status.IN_PROGRESS:
            return {"status": "ignored"}

        req["partsFinished"].add(result.partNumber)
        req["partsInProgress"].pop(result.partNumber, None)
        req["partsDone"] += 1
        req["results"].extend(result.results)

    # обновляем глобальную статистику
    async with _stats_lock:
        _total_checked_words  += result.checked
        _total_execution_time += result.executionTime
        _completed_parts      += 1

    async with req["lock"]:
        if req["partsDone"] >= req["partCount"]:
            req["status"] = Status.READY

    return {"status": "OK"}



async def distribute_tasks(request_id: str):
    """Рассылает все части задачи воркерам и запускает watchdog."""
    req = requests_store[request_id]
    part_count = req["partCount"]

    async with httpx.AsyncClient(timeout=TASK_TIMEOUT) as client:
        send_tasks = []
        for part in range(part_count):
            if req["cancelled"]:
                return
            payload = _build_payload(request_id, req, part)
            req["partsInProgress"][part] = time.time()
            send_tasks.append(_send_task(client, payload, request_id))

        # отправляем все части параллельно
        await asyncio.gather(*send_tasks, return_exceptions=True)

    # запускаем watchdog — следит за зависшими частями
    asyncio.create_task(_watchdog(request_id))


async def _send_task(client: httpx.AsyncClient, payload: dict, request_id: str,
                     retries: int = 3) -> None:
    """Отправляет одну часть воркеру. При ошибке — повторяет retries раз."""
    part = payload["partNumber"]
    url  = _worker_url_for(part)

    for attempt in range(retries):
        req = requests_store.get(request_id)
        if req is None or req["cancelled"]:
            return
        try:
            r = await client.post(url, json=payload, timeout=TASK_TIMEOUT)
            r.raise_for_status()
            return  # успех
        except Exception:
            if attempt < retries - 1:
                await asyncio.sleep(1)

    # все попытки провалились — помечаем ошибку
    req = requests_store.get(request_id)
    if req and req["status"] == Status.IN_PROGRESS:
        req["partsInProgress"].pop(part, None)
        # watchdog подхватит и переназначит


async def _watchdog(request_id: str):
    """Переназначает части, которые не получили ответа за PART_TIMEOUT секунд."""
    while True:
        await asyncio.sleep(10)

        req = requests_store.get(request_id)
        if req is None:
            return
        if req["status"] != Status.IN_PROGRESS:
            return

        now = time.time()
        timed_out = [
            part for part, ts in list(req["partsInProgress"].items())
            if now - ts > PART_TIMEOUT and part not in req["partsFinished"]
        ]

        if not timed_out:
            # проверяем, не завис ли запрос в целом
            all_done = req["partsDone"] >= req["partCount"]
            if all_done:
                req["status"] = Status.READY
                return
            continue

        # переотправляем зависшие части
        async with httpx.AsyncClient(timeout=TASK_TIMEOUT) as client:
            retry_tasks = []
            for part in timed_out:
                if part in req["partsFinished"]:
                    continue
                payload = _build_payload(request_id, req, part)
                req["partsInProgress"][part] = time.time()
                retry_tasks.append(_send_task(client, payload, request_id))
            await asyncio.gather(*retry_tasks, return_exceptions=True)


async def _notify_workers_cancel(request_id: str, req: dict):
    """Посылает сигнал отмены всем воркерам, обрабатывающим этот запрос."""
    async with httpx.AsyncClient(timeout=5) as client:
        tasks = []
        for url_base in WORKER_URLS:
            cancel_url = url_base + "/internal/api/worker/hash/crack/cancel"
            tasks.append(
                client.post(cancel_url, json={"requestId": request_id})
            )
        await asyncio.gather(*tasks, return_exceptions=True)



@app.get("/", response_class=HTMLResponse)
def web_ui():
    return """
<<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<title>CrackHash</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: monospace; background: #0d0f14; color: #d4dae8; padding: 40px; }
 
  h2   { color: #e8ff47; margin-bottom: 24px; font-size: 22px; letter-spacing: 1px; }
  h3   { color: #a0a8c0; font-size: 13px; letter-spacing: 2px; text-transform: uppercase;
         margin: 32px 0 14px; border-bottom: 1px solid #1e2330; padding-bottom: 8px; }
 
  label { color: #5a6278; font-size: 11px; display: block; margin-bottom: 4px; margin-top: 14px;
          letter-spacing: 1px; text-transform: uppercase; }
 
  input { background: #1a1d26; border: 1px solid #2e3347; color: #d4dae8;
          padding: 8px 12px; border-radius: 6px; width: 380px; font-family: monospace; font-size: 13px; }
  input:focus { outline: none; border-color: #e8ff47; }
 
  .btn-row { display: flex; gap: 10px; margin-top: 20px; }
 
  button {
    background: #e8ff47; color: #000; border: none; padding: 9px 22px;
    border-radius: 6px; cursor: pointer; font-weight: bold; font-family: monospace;
    font-size: 13px; transition: opacity 0.15s;
  }
  button:hover { opacity: 0.85; }
  button.secondary { background: #1e2330; color: #d4dae8; border: 1px solid #2e3347; }
  button.secondary:hover { border-color: #e8ff47; color: #e8ff47; }
  button.danger { background: #ff4b4b; color: #fff; }
 
  pre {
    background: #13161e; padding: 18px; border-radius: 8px; color: #47c8ff;
    margin-top: 14px; font-size: 13px; line-height: 1.6; min-height: 60px;
    border: 1px solid #1e2330; white-space: pre-wrap; word-break: break-all;
  }
 
  /* ── Metrics cards ── */
  .metrics-grid {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 12px;
    margin-top: 14px;
  }
  .metric-card {
    background: #13161e;
    border: 1px solid #1e2330;
    border-radius: 10px;
    padding: 16px 18px;
    transition: border-color 0.2s;
  }
  .metric-card:hover { border-color: #2e3347; }
  .metric-card .val {
    font-size: 28px; font-weight: bold; color: #e8ff47; line-height: 1.1;
  }
  .metric-card .lbl {
    font-size: 11px; color: #5a6278; margin-top: 6px; letter-spacing: 1px;
    text-transform: uppercase;
  }
  .metric-card .unit { font-size: 11px; color: #3a4055; margin-top: 2px; }
 
  .status-badge {
    display: inline-block; padding: 2px 10px; border-radius: 99px;
    font-size: 11px; font-weight: bold; letter-spacing: 1px; margin-left: 10px;
    vertical-align: middle;
  }
  .badge-progress { background: rgba(255,173,51,0.15); color: #ffad33; border: 1px solid rgba(255,173,51,0.3); }
  .badge-ready    { background: rgba(68,216,127,0.15); color: #44d87f; border: 1px solid rgba(68,216,127,0.3); }
  .badge-error    { background: rgba(255,75,75,0.15);  color: #ff4b4b; border: 1px solid rgba(255,75,75,0.3); }
  .badge-cancel   { background: rgba(90,98,120,0.2);   color: #8a92a8; border: 1px solid rgba(90,98,120,0.3); }
</style>
</head>
<body>
 
<h2>⚡ CrackHash</h2>
 
<!-- ── Crack form ── -->
<h3>Новый запрос</h3>
 
<label>Hash (MD5):</label>
<input id="hash" placeholder="e2fc714c4727ee9395f324cd2e7f331f">
 
<label>Max Length:</label>
<input id="maxLength" value="4">
 
<label>Alphabet:</label>
<input id="alphabet" value="abcdefghijklmnopqrstuvwxyz0123456789">
 
<div class="btn-row">
  <button onclick="start()">▶ Start</button>
  <button class="danger" onclick="cancelReq()" id="cancelBtn" style="display:none">✕ Cancel</button>
</div>
 
<div id="statusLabel" style="margin-top:18px; font-size:13px; color:#5a6278;"></div>
<pre id="status" style="display:none"></pre>
 
<!-- ── Metrics ── -->
<h3>
  Метрики
  <button class="secondary" onclick="loadMetrics()"
    style="padding:4px 14px; font-size:11px; margin-left:12px; vertical-align:middle;">
    ↻ Обновить
  </button>
</h3>
 
<div class="metrics-grid">
  <div class="metric-card">
    <div class="val" id="m-total">—</div>
    <div class="lbl">Всего задач</div>
    <div class="unit">totalTasks</div>
  </div>
  <div class="metric-card">
    <div class="val" id="m-active" style="color:#ffad33">—</div>
    <div class="lbl">Активных</div>
    <div class="unit">activeTasks</div>
  </div>
  <div class="metric-card">
    <div class="val" id="m-done" style="color:#44d87f">—</div>
    <div class="lbl">Завершённых</div>
    <div class="unit">completedTasks</div>
  </div>
  <div class="metric-card">
    <div class="val" id="m-speed" style="color:#47c8ff">—</div>
    <div class="lbl">Скорость</div>
    <div class="unit">слов / сек</div>
  </div>
</div>
 
<script>
let requestId = null;
let polling   = null;
 
// ── Crack ────────────────────────────────────
 
async function start() {
  const hash      = document.getElementById("hash").value.trim();
  const maxLength = parseInt(document.getElementById("maxLength").value);
  const alphabet  = document.getElementById("alphabet").value;
 
  if (!hash) { setStatusLabel("Введите хэш!"); return; }
 
  clearInterval(polling);
  setStatusLabel("Отправляю запрос…");
  document.getElementById("status").style.display = "none";
 
  const res  = await fetch('/api/hash/crack', {
    method:  'POST',
    headers: {'Content-Type': 'application/json'},
    body:    JSON.stringify({hash, maxLength, algorithm: "MD5", alphabet})
  });
  const data = await res.json();
  requestId  = data.requestId;
 
  document.getElementById("cancelBtn").style.display = "inline-block";
  setStatusLabel("requestId: " + requestId);
  polling = setInterval(checkStatus, 1500);
}
 
async function checkStatus() {
  if (!requestId) return;
  const res  = await fetch('/api/hash/status?requestId=' + requestId);
  const data = await res.json();
 
  const statusColors = {
    IN_PROGRESS: "badge-progress",
    READY:       "badge-ready",
    ERROR:       "badge-error",
    CANCELLED:   "badge-cancel",
  };
  const badge = `<span class="status-badge ${statusColors[data.status] || ''}">${data.status}</span>`;
  setStatusLabel("requestId: " + requestId + badge);
 
  const pre = document.getElementById("status");
  pre.style.display = "block";
  pre.innerText = JSON.stringify(data, null, 2);
 
  if (data.status !== "IN_PROGRESS") {
    clearInterval(polling);
    document.getElementById("cancelBtn").style.display = "none";
    loadMetrics();  // обновляем метрики после завершения
  }
}
 
async function cancelReq() {
  if (!requestId) return;
  await fetch('/api/hash/crack?requestId=' + requestId, {method: 'DELETE'});
  clearInterval(polling);
  document.getElementById("cancelBtn").style.display = "none";
  setStatusLabel("Запрос отменён.");
  loadMetrics();
}
 
function setStatusLabel(html) {
  document.getElementById("statusLabel").innerHTML = html;
}
 
// ── Metrics ──────────────────────────────────
 
async function loadMetrics() {
  try {
    const res  = await fetch('/api/metrics');
    const data = await res.json();
 
    document.getElementById("m-total").innerText  = data.totalTasks      ?? "—";
    document.getElementById("m-active").innerText = data.activeTasks     ?? "—";
    document.getElementById("m-done").innerText   = data.completedTasks  ?? "—";
 
    const speed = data.avgExecutionTime;
    document.getElementById("m-speed").innerText  =
      speed > 0 ? speed.toLocaleString("ru-RU") : "—";
  } catch(e) {
    console.error("Metrics error:", e);
  }
}
 
// загружаем метрики при открытии страницы
loadMetrics();
</script>
</body>
</html>
"""