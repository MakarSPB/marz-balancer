import os
import time
import asyncio
import re
from typing import Dict, Any, Optional, List
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
import logging
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler

import aiohttp
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

from config import (
    MARZBAN_URL, MARZBAN_ADMIN_USER, MARZBAN_ADMIN_PASS,
    POLL_INTERVAL, APP_PORT, IP_AGENT_PORT, IP_AGENT_SCHEME,
    MONITOR_PORT, TELEGRAM_ENABLED, NODE_CANDIDATE_PATHS,
    stats, _token_cache,
    NODE_RECONNECT_THRESHOLD, NODE_RECONNECT_COOLDOWN
)
from telegram_notify import process_notifications

# ===================== ЛОГИРОВАНИЕ =====================
LOG_DIR = os.getenv("LOG_DIR", "./logs")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_MAX_BYTES = int(os.getenv("LOG_MAX_BYTES", "1048576"))
LOG_BACKUP_COUNT = int(os.getenv("LOG_BACKUP_COUNT", "5"))
LOG_ROTATE_WHEN = os.getenv("LOG_ROTATE_WHEN", "").strip()
LOG_ROTATE_INTERVAL = int(os.getenv("LOG_ROTATE_INTERVAL", "1"))
LOG_STDOUT = os.getenv("LOG_STDOUT", "1").lower() in ("1", "true", "yes")

os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "marz_balancer.log")

logger = logging.getLogger("marz_balancer")
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

if not logger.handlers:
    if LOG_ROTATE_WHEN:
        handler = TimedRotatingFileHandler(
            LOG_FILE, when=LOG_ROTATE_WHEN, interval=LOG_ROTATE_INTERVAL,
            backupCount=LOG_BACKUP_COUNT, encoding="utf-8"
        )
    else:
        handler = RotatingFileHandler(
            LOG_FILE, maxBytes=LOG_MAX_BYTES,
            backupCount=LOG_BACKUP_COUNT, encoding="utf-8"
        )
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)8s | %(name)s | %(message)s",
        "%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(fmt)
    logger.addHandler(handler)
    if LOG_STDOUT:
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        logger.addHandler(sh)

logger.info("Инициализация приложения (rotate=%s, file=%s)",
            f"time:{LOG_ROTATE_WHEN}" if LOG_ROTATE_WHEN else f"size:{LOG_MAX_BYTES}", LOG_FILE)
# =======================================================

first_run = True


async def _fetch_token(session: aiohttp.ClientSession) -> Optional[str]:
    if not MARZBAN_URL or not MARZBAN_ADMIN_USER or not MARZBAN_ADMIN_PASS:
        return None
    now = time.time()
    if _token_cache["token"] and now - _token_cache["fetched_at"] < _token_cache["ttl"]:
        return _token_cache["token"]
    url = f"{MARZBAN_URL}/api/admin/token"
    data = {"username": MARZBAN_ADMIN_USER, "password": MARZBAN_ADMIN_PASS}
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    try:
        async with session.post(url, data=data, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                logger.warning("Не удалось получить токен (status=%s)", resp.status)
                return None
            j = await resp.json()
            token = j.get("access_token") or j.get("token")
            if token:
                _token_cache["token"] = token
                _token_cache["fetched_at"] = now
                return token
    except Exception as ex:
        logger.exception("Ошибка получения токена: %s", ex)
    return None


async def _fetch_generic(session, url, headers=None, timeout=10):
    try:
        async with session.get(url, headers=headers or {}, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
            if resp.status != 200:
                return None
            return await resp.json()
    except Exception:
        return None


async def _fetch_nodes(session: aiohttp.ClientSession, token: Optional[str]) -> Optional[List[Dict[str, Any]]]:
    if not MARZBAN_URL:
        return None
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    return await _fetch_generic(session, f"{MARZBAN_URL}/api/nodes", headers, 10)


async def _fetch_system(session: aiohttp.ClientSession, token: Optional[str]) -> Optional[Dict[str, Any]]:
    if not MARZBAN_URL:
        return None
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    return await _fetch_generic(session, f"{MARZBAN_URL}/api/system", headers, 10)


async def _fetch_nodes_usage(session: aiohttp.ClientSession, token: Optional[str],
                             start: Optional[str] = None, end: Optional[str] = None) -> Optional[Dict[str, Any]]:
    if not MARZBAN_URL:
        return None
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    params = {}
    if start:
        params["start"] = start
    if end:
        params["end"] = end
    try:
        async with session.get(f"{MARZBAN_URL}/api/nodes/usage",
                               headers=headers, params=params,
                               timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                return None
            return await resp.json()
    except Exception:
        return None


async def _fetch_users_usage(session: aiohttp.ClientSession, token: Optional[str],
                             start: Optional[str] = None, end: Optional[str] = None) -> Optional[List[Dict[str, Any]]]:
    if not MARZBAN_URL:
        return None
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    params = {}
    if start:
        params["start"] = start
    if end:
        params["end"] = end
    try:
        async with session.get(f"{MARZBAN_URL}/api/users/usage",
                               headers=headers, params=params,
                               timeout=aiohttp.ClientTimeout(total=20)) as resp:
            if resp.status != 200:
                return None
            return await resp.json()
    except Exception:
        return None


def _build_node_base(node: Dict[str, Any]) -> str:
    addr = node.get("address") or node.get("name") or ""
    api_port = node.get("api_port")
    if not addr:
        return ""
    if addr.startswith(("http://", "https://")):
        if ":" not in addr.split("://", 1)[1]:
            if api_port:
                return f"{addr.rstrip('/')}:{api_port}"
            if IP_AGENT_PORT:
                return f"{addr.rstrip('/')}:{IP_AGENT_PORT}"
        return addr.rstrip("/")
    if api_port:
        return f"http://{addr}:{api_port}"
    if IP_AGENT_PORT:
        return f"http://{addr}:{IP_AGENT_PORT}"
    return f"http://{addr}"


async def _try_node_path(session: aiohttp.ClientSession, base: str, path: str, timeout_s: int = 5) -> Optional[Any]:
    url = f"{base.rstrip('/')}{path}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout_s)) as resp:
            if resp.status != 200:
                return {"error": f"{resp.status} {resp.reason}"}
            try:
                return await resp.json()
            except Exception:
                text = await resp.text()
                return {"raw": text}
    except Exception as ex:
        return {"error": str(ex)}


def _normalize_node_response(data: Any) -> Dict[str, Any]:
    clients: List[Any] = []
    count = 0
    count_all = None
    if isinstance(data, list):
        clients = data
        count = len(clients)
        return {"count": count, "clients": clients}
    if isinstance(data, dict):
        if "count_all" in data and isinstance(data["count_all"], (int, float)):
            count_all = int(data["count_all"])
        if "ips" in data and isinstance(data["ips"], list):
            clients = data["ips"]
            count = int(data.get("count", len(clients)))
            return {
                "count": count,
                "count_all": count_all,
                "clients": clients,
                "port": data.get("port"),
                "meta": {
                    k: data.get(k) for k in (
                        "count_ipv4_enabled", "count_ipv6_enabled",
                        "trusted_ips_configured", "count_all"
                    ) if k in data
                }
            }
        for key in ("clients", "connections", "peers", "addresses"):
            if key in data and isinstance(data[key], list):
                clients = data[key]
                count = len(clients)
                return {"count": count, "count_all": count_all, "clients": clients}
        if "count" in data and isinstance(data["count"], int):
            return {"count": data["count"], "count_all": count_all, "clients": []}
        for k, v in data.items():
            if isinstance(v, list):
                clients = v
                count = len(v)
                return {"count": count, "count_all": count_all, "clients": clients}
    return {"count": count, "count_all": count_all, "clients": clients}


async def reconnect_node(session: aiohttp.ClientSession, token: Optional[str], node_id: int) -> bool:
    if not MARZBAN_URL or not token:
        logger.warning("reconnect_node: нет URL или токена (node_id=%s)", node_id)
        return False
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{MARZBAN_URL}/api/node/{node_id}/reconnect"
    try:
        async with session.post(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            ok = resp.status == 200
            if ok:
                logger.info("Успешный reconnect (node_id=%s)", node_id)
            else:
                logger.warning("Неудачный reconnect (node_id=%s status=%s)", node_id, resp.status)
            return ok
    except Exception as e:
        logger.exception("Ошибка reconnect node_id=%s: %s", node_id, e)
        return False


def _build_ip_agent_base(node: Dict[str, Any]) -> Optional[str]:
    addr = node.get("address") or node.get("name") or ""
    if not addr:
        return None
    if addr.startswith(("http://", "https://")):
        host_part = addr.rstrip("/")
        if ":" not in host_part.split("://", 1)[1] and IP_AGENT_PORT:
            return f"{host_part}:{IP_AGENT_PORT}"
        return host_part
    port = IP_AGENT_PORT or node.get("api_port")
    scheme = IP_AGENT_SCHEME or "http"
    if port:
        return f"{scheme}://{addr}:{port}"
    return f"{scheme}://{addr}"


async def fetch_node_clients(session: aiohttp.ClientSession, node: Dict[str, Any]) -> Dict[str, Any]:
    result = {"count": 0, "count_all": None, "clients": [], "detected_path": None, "error": None}
    base_ip_agent = _build_ip_agent_base(node)
    if base_ip_agent:
        res = await _try_node_path(session, base_ip_agent, "/connections", timeout_s=5)
        if res is not None and not (isinstance(res, dict) and res.get("error")):
            norm = _normalize_node_response(res)
            if norm.get("count", 0) > 0 or len(norm.get("clients", [])) > 0 or isinstance(res, (list, dict)):
                if "meta" in norm:
                    result["meta"] = norm["meta"]
                if "port" in norm:
                    result["port"] = norm["port"]
                if norm.get("count_all") is not None:
                    result["count_all"] = norm["count_all"]
                result.update({
                    "count": norm.get("count", 0),
                    "clients": norm.get("clients", []),
                    "detected_path": f"{base_ip_agent}/connections"
                })
                return result
    base = _build_node_base(node)
    if not base:
        result["error"] = "no base address"
        return result
    paths = []
    cfg_path = node.get("clients_path")
    if cfg_path:
        paths.append(cfg_path)
    for p in NODE_CANDIDATE_PATHS:
        if p not in paths:
            paths.append(p)
    for p in paths:
        res = await _try_node_path(session, base, p, timeout_s=5)
        if res is None:
            continue
        if isinstance(res, dict) and res.get("error"):
            continue
        norm = _normalize_node_response(res)
        if norm.get("count", 0) > 0 or len(norm.get("clients", [])) > 0 or isinstance(res, (list, dict)):
            if "meta" in norm:
                result["meta"] = norm["meta"]
            if "port" in norm:
                result["port"] = norm["port"]
            if norm.get("count_all") is not None:
                result["count_all"] = norm.get("count_all")
            result.update({
                "count": norm.get("count", 0),
                "clients": norm.get("clients", []),
                "detected_path": p
            })
            return result
    result["error"] = "no usable endpoint"
    return result


def get_unique_remote_ips(port: int) -> List[str]:
    return []  # Отключено


def _parse_ss_output_for_remote_ips(output: str) -> List[str]:
    ips = set()
    for line in output.splitlines():
        line = line.strip()
        if not line or line.lower().startswith(("netid", "state")):
            continue
        parts = re.split(r"\s+", line)
        if not parts:
            continue
        peer = parts[-1]
        m = re.match(r"^\[?([^\]]+?)\]?:(\d+)$", peer)
        if m:
            ips.add(m.group(1))
    return list(ips)


async def poll_loop():
    logger.info("Запуск poll_loop (interval=%s)", POLL_INTERVAL)
    global first_run
    node_failures: Dict[int, Dict[str, float]] = {}
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                token = await _fetch_token(session)
                tasks_master = [
                    _fetch_nodes(session, token),
                    _fetch_system(session, token),
                    _fetch_nodes_usage(session, token),
                    _fetch_users_usage(session, token),
                ]
                nodes, system_stat, nodes_usage, users_usage = await asyncio.gather(*tasks_master)
                stats["system"] = system_stat
                stats["nodes_usage"] = nodes_usage
                stats["users_usage"] = users_usage
                if nodes is None:
                    stats["error"] = "failed to fetch nodes"
                    stats["nodes"] = []
                    stats["last_update"] = time.time()
                    logger.warning("Не удалось получить список нод")
                    await asyncio.sleep(POLL_INTERVAL)
                    continue
                current_ids = {n.get("id") for n in nodes if n.get("id") is not None}
                node_failures = {nid: data for nid, data in node_failures.items() if nid in current_ids}
                node_entries: List[Dict[str, Any]] = []
                for n in nodes:
                    node_id = n.get("id")
                    status = n.get("status")
                    if node_id and status != "connected" and NODE_RECONNECT_THRESHOLD > 0:
                        if node_id not in node_failures:
                            node_failures[node_id] = {"count": 0, "last_reconnect": 0.0}
                        node_failures[node_id]["count"] += 1
                        current_time = time.time()
                        if (node_failures[node_id]["count"] >= NODE_RECONNECT_THRESHOLD and
                                current_time - node_failures[node_id]["last_reconnect"] > NODE_RECONNECT_COOLDOWN):
                            logger.info("Триггер авто reconnect node_id=%s attempts=%s",
                                        node_id, node_failures[node_id]["count"])
                            success = await reconnect_node(session, token, node_id)
                            node_failures[node_id]["last_reconnect"] = current_time
                            if TELEGRAM_ENABLED:
                                node_name = n.get("name") or n.get("address") or f"Node {node_id}"
                                from telegram_notify import send_telegram_message
                                await send_telegram_message(
                                    f"🔄 <b>Авто переподключение:</b> {node_name}\n"
                                    f"Результат: {'✅ OK' if success else '❌ Fail'}"
                                )
                            if success:
                                node_failures[node_id]["count"] = 0
                    elif node_id and status == "connected" and node_id in node_failures:
                        node_failures[node_id]["count"] = 0
                    node_entries.append({
                        "id": node_id,
                        "name": n.get("name"),
                        "address": n.get("address"),
                        "api_port": n.get("api_port"),
                        "status": status,
                        "message": n.get("message"),
                        "clients_count": None,
                        "clients": [],
                        "detected_path": None,
                        "clients_error": None,
                        "uplink": None,
                        "downlink": None,
                        "reconnect_attempts": node_failures.get(node_id, {}).get("count", 0)
                    })
                if nodes_usage and isinstance(nodes_usage, dict):
                    usages = nodes_usage.get("usages") or []
                    for entry in node_entries:
                        for u in usages:
                            if ((entry["id"] is not None and u.get("node_id") == entry["id"]) or
                                    (u.get("node_name") and u.get("node_name") == entry.get("name"))):
                                entry["uplink"] = u.get("uplink")
                                entry["downlink"] = u.get("downlink")
                                break
                tasks = [fetch_node_clients(session, n) for n in nodes]
                clients_results = await asyncio.gather(*tasks, return_exceptions=True)
                for i, res in enumerate(clients_results):
                    if isinstance(res, Exception):
                        node_entries[i]["clients_error"] = str(res)
                        node_entries[i]["clients_count"] = None
                        continue
                    node_entries[i]["clients_count"] = res.get("count", 0)
                    node_entries[i]["count_all"] = res.get("count_all")
                    node_entries[i]["clients"] = res.get("clients", [])
                    node_entries[i]["detected_path"] = res.get("detected_path")
                    node_entries[i]["clients_error"] = res.get("error")
                    if res.get("port") is not None:
                        node_entries[i]["clients_port"] = res.get("port")
                    if res.get("meta") is not None:
                        node_entries[i]["clients_meta"] = res.get("meta")
                if TELEGRAM_ENABLED:
                    is_first_time = first_run
                    if first_run:
                        first_run = False
                    await process_notifications(node_entries, is_first_time)
                stats["nodes"] = node_entries
                stats["port_8443"] = {"unique_clients": 0, "clients": []}
                stats["error"] = None
                stats["last_update"] = time.time()
                logger.debug("poll_loop обновлен: nodes=%s", len(node_entries))
            except Exception as ex:
                logger.exception("Итерация poll_loop с ошибкой: %s", ex)
                stats["error"] = str(ex)
                stats["nodes"] = []
                stats["last_update"] = time.time()
            await asyncio.sleep(POLL_INTERVAL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Старт приложения (lifespan)")
    if not MARZBAN_URL:
        stats["error"] = "MARZBAN_URL not configured"
    task = asyncio.create_task(poll_loop())
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        logger.info("Остановка приложения (lifespan)")


def human_bytes(num: Optional[int]) -> str:
    if num is None:
        return "—"
    for unit in ['Б', 'КБ', 'МБ', 'ГБ', 'ТБ', 'ПБ']:
        if abs(num) < 1024.0:
            if unit == 'Б':
                return f"{num} {unit}"
            return f"{num:.2f} {unit}"
        num /= 1024.0
    return f"{num:.2f} ЭБ"


def get_usage_range(period: str) -> tuple[Optional[str], Optional[str]]:
    now = datetime.utcnow()
    if period == "1d":
        start = (now - timedelta(days=1)).isoformat(timespec="seconds") + "Z"
    elif period == "1w":
        start = (now - timedelta(weeks=1)).isoformat(timespec="seconds") + "Z"
    elif period == "1m":
        start = (now - timedelta(days=30)).isoformat(timespec="seconds") + "Z"
    else:
        return (None, None)
    return (start, now.isoformat(timespec="seconds") + "Z")


def status_badge_class(status: Optional[str]) -> str:
    if status == "connected":
        return "bg-success"
    elif status == "connecting":
        return "bg-warning text-dark"
    else:
        return "bg-danger"


APP = FastAPI(lifespan=lifespan)


@APP.get("/api/stats")
async def api_stats():
    return JSONResponse(content=stats)


@APP.post("/api/reconnect/{node_id}")
async def api_reconnect(node_id: int):
    logger.info("Ручной запрос reconnect node_id=%s", node_id)
    async with aiohttp.ClientSession() as session:
        token = await _fetch_token(session)
        if not token:
            logger.warning("Ручной reconnect: не удалось получить токен (node_id=%s)", node_id)
            return JSONResponse(content={"success": False, "error": "Failed to authenticate"}, status_code=401)
        success = await reconnect_node(session, token, node_id)
        if success and TELEGRAM_ENABLED:
            for n in stats.get("nodes", []):
                if n.get("id") == node_id:
                    node_name = n.get("name") or n.get("address") or f"Node {node_id}"
                    from telegram_notify import send_telegram_message
                    await send_telegram_message(f"🔄 <b>Ручное переподключение:</b> {node_name}")
                    break
        logger.info("Ручной reconnect node_id=%s result=%s", node_id, success)
        if success:
            return JSONResponse(content={"success": True, "message": "Node reconnect request sent"})
        return JSONResponse(content={"success": False, "error": "Failed to reconnect node"}, status_code=500)


@APP.get("/", response_class=HTMLResponse)
async def index(request: Request):
    nodes = stats.get("nodes", [])
    last = stats.get("last_update")
    err = stats.get("error")
    system = stats.get("system")
    port_info = stats.get("port_8443", {})
    last_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(last)) if last else "—"
    total_clients = sum(int(n.get('clients_count') or 0) for n in nodes)
    total_connections = sum(int(n.get('count_all') or n.get('clients_count') or 0) for n in nodes)
    header = f"""
    <div class="mb-3 d-flex flex-wrap align-items-center">
        <span class="badge bg-secondary">Последнее обновление: {last_str}</span>
        <span class="badge bg-info text-dark ms-2">Подключений к порту {MONITOR_PORT}: {port_info.get('unique_clients', '—')}</span>
        <span class="badge bg-dark ms-2">Уникальных клиентов: {total_clients}</span>
        <span class="badge bg-primary ms-2">Всего соединений: {total_connections}</span>
        {'<span class="badge bg-success ms-2">Telegram уведомления: включены</span>' if TELEGRAM_ENABLED else '<span class="badge bg-danger ms-2">Telegram уведомления: выключены</span>'}
    </div>
    """
    if system:
        header += f"""
        <div class="mb-3">
            <span class="badge bg-success">Online users (master): {system.get('online_users', '—')}</span>
            <span class="badge bg-primary ms-2">Incoming bandwidth: {human_bytes(system.get('incoming_bandwidth'))}</span>
            <span class="badge bg-primary ms-2">Outgoing bandwidth: {human_bytes(system.get('outgoing_bandwidth'))}</span>
        </div>
        """
    items = ""
    for n in nodes:
        count_all = n.get('count_all')
        clients_count = n.get('clients_count')
        reconnect_attempts = n.get('reconnect_attempts', 0)
        reconnect_info = ""
        if reconnect_attempts > 0:
            reconnect_info = f'<div class="alert alert-warning mt-2">Попыток переподключения: {reconnect_attempts}</div>'
        items += f"""
        <div class="col">
            <div class="card shadow-sm mb-4">
                <div class="card-header bg-light d-flex justify-content-between align-items-center">
                    <b>{n.get('name') or n.get('address')}</b>
                    <div class="d-flex align-items-center">
                        <span class="badge {status_badge_class(n.get('status'))}">{n.get('status') or '—'}</span>
                        <button class="btn btn-sm btn-outline-secondary ms-2 reconnect-btn"
                                data-node-id="{n.get('id')}"
                                data-bs-toggle="tooltip"
                                title="Переподключить ноду">
                            <i class="bi bi-arrow-clockwise"></i>
                        </button>
                    </div>
                </div>
                <div class="card-body">
                    <ul class="list-group list-group-flush">
                        <li class="list-group-item"><b>Address:</b> {n.get('address') or '—'}</li>
                        <li class="list-group-item"><b>API port:</b> {n.get('api_port') or '—'}</li>
                        <li class="list-group-item">
                            <b>Клиентов:</b> {clients_count if clients_count is not None else '—'}
                            {f" <span class='text-muted'>({count_all} соед.)</span>" if count_all is not None and count_all != clients_count else ""}
                        </li>
                        <li class="list-group-item"><b>Uplink:</b> {human_bytes(n.get('uplink'))} <b>Downlink:</b> {human_bytes(n.get('downlink'))}</li>
                    </ul>
                    {"<div class='alert alert-danger mt-2'>Clients error: " + n.get('clients_error') + "</div>" if n.get('clients_error') else ""}
                    {reconnect_info}
                </div>
            </div>
        </div>
        """
    if not items:
        items = "<div class='alert alert-warning'>Ноды не обнаружены.</div>"
    html = f"""<!doctype html>
<html lang="ru">
<head>
    <meta charset="utf-8">
    <title>Marzban nodes</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.2/font/bootstrap-icons.min.css">
</head>
<body class="bg-light">
<div class="container py-4">
    <h1 class="mb-4">Marzban — Ноды</h1>
    {header}
    <div style="color:#b00">{err or ''}</div>
    <div class="row row-cols-1 row-cols-md-2 row-cols-lg-3 g-4">
        {items}
    </div>
</div>
<a href="https://github.com/Makar-aka/marz-balancer"
   target="_blank" rel="noopener noreferrer"
   class="position-fixed end-0 bottom-0 m-3 small text-muted text-decoration-underline"
   style="z-index:9999;">
   &copy; MakarSPB
</a>
<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
<script>
document.addEventListener('DOMContentLoaded', function() {{
    const tooltipTriggerList = document.querySelectorAll('[data-bs-toggle="tooltip"]');
    [...tooltipTriggerList].forEach(el => new bootstrap.Tooltip(el));
    document.querySelectorAll('.reconnect-btn').forEach(btn => {{
        btn.addEventListener('click', async function() {{
            const nodeId = this.getAttribute('data-node-id');
            if (!nodeId) return;
            if (!confirm('Переподключить ноду?')) return;
            this.disabled = true;
            const original = this.innerHTML;
            this.innerHTML = '<span class="spinner-border spinner-border-sm"></span>';
            try {{
                const resp = await fetch(`/api/reconnect/${{nodeId}}`, {{method:'POST'}});
                const data = await resp.json();
                alert(data.success ? 'Отправлено' : ('Ошибка: ' + (data.error||'unknown')));
                setTimeout(()=>location.reload(), 1500);
            }} catch(e) {{
                alert('Ошибка: ' + e.message);
                this.disabled = false;
                this.innerHTML = original;
            }}
        }});
    }});
}});
setTimeout(()=>location.reload(), {int(POLL_INTERVAL*1000)});
</script>
</body>
</html>"""
    return HTMLResponse(content=html)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("marz_balancer:APP", host="0.0.0.0", port=APP_PORT, reload=True)