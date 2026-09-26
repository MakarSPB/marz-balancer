import os
import time
import asyncio
import subprocess
import re
import sqlite3
import secrets
from urllib.parse import parse_qs
from typing import Dict, Any, Optional, List
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

import aiohttp
from dotenv import load_dotenv
from fastapi import FastAPI, Request, Depends, HTTPException, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials

load_dotenv()


def _to_bool(value: Optional[str], default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "on")


MARZBAN_URL = os.getenv("MARZBAN_URL", "").rstrip("/")
MARZBAN_ADMIN_USER = os.getenv("MARZBAN_ADMIN_USER", "")
MARZBAN_ADMIN_PASS = os.getenv("MARZBAN_ADMIN_PASS", "")
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "5"))
APP_PORT = int(os.getenv("APP_PORT", "8023"))
IP_AGENT_PORT = os.getenv("IP_AGENT_PORT", "").strip()
IP_AGENT_SCHEME = os.getenv("IP_AGENT_SCHEME", "http").strip()
IP_AGENT_ENABLED = _to_bool(os.getenv("IP_AGENT_ENABLED", "1"), default=True)
TELEGRAM_PROXY_URL = os.getenv("TELEGRAM_PROXY_URL", "").strip().rstrip("/")
TELEGRAM_API_BASE = TELEGRAM_PROXY_URL or "https://api.telegram.org"

# Telegram notifications
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_MIN_INTERVAL = int(os.getenv("TELEGRAM_MIN_INTERVAL", "300"))
TELEGRAM_NOTIFY_ON_ONLINE = _to_bool(os.getenv("TELEGRAM_NOTIFY_ON_ONLINE", "1"), default=True)
TELEGRAM_NOTIFY_ON_OFFLINE = _to_bool(os.getenv("TELEGRAM_NOTIFY_ON_OFFLINE", "1"), default=True)
TELEGRAM_NOTIFY_ON_CONNECTING = _to_bool(os.getenv("TELEGRAM_NOTIFY_ON_CONNECTING", "0"), default=False)

# UI auth
UI_LOGIN = os.getenv("UI_LOGIN", "").strip()
UI_PASSWORD = os.getenv("UI_PASSWORD", "").strip()

SETTINGS_DB_PATH = os.getenv("SETTINGS_DB_PATH", "/data/settings.db").strip() or "/data/settings.db"
MONITOR_PORT = int(os.getenv("MONITOR_PORT", "8443"))

NODE_CANDIDATE_PATHS = [
    "/connections",
    "/clients",
    "/status",
]

# runtime state
stats: Dict[str, Any] = {
    "nodes": [],
    "last_update": None,
    "error": None,
    "system": None,
    "nodes_usage": None,
    "users_usage": None,
    "telegram_api_base": TELEGRAM_API_BASE,
    "reconnect_attempts": [],
    "port_8443": {"unique_clients": 0, "clients": []},
}
_token_cache: Dict[str, Any] = {"token": None, "fetched_at": 0, "ttl": 300}
_last_master_error = ""
_node_status_cache: Dict[str, str] = {}


def _init_settings_db() -> bool:
    try:
        db_dir = os.path.dirname(SETTINGS_DB_PATH)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        with sqlite3.connect(SETTINGS_DB_PATH) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS app_settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
            )
        return True
    except Exception:
        return False


def _read_settings_db() -> Dict[str, str]:
    try:
        with sqlite3.connect(SETTINGS_DB_PATH) as conn:
            rows = conn.execute("SELECT key, value FROM app_settings").fetchall()
        return {k: v for k, v in rows}
    except Exception:
        return {}


def _save_settings_db(updates: Dict[str, str]) -> bool:
    if not updates:
        return True
    try:
        with sqlite3.connect(SETTINGS_DB_PATH) as conn:
            conn.executemany(
                "INSERT INTO app_settings(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                list(updates.items()),
            )
        return True
    except Exception:
        return False


def _apply_saved_settings() -> None:
    global MARZBAN_URL, MARZBAN_ADMIN_USER, MARZBAN_ADMIN_PASS
    global IP_AGENT_ENABLED
    global TELEGRAM_PROXY_URL, TELEGRAM_API_BASE, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
    global TELEGRAM_NOTIFY_ON_ONLINE, TELEGRAM_NOTIFY_ON_OFFLINE, TELEGRAM_NOTIFY_ON_CONNECTING

    saved = _read_settings_db()
    if not saved:
        return

    if "MARZBAN_URL" in saved:
        MARZBAN_URL = (saved.get("MARZBAN_URL", "") or "").strip().rstrip("/")
    if "MARZBAN_ADMIN_USER" in saved:
        MARZBAN_ADMIN_USER = (saved.get("MARZBAN_ADMIN_USER", "") or "").strip()
    if "MARZBAN_ADMIN_PASS" in saved:
        MARZBAN_ADMIN_PASS = (saved.get("MARZBAN_ADMIN_PASS", "") or "").strip()

    if "IP_AGENT_ENABLED" in saved:
        IP_AGENT_ENABLED = _to_bool(saved.get("IP_AGENT_ENABLED"), default=True)

    if "TELEGRAM_PROXY_URL" in saved:
        TELEGRAM_PROXY_URL = (saved.get("TELEGRAM_PROXY_URL", "") or "").strip().rstrip("/")
    if "TELEGRAM_BOT_TOKEN" in saved:
        TELEGRAM_BOT_TOKEN = (saved.get("TELEGRAM_BOT_TOKEN", "") or "").strip()
    if "TELEGRAM_CHAT_ID" in saved:
        TELEGRAM_CHAT_ID = (saved.get("TELEGRAM_CHAT_ID", "") or "").strip()

    if "TELEGRAM_NOTIFY_ON_ONLINE" in saved:
        TELEGRAM_NOTIFY_ON_ONLINE = _to_bool(saved.get("TELEGRAM_NOTIFY_ON_ONLINE"), default=True)
    if "TELEGRAM_NOTIFY_ON_OFFLINE" in saved:
        TELEGRAM_NOTIFY_ON_OFFLINE = _to_bool(saved.get("TELEGRAM_NOTIFY_ON_OFFLINE"), default=True)
    if "TELEGRAM_NOTIFY_ON_CONNECTING" in saved:
        TELEGRAM_NOTIFY_ON_CONNECTING = _to_bool(saved.get("TELEGRAM_NOTIFY_ON_CONNECTING"), default=False)

    TELEGRAM_API_BASE = TELEGRAM_PROXY_URL or "https://api.telegram.org"
    stats["telegram_api_base"] = TELEGRAM_API_BASE
    _token_cache["token"] = None
    _token_cache["fetched_at"] = 0


_init_settings_db()
_apply_saved_settings()


async def _fetch_token(session: aiohttp.ClientSession) -> Optional[str]:
    global _last_master_error
    if not MARZBAN_URL or not MARZBAN_ADMIN_USER or not MARZBAN_ADMIN_PASS:
        _last_master_error = "MARZBAN settings are incomplete (url/user/pass)"
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
                _last_master_error = f"token request failed: {resp.status} {resp.reason}"
                return None
            j = await resp.json()
            token = j.get("access_token") or j.get("token")
            if token:
                _token_cache["token"] = token
                _token_cache["fetched_at"] = now
                _last_master_error = ""
                return token
            _last_master_error = "token not found in response"
    except Exception as ex:
        _last_master_error = f"token request exception: {ex}"
        return None
    return None

async def _fetch_nodes(session: aiohttp.ClientSession, token: Optional[str]) -> Optional[List[Dict[str, Any]]]:
    global _last_master_error
    if not MARZBAN_URL:
        _last_master_error = "MARZBAN_URL is empty"
        return None
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    url = f"{MARZBAN_URL}/api/nodes"
    try:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                _last_master_error = f"nodes request failed: {resp.status} {resp.reason}"
                return None
            _last_master_error = ""
            return await resp.json()
    except Exception as ex:
        _last_master_error = f"nodes request exception: {ex}"
        return None

async def _reconnect_node(session: aiohttp.ClientSession, token: Optional[str], node_id: Any) -> Dict[str, Any]:
    if not MARZBAN_URL:
        return {"ok": False, "error": "MARZBAN_URL is empty"}
    if not token:
        return {"ok": False, "error": "token is missing"}
    if node_id is None:
        return {"ok": False, "error": "node_id is missing"}

    headers = {"Authorization": f"Bearer {token}"}
    url = f"{MARZBAN_URL}/api/node/{node_id}/reconnect"
    try:
        async with session.post(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status not in (200, 202, 204):
                body = await resp.text()
                return {"ok": False, "error": f"{resp.status} {resp.reason}", "body": body[:300]}
            return {"ok": True}
    except Exception as ex:
        return {"ok": False, "error": str(ex)}


async def _fetch_system(session: aiohttp.ClientSession, token: Optional[str]) -> Optional[Dict[str, Any]]:
    if not MARZBAN_URL:
        return None
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    url = f"{MARZBAN_URL}/api/system"
    try:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                return None
            return await resp.json()
    except Exception:
        return None

async def _fetch_nodes_usage(session: aiohttp.ClientSession, token: Optional[str], start: Optional[str] = None, end: Optional[str] = None) -> Optional[Dict[str, Any]]:
    if not MARZBAN_URL:
        return None
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    params = {}
    if start:
        params["start"] = start
    if end:
        params["end"] = end
    url = f"{MARZBAN_URL}/api/nodes/usage"
    try:
        async with session.get(url, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                return None
            return await resp.json()
    except Exception:
        return None

async def _fetch_users_usage(session: aiohttp.ClientSession, token: Optional[str], start: Optional[str] = None, end: Optional[str] = None) -> Optional[List[Dict[str, Any]]]:
    if not MARZBAN_URL:
        return None
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    params = {}
    if start:
        params["start"] = start
    if end:
        params["end"] = end
    url = f"{MARZBAN_URL}/api/users/usage"
    try:
        async with session.get(url, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=20)) as resp:
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
    if addr.startswith("http://") or addr.startswith("https://"):
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
    if isinstance(data, list):
        clients = data
        count = len(clients)
        return {"count": count, "clients": clients}
    if isinstance(data, dict):
        if "ips" in data and isinstance(data["ips"], list):
            clients = data["ips"]
            count = int(data.get("count", len(clients)))
            return {"count": count, "clients": clients, "port": data.get("port"), "meta": {k: data.get(k) for k in ("count_ipv4_enabled", "count_ipv6_enabled", "trusted_ips_configured") if k in data}}
        for key in ("clients", "connections", "peers", "addresses"):
            if key in data and isinstance(data[key], list):
                clients = data[key]
                count = len(clients)
                return {"count": count, "clients": clients}
        if "count" in data and isinstance(data["count"], int):
            return {"count": data["count"], "clients": []}
        for k, v in data.items():
            if isinstance(v, list):
                clients = v
                count = len(v)
                return {"count": count, "clients": clients}
    return {"count": count, "clients": clients}

def _build_ip_agent_base(node: Dict[str, Any]) -> Optional[str]:
    addr = node.get("address") or node.get("name") or ""
    if not addr:
        return None
    if addr.startswith("http://") or addr.startswith("https://"):
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
    result = {"count": 0, "clients": [], "detected_path": None, "error": None}
    if not IP_AGENT_ENABLED:
        result["detected_path"] = "ip-agent-disabled"
        return result

    base_ip_agent = _build_ip_agent_base(node)
    if IP_AGENT_ENABLED and base_ip_agent:
        res = await _try_node_path(session, base_ip_agent, "/connections", timeout_s=5)
        if res is not None and not (isinstance(res, dict) and res.get("error")):
            norm = _normalize_node_response(res)
            if norm.get("count", 0) > 0 or len(norm.get("clients", [])) > 0 or isinstance(res, (list, dict)):
                if isinstance(norm, dict) and "meta" in norm:
                    result["meta"] = norm["meta"]
                if isinstance(norm, dict) and "port" in norm:
                    result["port"] = norm["port"]
                result.update({"count": norm.get("count", 0), "clients": norm.get("clients", []), "detected_path": f"{base_ip_agent}/connections"})
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
            if isinstance(norm, dict) and "meta" in norm:
                result["meta"] = norm["meta"]
            if isinstance(norm, dict) and "port" in norm:
                result["port"] = norm["port"]
            result.update({"count": norm.get("count", 0), "clients": norm.get("clients", []), "detected_path": p})
            return result

    result["error"] = "no usable endpoint"
    return result

def _parse_ss_output_for_remote_ips(output: str) -> List[str]:
    ips = set()
    for line in output.splitlines():
        line = line.strip()
        if not line or line.lower().startswith("netid") or line.lower().startswith("state"):
            continue
        parts = re.split(r"\s+", line)
        if len(parts) < 1:
            continue
        peer = parts[-1]
        m = re.match(r"^\[?([^\]]+?)\]?:(\d+)$", peer)
        if m:
            ip = m.group(1)
            ips.add(ip)
    return sorted(ips)


def get_unique_remote_ips(port: int) -> List[str]:
    try:
        output = subprocess.check_output(
            ["ss", "-tn", f"sport = :{port}"],
            stderr=subprocess.DEVNULL,
            text=True,
        )
        return _parse_ss_output_for_remote_ips(output)
    except Exception:
        return []


# last-sent timestamp to avoid spamming
_tg_last_sent: Dict[str, float] = {"at": 0.0}
_tg_spam_cache: Dict[str, Dict[str, Any]] = {}  # message_hash -> {timestamp, content}
_TELEGRAM_SPAM_TTL = 300  # 5 minutes

async def send_telegram_message(session: aiohttp.ClientSession, text: str, force: bool = False) -> bool:
    """Отправляет текстовое уведомление в указанный чат Telegram через configured proxy/base.
    Возвращает True при успешной отправке, False в противном случае или если параметры не заданы.
    Защита от спама: не отправляет одинаковые сообщения чаще чем раз в 5 минут (если не force=True).
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False

    now = time.time()
    msg_hash = hash(text)

    # Спам-кэш: проверяем, отправляли ли мы это сообщение недавно
    if not force and msg_hash in _tg_spam_cache:
        cached = _tg_spam_cache[msg_hash]
        if now - cached["timestamp"] < _TELEGRAM_SPAM_TTL:
            return False

    # Проверка интервала между сообщениями
    if not force and now - _tg_last_sent.get("at", 0) < TELEGRAM_MIN_INTERVAL:
        return False

    try:
        url = f"{TELEGRAM_API_BASE}/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text}
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 200:
                _tg_last_sent["at"] = now
                # Добавляем в спам-кэш
                _tg_spam_cache[msg_hash] = {"timestamp": now, "content": text}
                return True
    except Exception:
        return False
    return False


def _normalize_node_state(status_value: Any) -> Optional[str]:
    status_text = str(status_value or "").strip().lower()
    if status_text in ("connected", "online", "healthy", "active", "up"):
        return "online"
    if status_text in ("disconnected", "offline", "error", "failed", "down"):
        return "offline"
    return None

def _should_notify_status(current_state: str) -> bool:
    """Check if we should send notification for this status"""
    if current_state == "online":
        return TELEGRAM_NOTIFY_ON_ONLINE
    elif current_state == "offline":
        return TELEGRAM_NOTIFY_ON_OFFLINE
    elif current_state == "connecting":
        return TELEGRAM_NOTIFY_ON_CONNECTING
    return False

async def poll_loop():
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
                    stats["error"] = _last_master_error or "failed to fetch nodes"
                    stats["debug"] = {
                        "marzban_url": MARZBAN_URL,
                        "marzban_user_set": bool(MARZBAN_ADMIN_USER),
                        "marzban_pass_set": bool(MARZBAN_ADMIN_PASS),
                    }
                    stats["nodes"] = []
                    stats["last_update"] = time.time()
                    await asyncio.sleep(POLL_INTERVAL)
                    continue

                node_entries: List[Dict[str, Any]] = []
                for n in nodes:
                    entry = {
                        "id": n.get("id"),
                        "name": n.get("name"),
                        "address": n.get("address"),
                        "api_port": n.get("api_port"),
                        "status": n.get("status"),
                        "message": n.get("message"),
                        "clients_count": None,
                        "clients": [],
                        "detected_path": None,
                        "clients_error": None,
                        "uplink": None,
                        "downlink": None,
                    }
                    node_entries.append(entry)

                status_change_messages: List[str] = []
                for entry in node_entries:
                    node_key = str(entry.get("id")) if entry.get("id") is not None else (entry.get("name") or entry.get("address") or "")
                    if not node_key:
                        continue
                    current_state = _normalize_node_state(entry.get("status"))
                    previous_state = _node_status_cache.get(node_key)
                    if previous_state and current_state and previous_state != current_state:
                        if _should_notify_status(current_state):
                            node_label = entry.get("name") or entry.get("address") or f"node-{node_key}"
                            status_change_messages.append(f"Нода {node_label}: {previous_state} -> {current_state}")
                    elif not previous_state and current_state == "online" and TELEGRAM_NOTIFY_ON_ONLINE:
                        # First time seeing this node and it's online
                        node_label = entry.get("name") or entry.get("address") or f"node-{node_key}"
                        status_change_messages.append(f"Нода {node_label}: впервые обнаружена online")
                    if current_state:
                        _node_status_cache[node_key] = current_state

                if status_change_messages:
                    for message in status_change_messages:
                        await send_telegram_message(session, message, force=True)
                for entry in node_entries:
                    status_value = str(entry.get("status") or "").strip().lower()
                    if status_value in ("connected", "online"):
                        continue
                    reconnect_result = await _reconnect_node(session, token, entry.get("id"))
                    reconnect_attempts.append(
                        {
                            "node_id": entry.get("id"),
                            "node_name": entry.get("name") or entry.get("address"),
                            "status": entry.get("status"),
                            "ok": reconnect_result.get("ok", False),
                            "error": reconnect_result.get("error"),
                        }
                    )

                stats["reconnect_attempts"] = reconnect_attempts

                if nodes_usage and isinstance(nodes_usage, dict):
                    usages = nodes_usage.get("usages") or []
                    for entry in node_entries:
                        for u in usages:
                            if (entry["id"] is not None and u.get("node_id") == entry["id"]) or (u.get("node_name") and u.get("node_name") == entry.get("name")):
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
                    node_entries[i]["clients"] = res.get("clients", [])
                    node_entries[i]["detected_path"] = res.get("detected_path")
                    node_entries[i]["clients_error"] = res.get("error")
                    if res.get("port") is not None:
                        node_entries[i]["clients_port"] = res.get("port")
                    if res.get("meta") is not None:
                        node_entries[i]["clients_meta"] = res.get("meta")

                stats["nodes"] = node_entries

                try:
                    unique_ips = await asyncio.to_thread(get_unique_remote_ips, MONITOR_PORT)
                    stats["port_8443"] = {"unique_clients": len(unique_ips), "clients": unique_ips[:200]}
                except Exception:
                    stats["port_8443"] = {"unique_clients": 0, "clients": []}

                stats["error"] = None
                stats["last_update"] = time.time()
            except Exception as ex:
                stats["error"] = str(ex)
                stats["nodes"] = []
                # отправляем уведомление в Telegram (если настроено) — не блокируем цикл
                try:
                    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
                        asyncio.create_task(send_telegram_message(session, f"MarzBalancer error: {str(ex)}"))
                except Exception:
                    pass
                stats["last_update"] = time.time()
            await asyncio.sleep(POLL_INTERVAL)

@asynccontextmanager
async def lifespan(app: FastAPI):
    if not MARZBAN_URL:
        stats["error"] = "MARZBAN_URL not configured"
    # Send startup notification
    await send_telegram_message(
        None,
        "Привет! Я монитор нод Marzban. Я подключился 👋",
        force=True
    )
    task = asyncio.create_task(poll_loop())
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

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


security = HTTPBasic(auto_error=False)


def require_ui_auth(credentials: Optional[HTTPBasicCredentials] = Depends(security)) -> str:
    if not UI_LOGIN or not UI_PASSWORD:
        raise HTTPException(status_code=503, detail="UI auth not configured")
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Basic"},
        )

    is_login_ok = secrets.compare_digest(credentials.username, UI_LOGIN)
    is_password_ok = secrets.compare_digest(credentials.password, UI_PASSWORD)
    if not (is_login_ok and is_password_ok):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username


APP = FastAPI(lifespan=lifespan, dependencies=[Depends(require_ui_auth)])

@APP.get("/api/stats")
async def api_stats():
    return JSONResponse(content=stats)

@APP.get("/", response_class=HTMLResponse)
async def index(request: Request):
    nodes = stats.get("nodes", [])
    last = stats.get("last_update")
    err = stats.get("error")
    system = stats.get("system") or {}
    port_info = stats.get("port_8443", {})
    last_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(last)) if last else "—"

    total_clients = sum(int(n.get("clients_count") or 0) for n in nodes)
    online_nodes = sum(1 for n in nodes if str(n.get("status") or "").lower() in ("connected", "online", "healthy"))

    items = ""
    for n in nodes:
        status_raw = str(n.get("status") or "—")
        status_key = status_raw.lower()
        status_class = "badge bg-secondary"
        if status_key in ("connected", "online", "healthy"):
            status_class = "badge bg-success"
        elif status_key in ("error", "offline", "disconnected"):
            status_class = "badge bg-danger"
        clients_err = n.get("clients_error")
        items += f"""
        <article class="node-card">
            <div class="node-card-head">
                <h3>{n.get('name') or n.get('address') or 'unknown-node'}</h3>
                <span class="{status_class}">{status_raw}</span>
            </div>
            <div class="node-grid">
                <div><span>Адрес</span><strong>{n.get('address') or '—'}</strong></div>
                <div><span>API порт</span><strong>{n.get('api_port') or '—'}</strong></div>
                <div><span>Клиенты</span><strong>{n.get('clients_count') if n.get('clients_count') is not None else '—'}</strong></div>
                <div><span>Uplink / Downlink</span><strong>{human_bytes(n.get('uplink'))} / {human_bytes(n.get('downlink'))}</strong></div>
            </div>
            {f"<div class='node-error'>Ошибка клиентов: {clients_err}</div>" if clients_err else ""}
        </article>
        """

    if not items:
        items = "<div class='empty-state'>Ноды не обнаружены.</div>"

    html = f"""<!doctype html>
<html lang="ru">
<head>
    <meta charset="utf-8">
    <title>MarzBalancer Dashboard</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        body {{ background: #0b1020; color: #d8e1ff; margin: 0; }}
        .navbar-custom {{ background: #080e1f; border-bottom: 1px solid #27345b; padding: 12px 0; position: sticky; top: 0; z-index: 100; }}
        .navbar-title {{ font-weight: 700; font-size: 1.2rem; margin: 0; color: #d8e1ff; }}
        .nav-buttons {{ display: flex; gap: 8px; align-items: center; }}
        .nav-btn {{ padding: 6px 14px; border: 1px solid #4b6bb0; border-radius: 8px; text-decoration: none; color: #b9c8ef; font-size: 0.95rem; transition: all 0.2s; }}
        .nav-btn:hover {{ background: #1a2847; color: #d8e1ff; border-color: #6b8fd9; }}
        .nav-btn.active {{ background: #2b4a8c; color: #d8e1ff; border-color: #6b8fd9; }}
        .navbar-wrapper {{ max-width: 1280px; margin: 0 auto; padding: 0 20px; display: flex; justify-content: space-between; align-items: center; }}
        .app-wrap {{ max-width: 1280px; margin: 0 auto; padding: 28px 20px 36px; }}
        .hero {{ display:flex; justify-content:space-between; gap:16px; align-items:flex-start; margin-bottom:22px; flex-wrap:wrap; }}
        .hero h1 {{ margin:0; font-size:1.8rem; font-weight:700; }}
        .hero p {{ margin:6px 0 0; color:#9fb0de; }}
        .stats-grid {{ display:grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap:12px; margin-bottom:16px; }}
        .stat-card {{ background:#131b33; border:1px solid #27345b; border-radius:14px; padding:14px 16px; }}
        .stat-card span {{ display:block; color:#93a6d8; font-size:.86rem; margin-bottom:4px; }}
        .stat-card strong {{ font-size:1.2rem; }}
        .error-banner {{ background:#4a1d2a; border:1px solid #a83f58; color:#ffd4df; border-radius:10px; padding:10px 12px; margin-bottom:16px; }}
        .nodes-grid {{ display:grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap:14px; }}
        .node-card {{ background:#121a30; border:1px solid #2b3d69; border-radius:14px; padding:14px; }}
        .node-card-head {{ display:flex; justify-content:space-between; align-items:center; gap:10px; margin-bottom:10px; }}
        .node-card-head h3 {{ margin:0; font-size:1.05rem; }}
        .node-grid {{ display:grid; grid-template-columns: 1fr 1fr; gap:10px 12px; }}
        .node-grid span {{ display:block; font-size:.8rem; color:#8ea2d9; margin-bottom:1px; }}
        .node-grid strong {{ font-size:.95rem; color:#ecf2ff; }}
        .node-error {{ margin-top:12px; color:#ffd4df; background:#4a1d2a; border:1px solid #a83f58; border-radius:10px; padding:8px 10px; font-size:.9rem; }}
        .empty-state {{ grid-column:1/-1; background:#1a233f; border:1px dashed #4b5f92; color:#b9c8ef; border-radius:12px; padding:20px; text-align:center; }}
        .footer-link {{ position:fixed; right:16px; bottom:12px; color:#91a4dc; text-decoration:none; font-size:.85rem; opacity:.8; }}
        .footer-link:hover {{ opacity:1; color:#c7d5ff; }}
        @media (max-width: 700px) {{ .node-grid {{ grid-template-columns: 1fr; }} .nav-buttons {{ flex-direction: column; width: 100%; margin-top: 12px; }} }}
    </style>
</head>
<body>
    <nav class="navbar-custom">
        <div class="navbar-wrapper">
            <h2 class="navbar-title">MarzBalancer</h2>
            <div class="nav-buttons">
                <a href="/" class="nav-btn active">Статус нод</a>
                <a href="/reconnects" class="nav-btn">Переподключения</a>
                <a href="/settings" class="nav-btn">Настройки</a>
            </div>
        </div>
    </nav>
    <main class="app-wrap">
        <section class="hero">
            <div>
                <h1>Статус нод</h1>
                <p>Состояние нод и агрегированная статистика в реальном времени</p>
            </div>
            <span class="badge text-bg-secondary">Обновлено: {last_str}</span>
        </section>

        <section class="stats-grid">
            <div class="stat-card"><span>Ноды онлайн</span><strong>{online_nodes} / {len(nodes)}</strong></div>
            <div class="stat-card"><span>Активные клиенты</span><strong>{total_clients}</strong></div>
            <div class="stat-card"><span>Подключения к порту {MONITOR_PORT}</span><strong>{port_info.get('unique_clients', '—')}</strong></div>
            <div class="stat-card"><span>Online users (master)</span><strong>{system.get('online_users', '—')}</strong></div>
            <div class="stat-card"><span>Incoming bandwidth</span><strong>{human_bytes(system.get('incoming_bandwidth'))}</strong></div>
            <div class="stat-card"><span>Outgoing bandwidth</span><strong>{human_bytes(system.get('outgoing_bandwidth'))}</strong></div>
        </section>

        {f"<div class='error-banner'>{err}</div>" if err else ""}

        <section class="nodes-grid">
            {items}
        </section>
    </main>

    <a href="https://github.com/MakarSPB/marz-balancer" target="_blank" rel="noopener noreferrer" class="footer-link">&copy; MakarSPB</a>

    <script>
        setTimeout(() => location.reload(), {int(POLL_INTERVAL * 1000)});
    </script>
</body>
</html>"""
    return HTMLResponse(content=html)


@APP.get("/reconnects", response_class=HTMLResponse)
async def reconnects_page(request: Request):
    reconnect_attempts = stats.get("reconnect_attempts", [])

    reconnect_items = ""
    if reconnect_attempts:
        for attempt in reconnect_attempts:
            status_badge = "bg-success" if attempt.get("ok") else "bg-danger"
            status_text = "✓ OK" if attempt.get("ok") else "✗ FAILED"
            error_info = f"<div style='color: #ffa9c9; font-size: 0.9rem; margin-top: 8px;'><strong>Ошибка:</strong> {attempt.get('error')}</div>" if attempt.get("error") else ""

            reconnect_items += f"""
            <article class="node-card">
                <div class="node-card-head">
                    <h3>{attempt.get('node_name') or f"Нода #{attempt.get('node_id')}"}</h3>
                    <span class="badge bg-{status_badge}">{status_text}</span>
                </div>
                <div class="node-grid">
                    <div><span>ID</span><strong>{attempt.get('node_id')}</strong></div>
                    <div><span>Статус</span><strong>{attempt.get('status')}</strong></div>
                </div>
                {error_info}
            </article>
            """
    else:
        reconnect_items = "<div class='empty-state'>Попыток переподключения не найдено</div>"

    html = f"""<!doctype html>
<html lang="ru">
<head>
    <meta charset="utf-8">
    <title>Переподключения - MarzBalancer</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        body {{ background: #0b1020; color: #d8e1ff; margin: 0; }}
        .navbar-custom {{ background: #080e1f; border-bottom: 1px solid #27345b; padding: 12px 0; position: sticky; top: 0; z-index: 100; }}
        .navbar-title {{ font-weight: 700; font-size: 1.2rem; margin: 0; color: #d8e1ff; }}
        .nav-buttons {{ display: flex; gap: 8px; align-items: center; }}
        .nav-btn {{ padding: 6px 14px; border: 1px solid #4b6bb0; border-radius: 8px; text-decoration: none; color: #b9c8ef; font-size: 0.95rem; transition: all 0.2s; }}
        .nav-btn:hover {{ background: #1a2847; color: #d8e1ff; border-color: #6b8fd9; }}
        .nav-btn.active {{ background: #2b4a8c; color: #d8e1ff; border-color: #6b8fd9; }}
        .navbar-wrapper {{ max-width: 1280px; margin: 0 auto; padding: 0 20px; display: flex; justify-content: space-between; align-items: center; }}
        .app-wrap {{ max-width: 1280px; margin: 0 auto; padding: 28px 20px 36px; }}
        .hero {{ display:flex; justify-content:space-between; gap:16px; align-items:flex-start; margin-bottom:22px; flex-wrap:wrap; }}
        .hero h1 {{ margin:0; font-size:1.8rem; font-weight:700; }}
        .hero p {{ margin:6px 0 0; color:#9fb0de; }}
        .nodes-grid {{ display:grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap:14px; }}
        .node-card {{ background:#121a30; border:1px solid #2b3d69; border-radius:14px; padding:14px; }}
        .node-card-head {{ display:flex; justify-content:space-between; align-items:center; gap:10px; margin-bottom:10px; }}
        .node-card-head h3 {{ margin:0; font-size:1.05rem; }}
        .node-grid {{ display:grid; grid-template-columns: 1fr 1fr; gap:10px 12px; }}
        .node-grid span {{ display:block; font-size:.8rem; color:#8ea2d9; margin-bottom:1px; }}
        .node-grid strong {{ font-size:.95rem; color:#ecf2ff; }}
        .empty-state {{ grid-column:1/-1; background:#1a233f; border:1px dashed #4b5f92; color:#b9c8ef; border-radius:12px; padding:20px; text-align:center; }}
        .footer-link {{ position:fixed; right:16px; bottom:12px; color:#91a4dc; text-decoration:none; font-size:.85rem; opacity:.8; }}
        .footer-link:hover {{ opacity:1; color:#c7d5ff; }}
        @media (max-width: 700px) {{ .node-grid {{ grid-template-columns: 1fr; }} .nav-buttons {{ flex-direction: column; width: 100%; margin-top: 12px; }} }}
    </style>
</head>
<body>
    <nav class="navbar-custom">
        <div class="navbar-wrapper">
            <h2 class="navbar-title">MarzBalancer</h2>
            <div class="nav-buttons">
                <a href="/" class="nav-btn">Статус нод</a>
                <a href="/reconnects" class="nav-btn active">Переподключения</a>
                <a href="/settings" class="nav-btn">Настройки</a>
            </div>
        </div>
    </nav>
    <main class="app-wrap">
        <section class="hero">
            <div>
                <h1>История переподключений</h1>
                <p>Последние попытки автоматического переподключения офлайн нод</p>
            </div>
        </section>

        <section class="nodes-grid">
            {reconnect_items}
        </section>
    </main>

    <a href="https://github.com/MakarSPB/marz-balancer" target="_blank" rel="noopener noreferrer" class="footer-link">&copy; MakarSPB</a>

    <script>
        setTimeout(() => location.reload(), {int(POLL_INTERVAL * 1000)});
    </script>
</body>
</html>"""
    return HTMLResponse(content=html)


@APP.get("/settings", response_class=HTMLResponse)
async def settings_get(request: Request):
    msg = request.query_params.get("msg", "")
    token_display = TELEGRAM_BOT_TOKEN or "не задан"
    marz_pass_display = "задан" if MARZBAN_ADMIN_PASS else "не задан"

    html = f"""<!doctype html>
<html lang="ru"><head><meta charset="utf-8"><title>Настройки MarzBalancer</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
<style>
    body { background: #0b1020; color: #d8e1ff; margin: 0; }
    .navbar-custom { background: #080e1f; border-bottom: 1px solid #27345b; padding: 12px 0; position: sticky; top: 0; z-index: 100; }
    .navbar-title { font-weight: 700; font-size: 1.2rem; margin: 0; color: #d8e1ff; }
    .nav-buttons { display: flex; gap: 8px; align-items: center; }
    .nav-btn { padding: 6px 14px; border: 1px solid #4b6bb0; border-radius: 8px; text-decoration: none; color: #b9c8ef; font-size: 0.95rem; transition: all 0.2s; }
    .nav-btn:hover { background: #1a2847; color: #d8e1ff; border-color: #6b8fd9; }
    .nav-btn.active { background: #2b4a8c; color: #d8e1ff; border-color: #6b8fd9; }
    .navbar-wrapper { max-width: 1280px; margin: 0 auto; padding: 0 20px; display: flex; justify-content: space-between; align-items: center; }
    .app-wrap { max-width: 1280px; margin: 0 auto; padding: 28px 20px 36px; }
    .hero { margin-bottom:22px; }
    .hero h1 { margin:0; font-size:1.8rem; font-weight:700; }
    .hero p { margin:6px 0 0; color:#9fb0de; }
    .form-section { background: #121a30; border:1px solid #2b3d69; border-radius:14px; padding:20px; margin-bottom:20px; }
    .form-section h5 { color: #d8e1ff; margin-bottom: 16px; font-weight: 700; border-bottom: 1px solid #27345b; padding-bottom: 12px; }
    .form-label { color: #b9c8ef; font-size: 0.95rem; }
    .form-control { background: #0b0f1f; border: 1px solid #27345b; color: #d8e1ff; }
    .form-control:focus { background: #131a30; border-color: #4b6bb0; color: #d8e1ff; box-shadow: 0 0 0 0.2rem rgba(75, 107, 176, 0.25); }
    .form-check-input { background: #0b0f1f; border: 1px solid #27345b; }
    .form-check-input:checked { background: #2b5a9c; border-color: #4b6bb0; }
    .form-check-label { color: #b9c8ef; margin: 0; }
    .form-text { color: #8ea2d9; }
    .btn-primary { background: #2b5a9c; border: 1px solid #4b6bb0; color: #d8e1ff; }
    .btn-primary:hover { background: #3a70b8; border-color: #6b8fd9; }
    .btn-outline-success { border: 1px solid #27a745; color: #5fcd7d; }
    .btn-outline-success:hover { background: #27a745; color: #d8e1ff; border-color: #27a745; }
    .alert-success { background: #1a3a1f; border: 1px solid #2d5a3d; color: #7dd47d; }
    .footer-link { position:fixed; right:16px; bottom:12px; color:#91a4dc; text-decoration:none; font-size:.85rem; opacity:.8; }
    .footer-link:hover { opacity:1; color:#c7d5ff; }
    @media (max-width: 700px) { .nav-buttons { flex-direction: column; width: 100%; margin-top: 12px; } }
</style>
<nav class="navbar-custom">
    <div class="navbar-wrapper">
        <h2 class="navbar-title">MarzBalancer</h2>
        <div class="nav-buttons">
            <a href="/" class="nav-btn">Статус нод</a>
            <a href="/reconnects" class="nav-btn">Переподключения</a>
            <a href="/settings" class="nav-btn active">Настройки</a>
        </div>
    </div>
</nav>
<main class="app-wrap">
    <section class="hero">
        <div>
            <h1>Настройки</h1>
            <p>Конфигурация Marzban и Telegram уведомлений</p>
        </div>
    </section>
    {f'<div class="alert alert-success" role="alert">{msg}</div>' if msg else ''}
    <form method="post" action="/settings">
        <div class="form-section">
            <h5>Параметры Marzban</h5>
            <div class="mb-3">
                <label class="form-label">MARZBAN_URL</label>
                <input name="MARZBAN_URL" class="form-control" value="{MARZBAN_URL or ''}" placeholder="https://marzban.example.com">
            </div>
            <div class="mb-3">
                <label class="form-label">MARZBAN_ADMIN_USER</label>
                <input name="MARZBAN_ADMIN_USER" class="form-control" value="{MARZBAN_ADMIN_USER or ''}" placeholder="admin">
            </div>
            <div class="mb-3">
                <label class="form-label">MARZBAN_ADMIN_PASS</label>
                <input name="MARZBAN_ADMIN_PASS" type="password" class="form-control" placeholder="введите новый пароль или оставьте пустым">
                <div class="form-text">Текущий: {marz_pass_display}</div>
            </div>
            <div class="form-check mb-3">
                <input class="form-check-input" type="checkbox" name="IP_AGENT_ENABLED" id="ipAgentEnabled" {"checked" if IP_AGENT_ENABLED else ""}>
                <label class="form-check-label" for="ipAgentEnabled">Проверять IP-агент нод</label>
            </div>
        </div>

        <div class="form-section">
            <h5>Параметры Telegram</h5>
            <div class="mb-3">
                <label class="form-label">TELEGRAM_PROXY_URL</label>
                <input name="TELEGRAM_PROXY_URL" class="form-control" value="{TELEGRAM_PROXY_URL or ''}" placeholder="https://proxy.example">
            </div>
            <div class="mb-3">
                <label class="form-label">TELEGRAM_BOT_TOKEN</label>
                <input name="TELEGRAM_BOT_TOKEN" type="text" class="form-control" value="{TELEGRAM_BOT_TOKEN or ''}" placeholder="токен бота">
                <div class="form-text">Текущий: {token_display}</div>
            </div>
            <div class="mb-3">
                <label class="form-label">TELEGRAM_CHAT_ID</label>
                <input name="TELEGRAM_CHAT_ID" class="form-control" value="{TELEGRAM_CHAT_ID or ''}" placeholder="чат_ID">
            </div>
        </div>

        <div class="form-section">
            <h5>Фильтр статусов для уведомлений</h5>
            <div class="form-check mb-2">
                <input class="form-check-input" type="checkbox" name="TELEGRAM_NOTIFY_ON_ONLINE" id="notifyOnline" {"checked" if TELEGRAM_NOTIFY_ON_ONLINE else ""}>
                <label class="form-check-label" for="notifyOnline">Уведомлять при подключении ноды (online)</label>
            </div>
            <div class="form-check mb-2">
                <input class="form-check-input" type="checkbox" name="TELEGRAM_NOTIFY_ON_OFFLINE" id="notifyOffline" {"checked" if TELEGRAM_NOTIFY_ON_OFFLINE else ""}>
                <label class="form-check-label" for="notifyOffline">Уведомлять при отключении ноды (offline)</label>
            </div>
            <div class="form-check">
                <input class="form-check-input" type="checkbox" name="TELEGRAM_NOTIFY_ON_CONNECTING" id="notifyConnecting" {"checked" if TELEGRAM_NOTIFY_ON_CONNECTING else ""}>
                <label class="form-check-label" for="notifyConnecting">Уведомлять о переподключении ноды (connecting)</label>
            </div>
        </div>

        <button type="submit" class="btn btn-primary">Сохранить</button>
    </form>

        <form method="post" action="/settings/test" class="mt-3">
          <button class="btn btn-outline-success">Отправить тестовое уведомление</button>
        </form>

        <a href="https://github.com/MakarSPB/marz-balancer" target="_blank" rel="noopener noreferrer" class="footer-link">&copy; MakarSPB</a>
    </body>
    </html>"""
    return HTMLResponse(content=html)


@APP.post("/settings")
async def settings_post(request: Request):
    global MARZBAN_URL, MARZBAN_ADMIN_USER, MARZBAN_ADMIN_PASS
    global IP_AGENT_ENABLED
    global TELEGRAM_PROXY_URL, TELEGRAM_API_BASE, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
    global TELEGRAM_NOTIFY_ON_ONLINE, TELEGRAM_NOTIFY_ON_OFFLINE, TELEGRAM_NOTIFY_ON_CONNECTING
    raw_body = (await request.body()).decode("utf-8", errors="ignore")
    form = parse_qs(raw_body, keep_blank_values=True)

    marzban_url = (form.get("MARZBAN_URL", [""])[0] or "").strip().rstrip("/")
    marzban_user = (form.get("MARZBAN_ADMIN_USER", [""])[0] or "").strip()
    marzban_pass = (form.get("MARZBAN_ADMIN_PASS", [""])[0] or "").strip()
    ip_agent_enabled = "IP_AGENT_ENABLED" in form

    proxy = (form.get("TELEGRAM_PROXY_URL", [""])[0] or "").strip().rstrip("/")
    bot = (form.get("TELEGRAM_BOT_TOKEN", [""])[0] or "").strip()
    chat = (form.get("TELEGRAM_CHAT_ID", [""])[0] or "").strip()

    notify_online = "TELEGRAM_NOTIFY_ON_ONLINE" in form
    notify_offline = "TELEGRAM_NOTIFY_ON_OFFLINE" in form
    notify_connecting = "TELEGRAM_NOTIFY_ON_CONNECTING" in form

    updates: Dict[str, str] = {}
    if marzban_url != MARZBAN_URL:
        updates["MARZBAN_URL"] = marzban_url
    if marzban_user != MARZBAN_ADMIN_USER:
        updates["MARZBAN_ADMIN_USER"] = marzban_user
    if marzban_pass:
        updates["MARZBAN_ADMIN_PASS"] = marzban_pass

    if ip_agent_enabled != IP_AGENT_ENABLED:
        updates["IP_AGENT_ENABLED"] = "1" if ip_agent_enabled else "0"

    if proxy != TELEGRAM_PROXY_URL:
        updates["TELEGRAM_PROXY_URL"] = proxy
    if bot:
        updates["TELEGRAM_BOT_TOKEN"] = bot
    if chat != TELEGRAM_CHAT_ID:
        updates["TELEGRAM_CHAT_ID"] = chat

    if notify_online != TELEGRAM_NOTIFY_ON_ONLINE:
        updates["TELEGRAM_NOTIFY_ON_ONLINE"] = "1" if notify_online else "0"
    if notify_offline != TELEGRAM_NOTIFY_ON_OFFLINE:
        updates["TELEGRAM_NOTIFY_ON_OFFLINE"] = "1" if notify_offline else "0"
    if notify_connecting != TELEGRAM_NOTIFY_ON_CONNECTING:
        updates["TELEGRAM_NOTIFY_ON_CONNECTING"] = "1" if notify_connecting else "0"

    # apply updates in-memory
    if "MARZBAN_URL" in updates:
        MARZBAN_URL = updates["MARZBAN_URL"]
        _token_cache["token"] = None
        _token_cache["fetched_at"] = 0
    if "MARZBAN_ADMIN_USER" in updates:
        MARZBAN_ADMIN_USER = updates["MARZBAN_ADMIN_USER"]
        _token_cache["token"] = None
        _token_cache["fetched_at"] = 0
    if "MARZBAN_ADMIN_PASS" in updates:
        MARZBAN_ADMIN_PASS = updates["MARZBAN_ADMIN_PASS"]
        _token_cache["token"] = None
        _token_cache["fetched_at"] = 0

    if "IP_AGENT_ENABLED" in updates:
        IP_AGENT_ENABLED = _to_bool(updates["IP_AGENT_ENABLED"], default=True)

    if "TELEGRAM_PROXY_URL" in updates:
        TELEGRAM_PROXY_URL = updates["TELEGRAM_PROXY_URL"]
        TELEGRAM_API_BASE = TELEGRAM_PROXY_URL or "https://api.telegram.org"
        stats["telegram_api_base"] = TELEGRAM_API_BASE
    if "TELEGRAM_BOT_TOKEN" in updates:
        TELEGRAM_BOT_TOKEN = updates["TELEGRAM_BOT_TOKEN"]
    if "TELEGRAM_CHAT_ID" in updates:
        TELEGRAM_CHAT_ID = updates["TELEGRAM_CHAT_ID"]

    if "TELEGRAM_NOTIFY_ON_ONLINE" in updates:
        TELEGRAM_NOTIFY_ON_ONLINE = _to_bool(updates["TELEGRAM_NOTIFY_ON_ONLINE"], default=True)
    if "TELEGRAM_NOTIFY_ON_OFFLINE" in updates:
        TELEGRAM_NOTIFY_ON_OFFLINE = _to_bool(updates["TELEGRAM_NOTIFY_ON_OFFLINE"], default=True)
    if "TELEGRAM_NOTIFY_ON_CONNECTING" in updates:
        TELEGRAM_NOTIFY_ON_CONNECTING = _to_bool(updates["TELEGRAM_NOTIFY_ON_CONNECTING"], default=False)

    # persist to sqlite
    if updates:
        ok = _save_settings_db(updates)
        msg = "Настройки сохранены" if ok else "Настройки применены (не удалось записать в sqlite)"
    else:
        msg = "Новых настроек не обнаружено"

    return RedirectResponse(url=f"/settings?msg={msg}", status_code=303)


@APP.post("/settings/test")
async def settings_test_notification():
    async with aiohttp.ClientSession() as session:
        sent = await send_telegram_message(
            session,
            "привет! я монитор нод marz. Я подключился",
            force=True,
        )
    msg = "Тестовое уведомление отправлено" if sent else "Не удалось отправить тестовое уведомление"
    return RedirectResponse(url=f"/settings?msg={msg}", status_code=303)
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("marz_balancer:APP", host="0.0.0.0", port=APP_PORT, reload=True)
