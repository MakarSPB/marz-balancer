import os
from dotenv import load_dotenv

load_dotenv()

MARZBAN_URL = os.getenv("MARZBAN_URL", "").rstrip("/")
MARZBAN_ADMIN_USER = os.getenv("MARZBAN_ADMIN_USER", "")
MARZBAN_ADMIN_PASS = os.getenv("MARZBAN_ADMIN_PASS", "")
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "5"))
APP_PORT = int(os.getenv("APP_PORT", "8023"))
IP_AGENT_PORT = os.getenv("IP_AGENT_PORT", "").strip()
IP_AGENT_SCHEME = os.getenv("IP_AGENT_SCHEME", "http").strip()
MONITOR_PORT = int(os.getenv("MONITOR_PORT", "8443"))

TELEGRAM_ENABLED = os.getenv("TELEGRAM_ENABLED", "false").lower() in ("true", "1", "yes")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
NODE_REMINDER_INTERVAL = int(os.getenv("NODE_REMINDER_INTERVAL", "10"))  # минуты

# Авто‑переподключение
def _int_env(name: str, default: int) -> int:
    try:
        v = int(os.getenv(name, str(default)))
        return v
    except Exception:
        return default

NODE_RECONNECT_THRESHOLD = _int_env("NODE_RECONNECT_THRESHOLD", 5)   # 0 => отключено
NODE_RECONNECT_COOLDOWN = _int_env("NODE_RECONNECT_COOLDOWN", 30)    # сек

if NODE_RECONNECT_THRESHOLD < 0:
    NODE_RECONNECT_THRESHOLD = 0
if NODE_RECONNECT_COOLDOWN < 1:
    NODE_RECONNECT_COOLDOWN = 30

NODE_CANDIDATE_PATHS = [
    "/connections",
    "/clients",
    "/status",
]

stats = {
    "nodes": [],
    "last_update": None,
    "error": None,
    "system": None,
    "nodes_usage": None,
    "users_usage": None,
    "port_8443": {"unique_clients": 0, "clients": []},
}

_token_cache = {"token": None, "fetched_at": 0, "ttl": 300}