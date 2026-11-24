from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from functools import lru_cache
from zoneinfo import ZoneInfo

# শুধুমাত্র Facebook-এর অফিসিয়াল পেজ
FACEBOOK_URL = "https://www.facebook.com/"
FACEBOOK_SIGNUP_URL = "https://www.facebook.com/r.php"
FACEBOOK_RECOVERY_URL = "https://www.facebook.com/login/identify"


def _parse_ids(raw: str) -> frozenset[int]:
    ids: set[int] = set()
    for part in raw.replace(";", ",").split(","):
        part = part.strip()
        if part.isdigit():
            ids.add(int(part))
    return frozenset(ids)


def _int(name: str, default: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
        return value if value > 0 else default
    except ValueError:
        return default


@dataclass(frozen=True)
class Settings:
    bot_token: str
    admin_ids: frozenset[int]
    database_url: str
    timezone: str
    log_level: str
    webhook_url: str
    webhook_path: str
    webhook_secret: str
    port: int
    proxy_url: str
    backup_interval_hours: int
    rate_limit_count: int
    rate_limit_window: int

    @property
    def use_webhook(self) -> bool:
        return bool(self.webhook_url)


@lru_cache
def get_settings() -> Settings:
    token = os.getenv("BOT_TOKEN", "").strip()
    database_url = os.getenv("DATABASE_URL", "").strip()
    missing = [n for n, v in (("BOT_TOKEN", token), ("DATABASE_URL", database_url)) if not v]
    if missing:
        raise RuntimeError("অনুপস্থিত Environment Variable: " + ", ".join(missing))

    tz = os.getenv("TIMEZONE", "Asia/Dhaka").strip() or "Asia/Dhaka"
    try:
        ZoneInfo(tz)
    except Exception:
        tz = "Asia/Dhaka"

    digest = hashlib.sha256(token.encode()).hexdigest()
    return Settings(
        bot_token=token,
        admin_ids=_parse_ids(os.getenv("ADMIN_IDS", "")),
        database_url=database_url,
        timezone=tz,
        log_level=os.getenv("LOG_LEVEL", "INFO").strip() or "INFO",
        webhook_url=os.getenv("WEBHOOK_URL", "").strip().rstrip("/"),
        webhook_path=f"/webhook/{digest[:24]}",  # Token URL-এ প্রকাশ পায় না
        webhook_secret=digest[24:56],
        port=_int("PORT", 8080),
        proxy_url=os.getenv("PROXY_URL", "").strip(),
        backup_interval_hours=_int("BACKUP_INTERVAL_HOURS", 24),
        rate_limit_count=_int("RATE_LIMIT_COUNT", 10),
        rate_limit_window=_int("RATE_LIMIT_WINDOW", 10),
    )
