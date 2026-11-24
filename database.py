"""PostgreSQL ডেটা অ্যাক্সেস লেয়ার (asyncpg)। কোনো Password/OTP সংরক্ষণ করা হয় না।"""
from __future__ import annotations

import asyncio
import gzip
import json
import logging
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import asyncpg

from app.config import get_settings
from database.migrations import run_migrations

log = logging.getLogger(__name__)

_pool: asyncpg.Pool | None = None

SESSION_TABLES = {"verification": "verification_sessions", "recovery": "recovery_sessions"}
BACKUP_TABLES = (
    "users",
    "admins",
    "verification_sessions",
    "recovery_sessions",
    "bot_settings",
    "notifications",
)
COUNT_TABLES = BACKUP_TABLES + ("activity_logs", "system_health")


def pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("Database সংযুক্ত নেই")
    return _pool


async def connect(dsn: str) -> None:
    global _pool
    if _pool is not None:
        return
    new_pool = await asyncpg.create_pool(dsn, min_size=1, max_size=8, command_timeout=30)
    try:
        await run_migrations(new_pool)
    except Exception:
        await new_pool.close()
        raise
    _pool = new_pool
    log.info("Database সংযুক্ত হয়েছে")


async def close() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


async def ping() -> float | None:
    """সফল হলে মিলিসেকেন্ডে latency, ব্যর্থ হলে None।"""
    try:
        start = time.perf_counter()
        await asyncio.wait_for(pool().fetchval("SELECT 1"), timeout=5)
        return (time.perf_counter() - start) * 1000
    except Exception:
        return None


# ------------------------------------------------------------------ users
async def upsert_user(tid: int, username: str | None, full_name: str | None):
    return await pool().fetchrow(
        """
        INSERT INTO users (telegram_id, username, full_name) VALUES ($1, $2, $3)
        ON CONFLICT (telegram_id) DO UPDATE
            SET username = EXCLUDED.username,
                full_name = EXCLUDED.full_name,
                last_seen = now()
        RETURNING *
        """,
        tid,
        username,
        full_name,
    )


async def set_notifications(tid: int, enabled: bool) -> None:
    await pool().execute("UPDATE users SET notifications_enabled = $2 WHERE telegram_id = $1", tid, enabled)


async def set_banned(tid: int, banned: bool) -> bool:
    res = await pool().execute("UPDATE users SET is_banned = $2 WHERE telegram_id = $1", tid, banned)
    return not res.endswith(" 0")


async def list_recent_users(limit: int = 15):
    return await pool().fetch(
        "SELECT telegram_id, username, full_name, registered_at, is_banned "
        "FROM users ORDER BY registered_at DESC LIMIT $1",
        limit,
    )


async def broadcast_targets() -> list[int]:
    rows = await pool().fetch(
        "SELECT telegram_id FROM users WHERE NOT is_banned AND notifications_enabled"
    )
    return [r["telegram_id"] for r in rows]


# --------------------------------------------------------------- sessions
async def create_session(kind: str, uid: int) -> int:
    table = SESSION_TABLES[kind]
    async with pool().acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                f"UPDATE {table} SET status='cancelled', updated_at=now(), finished_at=now() "
                "WHERE user_id=$1 AND status='started'",
                uid,
            )
            return await conn.fetchval(f"INSERT INTO {table} (user_id) VALUES ($1) RETURNING id", uid)


async def active_session(kind: str, uid: int):
    table = SESSION_TABLES[kind]
    return await pool().fetchrow(
        f"SELECT * FROM {table} WHERE user_id=$1 AND status='started' ORDER BY id DESC LIMIT 1", uid
    )


async def finish_session(kind: str, uid: int, status: str) -> bool:
    table = SESSION_TABLES[kind]
    res = await pool().execute(
        f"UPDATE {table} SET status=$2, updated_at=now(), finished_at=now() "
        "WHERE user_id=$1 AND status='started'",
        uid,
        status,
    )
    return not res.endswith(" 0")


async def latest_session(kind: str, uid: int):
    table = SESSION_TABLES[kind]
    return await pool().fetchrow(f"SELECT * FROM {table} WHERE user_id=$1 ORDER BY id DESC LIMIT 1", uid)


async def session_history(kind: str, uid: int, limit: int = 10):
    table = SESSION_TABLES[kind]
    return await pool().fetch(
        f"SELECT status, started_at FROM {table} WHERE user_id=$1 ORDER BY id DESC LIMIT $2", uid, limit
    )


async def cancel_all_active(uid: int) -> int:
    total = 0
    for table in SESSION_TABLES.values():
        res = await pool().execute(
            f"UPDATE {table} SET status='cancelled', updated_at=now(), finished_at=now() "
            "WHERE user_id=$1 AND status='started'",
            uid,
        )
        total += int(res.split()[-1])
    return total


# --------------------------------------------------------------- activity
async def log_activity(uid: int, action: str, details: str | None = None) -> None:
    """শুধু Non-sensitive তথ্য। ব্যর্থ হলেও মূল কাজ থামবে না।"""
    try:
        await pool().execute(
            "INSERT INTO activity_logs (user_id, action, details) VALUES ($1, $2, $3)", uid, action, details
        )
    except Exception:
        log.exception("Activity log সংরক্ষণ ব্যর্থ")


async def user_activity(uid: int, today: bool, limit: int = 20):
    tz = ZoneInfo(get_settings().timezone)
    start = datetime.now(tz).replace(hour=0, minute=0, second=0, microsecond=0)
    op = ">=" if today else "<"
    return await pool().fetch(
        f"SELECT action, created_at FROM activity_logs WHERE user_id=$1 AND created_at {op} $2 "
        f"ORDER BY id DESC LIMIT {int(limit)}",
        uid,
        start,
    )


async def count_user_activity(uid: int) -> int:
    return await pool().fetchval("SELECT count(*) FROM activity_logs WHERE user_id=$1", uid)


async def clear_history(uid: int) -> None:
    async with pool().acquire() as conn:
        async with conn.transaction():
            await conn.execute("DELETE FROM verification_sessions WHERE user_id=$1", uid)
            await conn.execute("DELETE FROM recovery_sessions WHERE user_id=$1", uid)
            await conn.execute("DELETE FROM activity_logs WHERE user_id=$1", uid)


async def recent_activity(limit: int = 15):
    return await pool().fetch(
        "SELECT user_id, action, created_at FROM activity_logs ORDER BY id DESC LIMIT $1", limit
    )


# --------------------------------------------------------- settings/admins
async def get_setting(key: str, default: str | None = None) -> str | None:
    value = await pool().fetchval("SELECT value FROM bot_settings WHERE key=$1", key)
    return value if value is not None else default


async def set_setting(key: str, value: str) -> None:
    await pool().execute(
        "INSERT INTO bot_settings (key, value) VALUES ($1, $2) "
        "ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_at=now()",
        key,
        value,
    )


async def list_db_admins():
    return await pool().fetch("SELECT telegram_id, added_at FROM admins ORDER BY added_at")


async def is_db_admin(tid: int) -> bool:
    return bool(await pool().fetchval("SELECT 1 FROM admins WHERE telegram_id=$1", tid))


async def add_db_admin(tid: int, by: int) -> None:
    await pool().execute(
        "INSERT INTO admins (telegram_id, added_by) VALUES ($1, $2) ON CONFLICT DO NOTHING", tid, by
    )


async def remove_db_admin(tid: int) -> bool:
    res = await pool().execute("DELETE FROM admins WHERE telegram_id=$1", tid)
    return not res.endswith(" 0")


# ------------------------------------------------- notifications / health
async def record_notification(audience: str, message: str, sent_by: int, sent: int, failed: int) -> None:
    await pool().execute(
        "INSERT INTO notifications (audience, message, sent_by, sent_count, failed_count) "
        "VALUES ($1, $2, $3, $4, $5)",
        audience,
        message,
        sent_by,
        sent,
        failed,
    )


async def record_health(h: dict) -> None:
    await pool().execute(
        "INSERT INTO system_health (db_ok, telegram_ok, cpu_percent, memory_percent, uptime_seconds, error_count) "
        "VALUES ($1, $2, $3, $4, $5, $6)",
        h["db_ok"],
        h["telegram_ok"],
        h["cpu"],
        h["memory"],
        int(h["uptime"]),
        h["errors"],
    )
    await pool().execute("DELETE FROM system_health WHERE checked_at < now() - interval '7 days'")


# --------------------------------------------------------------- stats/db
async def stats():
    return await pool().fetchrow(
        """
        SELECT
          (SELECT count(*) FROM users) AS users,
          (SELECT count(*) FROM users WHERE registered_at > now() - interval '24 hours') AS new24,
          (SELECT count(*) FROM users WHERE last_seen > now() - interval '24 hours') AS active24,
          (SELECT count(*) FROM users WHERE is_banned) AS banned,
          (SELECT count(*) FROM verification_sessions) AS ver_total,
          (SELECT count(*) FROM verification_sessions WHERE status='completed') AS ver_done,
          (SELECT count(*) FROM recovery_sessions) AS rec_total,
          (SELECT count(*) FROM recovery_sessions WHERE status='completed') AS rec_done
        """
    )


async def count_users() -> int:
    return await pool().fetchval("SELECT count(*) FROM users")


async def db_info() -> dict:
    size = await pool().fetchval("SELECT pg_size_pretty(pg_database_size(current_database()))")
    version = await pool().fetchval("SHOW server_version")
    counts = {}
    for table in COUNT_TABLES:
        counts[table] = await pool().fetchval(f"SELECT count(*) FROM {table}")
    return {"size": size, "version": version, "counts": counts}


async def export_backup() -> bytes:
    """সব Non-sensitive টেবিলের JSON (gzip) ব্যাকআপ।"""
    data: dict = {"created_at": datetime.now(timezone.utc).isoformat()}
    async with pool().acquire() as conn:
        for table in BACKUP_TABLES:
            rows = await conn.fetch(f"SELECT * FROM {table}")
            data[table] = [dict(r) for r in rows]
        rows = await conn.fetch("SELECT * FROM activity_logs ORDER BY id DESC LIMIT 20000")
        data["activity_logs"] = [dict(r) for r in rows]
    return gzip.compress(json.dumps(data, default=str, ensure_ascii=False).encode("utf-8"))
