"""Cloud Health Monitoring ও অবস্থা-সংক্রান্ত টেক্সট।"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone

import psutil
from aiogram import Bot

from app import database as db
from app.services.logging import error_count
from app.services.notifications import notify_admins
from app.services.ui import STATUS_LABELS, fmt_dt, fmt_uptime

log = logging.getLogger(__name__)

_START = time.monotonic()
psutil.cpu_percent(interval=None)  # প্রথম কলটি বেসলাইন


def uptime_seconds() -> float:
    return time.monotonic() - _START


async def collect(bot: Bot) -> dict:
    db_ms = await db.ping()
    tg_ms = None
    try:
        start = time.perf_counter()
        await asyncio.wait_for(bot.get_me(), timeout=10)
        tg_ms = (time.perf_counter() - start) * 1000
    except Exception:
        pass

    last_backup = None
    if db_ms is not None:
        try:
            raw = await db.get_setting("last_backup_at")
            last_backup = datetime.fromisoformat(raw) if raw else None
        except Exception:
            pass

    proc = psutil.Process()
    return {
        "db_ok": db_ms is not None,
        "db_ms": db_ms,
        "telegram_ok": tg_ms is not None,
        "telegram_ms": tg_ms,
        "cpu": psutil.cpu_percent(interval=None),
        "memory": psutil.virtual_memory().percent,
        "proc_mb": proc.memory_info().rss / 1048576,
        "uptime": uptime_seconds(),
        "errors": error_count(),
        "last_backup": last_backup,
    }


def _flag(ok: bool, ms: float | None = None) -> str:
    if not ok:
        return "❌ সমস্যা"
    return f"✅ ঠিক আছে ({ms:.0f} ms)" if ms is not None else "✅ ঠিক আছে"


def format_health(h: dict) -> str:
    backup = fmt_dt(h["last_backup"]) if h["last_backup"] else "এখনো হয়নি"
    return (
        "☁️ <b>Cloud Server স্বাস্থ্য</b>\n\n"
        f"🤖 Bot ও Telegram API: {_flag(h['telegram_ok'], h['telegram_ms'])}\n"
        f"🗄️ Database: {_flag(h['db_ok'], h['db_ms'])}\n"
        f"🖥️ CPU: {h['cpu']:.0f}%\n"
        f"🧠 Memory: {h['memory']:.0f}% (Bot: {h['proc_mb']:.0f} MB)\n"
        f"⏱️ Uptime: {fmt_uptime(h['uptime'])}\n"
        f"⚠️ Application Error (এই রানে): {h['errors']}\n"
        f"💾 শেষ সফল Backup: {backup}"
    )


def format_user_server_status(h: dict) -> str:
    return (
        "📊 <b>Server অবস্থা</b>\n\n"
        f"🤖 Bot ও Telegram: {_flag(h['telegram_ok'])}\n"
        f"🗄️ Database: {_flag(h['db_ok'])}\n"
        f"⏱️ চালু আছে: {fmt_uptime(h['uptime'])}"
    )


async def session_status_text(kind: str, uid: int) -> str:
    title = "🆕 নতুন অ্যাকাউন্ট যাচাই" if kind == "verification" else "🔑 অ্যাকাউন্ট পুনরুদ্ধার"
    row = await db.latest_session(kind, uid)
    if not row:
        return f"{title}\n\nএখনো কোনো প্রক্রিয়া শুরু করা হয়নি।"
    return (
        f"{title}\n\n"
        f"অবস্থা: {STATUS_LABELS.get(row['status'], row['status'])}\n"
        f"শুরু: {fmt_dt(row['started_at'])}\n"
        f"সর্বশেষ আপডেট: {fmt_dt(row['updated_at'])}"
    )


async def overall_status_text(uid: int) -> str:
    parts = ["📊 <b>বর্তমান অবস্থা</b>"]
    for kind in ("verification", "recovery"):
        parts.append(await session_status_text(kind, uid))
    return "\n\n".join(parts)


async def _transition(bot: Bot, state: dict, key: str, ok: bool, bad: str, good: str) -> None:
    if ok == state[key]:
        return
    state[key] = ok
    await notify_admins(bot, good if ok else bad)


async def monitor_loop(bot: Bot, interval: int = 300) -> None:
    state = {"db": True, "tg": True, "res": True}
    ticks = 0
    while True:
        try:
            await asyncio.sleep(interval)
            h = await collect(bot)
            if h["db_ok"]:
                await db.record_health(h)
            ticks += 1
            await _transition(
                bot, state, "db", h["db_ok"],
                "🚨 <b>Database Connection Error</b>\nDatabase-এর সঙ্গে সংযোগ পাওয়া যাচ্ছে না।",
                "✅ Database সংযোগ আবার স্বাভাবিক হয়েছে।",
            )
            await _transition(
                bot, state, "tg", h["telegram_ok"],
                "🚨 <b>Cloud Server Error</b>\nTelegram API সংযোগে সমস্যা।",
                "✅ Telegram API সংযোগ আবার স্বাভাবিক হয়েছে।",
            )
            await _transition(
                bot, state, "res", h["cpu"] < 90 and h["memory"] < 90,
                f"🚨 <b>Cloud Server Error</b>\nResource ব্যবহার অতিরিক্ত: CPU {h['cpu']:.0f}%, Memory {h['memory']:.0f}%।",
                "✅ Server Resource ব্যবহার আবার স্বাভাবিক।",
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Health monitor-এ ত্রুটি")
