"""Database Backup: JSON (gzip) ফাইল অ্যাডমিনের Telegram-এ পাঠানো হয়, যা Cloud-এর বাইরেও নিরাপদ কপি রাখে।"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Iterable
from zoneinfo import ZoneInfo

from aiogram import Bot
from aiogram.types import BufferedInputFile

from app import database as db
from app.config import get_settings
from app.services.notifications import notify_admins
from app.services.ui import fmt_dt

log = logging.getLogger(__name__)


async def run_backup(bot: Bot, recipients: Iterable[int]) -> bool:
    recipients = list(recipients)
    if not recipients:
        log.warning("Backup পাঠানোর জন্য কোনো অ্যাডমিন নেই (ADMIN_IDS ফাঁকা)")
        return False
    data = await db.export_backup()
    tz = ZoneInfo(get_settings().timezone)
    name = f"backup_{datetime.now(tz).strftime('%Y%m%d_%H%M%S')}.json.gz"
    delivered = 0
    for uid in recipients:
        try:
            await bot.send_document(
                uid,
                BufferedInputFile(data, filename=name),
                caption=f"💾 Database Backup\n🕒 {fmt_dt(datetime.now(timezone.utc))}",
            )
            delivered += 1
        except Exception:
            log.exception("Backup পাঠানো ব্যর্থ: %s", uid)
    if not delivered:
        return False
    await db.set_setting("last_backup_at", datetime.now(timezone.utc).isoformat())
    await notify_admins(bot, "💾 <b>Database Backup সম্পন্ন হয়েছে।</b>")
    return True


async def backup_loop(bot: Bot) -> None:
    interval = get_settings().backup_interval_hours * 3600
    while True:
        try:
            wait = 120.0
            last = await db.get_setting("last_backup_at")
            if last:
                elapsed = (datetime.now(timezone.utc) - datetime.fromisoformat(last)).total_seconds()
                wait = max(120.0, interval - elapsed)
            await asyncio.sleep(wait)
            ok = await run_backup(bot, get_settings().admin_ids)
            if not ok:
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("স্বয়ংক্রিয় Backup ব্যর্থ")
            await asyncio.sleep(600)
