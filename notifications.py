"""Admin ও User Notification।"""
from __future__ import annotations

import asyncio
import logging
import time

from aiogram import Bot
from aiogram.exceptions import TelegramForbiddenError, TelegramRetryAfter, TelegramAPIError

from app import database as db
from app.config import get_settings
from app.services.ui import esc

log = logging.getLogger(__name__)

_throttle: dict[str, float] = {}


async def admin_targets() -> set[int]:
    ids = set(get_settings().admin_ids)
    try:
        ids.update(r["telegram_id"] for r in await db.list_db_admins())
    except Exception:
        pass  # Database বন্ধ থাকলেও ENV অ্যাডমিনদের কাছে যাবে
    return ids


async def _send(bot: Bot, uid: int, text: str) -> bool:
    try:
        await bot.send_message(uid, text)
        return True
    except TelegramRetryAfter as exc:
        await asyncio.sleep(exc.retry_after)
        try:
            await bot.send_message(uid, text)
            return True
        except TelegramAPIError:
            return False
    except TelegramForbiddenError:
        return False
    except Exception:
        log.warning("মেসেজ পাঠানো যায়নি: %s", uid)
        return False


async def notify_admins(bot: Bot, text: str, force: bool = False) -> None:
    if not force:
        try:
            if await db.get_setting("admin_alerts", "on") != "on":
                return
        except Exception:
            pass
    for uid in await admin_targets():
        await _send(bot, uid, text)


async def notify_admins_throttled(bot: Bot, key: str, text: str, seconds: int = 600) -> None:
    now = time.monotonic()
    if now - _throttle.get(key, 0) < seconds:
        return
    _throttle[key] = now
    await notify_admins(bot, text)


async def notify_user(bot: Bot, uid: int, text: str) -> bool:
    return await _send(bot, uid, text)


async def broadcast(bot: Bot, text: str, sent_by: int) -> tuple[int, int]:
    body = "📨 <b>বিজ্ঞপ্তি</b>\n\n" + esc(text)
    sent = failed = 0
    for uid in await db.broadcast_targets():
        if await _send(bot, uid, body):
            sent += 1
        else:
            failed += 1
        await asyncio.sleep(0.05)  # Telegram rate limit-এর মধ্যে থাকতে
    await db.record_notification("all_users", text[:1000], sent_by, sent, failed)
    return sent, failed
