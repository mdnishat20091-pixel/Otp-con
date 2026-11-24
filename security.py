"""নিরাপত্তা: Authorization, Rate Limiting, Input Validation, Ban/Maintenance চেক।"""
from __future__ import annotations

import logging
import re
import time
from collections import defaultdict, deque

from aiogram import BaseMiddleware
from aiogram.exceptions import TelegramAPIError
from aiogram.types import CallbackQuery, Message

from app import database as db
from app.config import get_settings
from app.services.notifications import notify_admins
from app.services.ui import esc

log = logging.getLogger(__name__)

OTP_LIKE_PATTERN = r"^\s*\d{4,8}\s*$"
_ID_RE = re.compile(r"^\d{5,15}$")

RATE_MSG = "⏳ অনুগ্রহ করে একটু ধীরে চলুন। কিছুক্ষণ পর আবার চেষ্টা করুন।"
BANNED_MSG = "🚫 এই Bot ব্যবহারের অনুমতি আপনার নেই।"
MAINT_MSG = "🔧 Bot এখন রক্ষণাবেক্ষণের কাজে আছে। কিছুক্ষণ পর আবার চেষ্টা করুন।"
DB_ERR_MSG = "⚠️ সাময়িক সেবা সমস্যা হচ্ছে। অনুগ্রহ করে কিছুক্ষণ পর আবার চেষ্টা করুন।"
ADMIN_ONLY_MSG = "⛔ এই অংশ শুধুমাত্র অনুমোদিত অ্যাডমিনদের জন্য।"


def parse_telegram_id(text: str | None) -> int | None:
    if text is None:
        return None
    text = text.strip()
    return int(text) if _ID_RE.match(text) else None


def is_owner(uid: int) -> bool:
    """ENV-এর ADMIN_IDS-ভুক্ত প্রধান অ্যাডমিন।"""
    return uid in get_settings().admin_ids


async def is_admin(uid: int) -> bool:
    if is_owner(uid):
        return True
    try:
        return await db.is_db_admin(uid)
    except Exception:
        return False


class RateLimiter:
    def __init__(self, limit: int, window: int) -> None:
        self.limit = limit
        self.window = window
        self.hits: dict[int, deque[float]] = defaultdict(deque)
        self.warned: dict[int, float] = {}

    def check(self, uid: int) -> str:
        now = time.monotonic()
        queue = self.hits[uid]
        while queue and now - queue[0] > self.window:
            queue.popleft()
        if len(queue) >= self.limit:
            if now - self.warned.get(uid, 0.0) > self.window:
                self.warned[uid] = now
                return "warn"
            return "drop"
        queue.append(now)
        if len(self.hits) > 10000:
            self._gc(now)
        return "ok"

    def _gc(self, now: float) -> None:
        for uid in [u for u, q in self.hits.items() if not q or now - q[-1] > self.window]:
            self.hits.pop(uid, None)
            self.warned.pop(uid, None)


_setting_cache: dict[str, tuple[float, str]] = {}


async def cached_setting(key: str, default: str = "off", ttl: float = 5.0) -> str:
    now = time.monotonic()
    hit = _setting_cache.get(key)
    if hit and now - hit[0] < ttl:
        return hit[1]
    value = await db.get_setting(key, default) or default
    _setting_cache[key] = (now, value)
    return value


def invalidate_setting(key: str) -> None:
    _setting_cache.pop(key, None)


async def _deny(event: Message | CallbackQuery, text: str) -> None:
    try:
        if isinstance(event, CallbackQuery):
            await event.answer(text, show_alert=True)
        else:
            await event.answer(text)
    except TelegramAPIError:
        pass


class AccessMiddleware(BaseMiddleware):
    """সব আপডেটে: ব্যবহারকারী নিবন্ধন, Rate Limit, Ban ও Maintenance চেক।"""

    def __init__(self) -> None:
        s = get_settings()
        self.limiter = RateLimiter(s.rate_limit_count, s.rate_limit_window)

    async def __call__(self, handler, event, data):
        user = event.from_user
        chat = event.chat if isinstance(event, Message) else (event.message.chat if event.message else None)
        if user is None or user.is_bot or (chat is not None and chat.type != "private"):
            return None  # শুধু Private Chat

        admin = await is_admin(user.id)
        if not admin:
            verdict = self.limiter.check(user.id)
            if verdict == "warn":
                await _deny(event, RATE_MSG)
                return None
            if verdict == "drop":
                if isinstance(event, CallbackQuery):
                    await _deny_silent(event)
                return None

        try:
            row = await db.upsert_user(user.id, user.username, user.full_name)
            maintenance = await cached_setting("maintenance_mode") == "on"
        except Exception:
            log.exception("Middleware-এ Database সমস্যা")
            await _deny(event, DB_ERR_MSG)
            return None

        if row["is_banned"] and not admin:
            await _deny(event, BANNED_MSG)
            return None
        if maintenance and not admin:
            await _deny(event, MAINT_MSG)
            return None

        data["db_user"] = row
        data["is_admin"] = admin
        return await handler(event, data)


async def _deny_silent(event: CallbackQuery) -> None:
    try:
        await event.answer()
    except TelegramAPIError:
        pass


_unauth_alerts: dict[int, float] = {}


class AdminOnlyMiddleware(BaseMiddleware):
    """Admin Router-এর প্রতিটি Handler-কে সুরক্ষিত রাখে।"""

    async def __call__(self, handler, event, data):
        user = event.from_user
        if user is not None and (data.get("is_admin") or await is_admin(user.id)):
            return await handler(event, data)

        if user is not None:
            await db.log_activity(user.id, "unauthorized_admin_access")
            now = time.monotonic()
            bot = data.get("bot")
            if bot is not None and now - _unauth_alerts.get(user.id, 0.0) > 600:
                _unauth_alerts[user.id] = now
                await notify_admins(
                    bot,
                    "🚨 <b>অননুমোদিত অ্যাডমিন অ্যাক্সেসের চেষ্টা</b>\n\n"
                    f"ব্যবহারকারী: {esc(user.full_name)} (<code>{user.id}</code>)",
                )
        await _deny(event, ADMIN_ONLY_MSG)
        return None
