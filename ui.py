"""সবার জন্য সাধারণ UI সহায়ক: নিরাপদ মেসেজ এডিট, তারিখ ফরম্যাট, লেবেল।"""
from __future__ import annotations

import html
from datetime import datetime
from zoneinfo import ZoneInfo

from aiogram.exceptions import TelegramBadRequest
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, Message

from app.config import get_settings

STATUS_LABELS = {
    "started": "🟡 চলমান",
    "completed": "✅ সম্পন্ন",
    "cancelled": "❌ বাতিল",
}

ACTION_LABELS = {
    "verification_started": "🆕 যাচাই শুরু",
    "verification_completed": "✅ যাচাই সম্পন্ন",
    "verification_cancelled": "❌ যাচাই বাতিল",
    "recovery_started": "🔎 Recovery শুরু",
    "recovery_completed": "✅ Recovery সম্পন্ন",
    "recovery_cancelled": "❌ Recovery বাতিল",
    "processes_cancelled": "❌ প্রক্রিয়া বাতিল",
    "history_cleared": "🗑️ ইতিহাস মোছা",
    "unauthorized_admin_access": "⛔ অননুমোদিত অ্যাডমিন চেষ্টা",
    "admin_ban": "🚫 ব্যবহারকারী ব্লক",
    "admin_unban": "✅ ব্যবহারকারী আনব্লক",
    "admin_broadcast": "📨 Notification পাঠানো",
    "admin_backup": "💾 Backup",
    "admin_maintenance": "🔧 Maintenance পরিবর্তন",
    "admin_setting": "⚙️ Bot Setting পরিবর্তন",
    "admin_add": "➕ অ্যাডমিন যোগ",
    "admin_remove": "➖ অ্যাডমিন অপসারণ",
    "admin_restart": "🔄 Server Restart",
}


def esc(value) -> str:
    return html.escape(str(value)) if value is not None else "-"


def fmt_dt(dt: datetime | None) -> str:
    if dt is None:
        return "—"
    try:
        tz = ZoneInfo(get_settings().timezone)
    except Exception:
        tz = ZoneInfo("UTC")
    return dt.astimezone(tz).strftime("%d-%m-%Y %I:%M %p")


def fmt_uptime(seconds: float) -> str:
    seconds = int(seconds)
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes = rem // 60
    parts = []
    if days:
        parts.append(f"{days} দিন")
    if hours or days:
        parts.append(f"{hours} ঘণ্টা")
    parts.append(f"{minutes} মিনিট")
    return " ".join(parts)


async def show(event: Message | CallbackQuery, text: str, markup: InlineKeyboardMarkup | None = None) -> None:
    """Callback হলে বর্তমান মেসেজ এডিট করে, Message হলে নতুন মেসেজ পাঠায়।"""
    if isinstance(event, CallbackQuery):
        msg = event.message
        try:
            if isinstance(msg, Message):
                await msg.edit_text(text, reply_markup=markup)
            else:
                await event.bot.send_message(event.from_user.id, text, reply_markup=markup)
        except TelegramBadRequest as exc:
            if "message is not modified" not in str(exc):
                await event.bot.send_message(event.from_user.id, text, reply_markup=markup)
        try:
            await event.answer()
        except TelegramBadRequest:
            pass  # ইতিমধ্যে উত্তর দেওয়া হয়েছে
    else:
        await event.answer(text, reply_markup=markup)
