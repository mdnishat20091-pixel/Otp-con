from __future__ import annotations

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message

from app import database as db
from app.keyboards.main_menu import history_clear_confirm, history_menu
from app.services.ui import ACTION_LABELS, STATUS_LABELS, esc, fmt_dt, show

router = Router(name="history")

MENU_TEXT = "📋 <b>কার্যক্রমের ইতিহাস</b>\n\nকোনটি দেখতে চান?"


@router.message(Command("history"))
@router.callback_query(F.data == "hist:menu")
async def menu(event: Message | CallbackQuery) -> None:
    await show(event, MENU_TEXT, history_menu())


async def _sessions(cb: CallbackQuery, kind: str, title: str) -> None:
    rows = await db.session_history(kind, cb.from_user.id, 10)
    body = "\n".join(
        f"{i}. {STATUS_LABELS.get(r['status'], r['status'])} — {fmt_dt(r['started_at'])}"
        for i, r in enumerate(rows, 1)
    ) or "কোনো তথ্য নেই।"
    await show(cb, f"{title}\n\n{body}", history_menu())


@router.callback_query(F.data == "hist:ver")
async def verification_history(cb: CallbackQuery) -> None:
    await _sessions(cb, "verification", "🆕 <b>Verification History</b> (সর্বশেষ ১০টি)")


@router.callback_query(F.data == "hist:rec")
async def recovery_history(cb: CallbackQuery) -> None:
    await _sessions(cb, "recovery", "🔑 <b>Recovery History</b> (সর্বশেষ ১০টি)")


async def _activity(cb: CallbackQuery, today: bool) -> None:
    rows = await db.user_activity(cb.from_user.id, today=today)
    title = "📅 <b>আজকের কার্যক্রম</b>" if today else "📅 <b>আগের কার্যক্রম</b> (সর্বশেষ ২০টি)"
    body = "\n".join(
        f"• {ACTION_LABELS.get(r['action'], esc(r['action']))} — {fmt_dt(r['created_at'])}" for r in rows
    ) or "কোনো তথ্য নেই।"
    await show(cb, f"{title}\n\n{body}", history_menu())


@router.callback_query(F.data == "hist:today")
async def today(cb: CallbackQuery) -> None:
    await _activity(cb, True)


@router.callback_query(F.data == "hist:old")
async def older(cb: CallbackQuery) -> None:
    await _activity(cb, False)


@router.callback_query(F.data == "hist:clear")
async def clear_ask(cb: CallbackQuery) -> None:
    await show(
        cb,
        "🗑️ <b>ইতিহাস মুছবেন?</b>\n\nআপনার সব Verification, Recovery ও কার্যক্রমের তথ্য স্থায়ীভাবে মুছে যাবে।",
        history_clear_confirm(),
    )


@router.callback_query(F.data == "hist:clear_yes")
async def clear_yes(cb: CallbackQuery) -> None:
    await db.clear_history(cb.from_user.id)
    await db.log_activity(cb.from_user.id, "history_cleared")
    await show(cb, "✅ আপনার ইতিহাস মুছে ফেলা হয়েছে।", history_menu())
