from __future__ import annotations

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message

from app import database as db
from app.keyboards.main_menu import profile_kb
from app.services.ui import esc, fmt_dt, show

router = Router(name="profile")


@router.message(Command("profile"))
@router.callback_query(F.data == "profile:view")
async def view(event: Message | CallbackQuery, db_user) -> None:
    uid = event.from_user.id
    total = await db.count_user_activity(uid)
    text = (
        "👤 <b>আমার প্রোফাইল</b>\n\n"
        f"নাম: {esc(db_user['full_name'])}\n"
        f"🆔 Telegram ID: <code>{uid}</code>\n"
        f"📅 নিবন্ধনের তারিখ: {fmt_dt(db_user['registered_at'])}\n"
        f"📊 মোট কার্যক্রম: {total}"
    )
    await show(event, text, profile_kb())


@router.callback_query(F.data == "profile:id")
async def show_id(cb: CallbackQuery) -> None:
    await cb.answer(f"Telegram ID: {cb.from_user.id}", show_alert=True)


@router.callback_query(F.data == "profile:reg")
async def show_reg(cb: CallbackQuery, db_user) -> None:
    await cb.answer(f"নিবন্ধনের তারিখ: {fmt_dt(db_user['registered_at'])}", show_alert=True)


@router.callback_query(F.data == "profile:total")
async def show_total(cb: CallbackQuery) -> None:
    total = await db.count_user_activity(cb.from_user.id)
    await cb.answer(f"মোট কার্যক্রম: {total}", show_alert=True)
