from __future__ import annotations

from aiogram import F, Router
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message

from app import database as db
from app.keyboards.main_menu import home_kb, main_menu, settings_kb
from app.services.security import OTP_LIKE_PATTERN
from app.services.status import overall_status_text
from app.services.ui import show

router = Router(name="start")
fallback_router = Router(name="fallback")

WELCOME = (
    "👋 <b>স্বাগতম!</b>\n\n"
    "<b>Facebook Verification Assistant Bot</b>-এ আপনাকে স্বাগতম।\n\n"
    "এই Bot আপনাকে Facebook-এর <b>অফিসিয়াল</b> যাচাই ও পুনরুদ্ধার প্রক্রিয়ায় "
    "ধাপে ধাপে সহায়তা করে।\n\n"
    "🔒 <b>নিরাপত্তা:</b> এই Bot কখনো আপনার Facebook পাসওয়ার্ড বা OTP চায় না এবং "
    "সংরক্ষণও করে না। OTP শুধু Facebook-এর অফিসিয়াল পেজে দিন।\n\n"
    "নিচের মেনু থেকে একটি অপশন বেছে নিন:"
)

HELP_TEXT = (
    "📖 <b>সাহায্য ও নির্দেশনা</b>\n\n"
    "<b>Bot কী করে?</b>\n"
    "Facebook-এর অফিসিয়াল পেজে ফোন নম্বর যাচাই ও অ্যাকাউন্ট পুনরুদ্ধারে ধাপে ধাপে "
    "নির্দেশনা দেয় এবং আপনার প্রক্রিয়ার অবস্থা মনে রাখে।\n\n"
    "<b>Bot যা করে না:</b>\n"
    "• পাসওয়ার্ড বা OTP চায়/সংরক্ষণ করে না\n"
    "• গোপনে অ্যাকাউন্ট খোঁজে না বা অস্তিত্ব নিশ্চিত করে না\n"
    "• Facebook-এর নিরাপত্তা বাইপাস করে না\n\n"
    "<b>কমান্ড:</b>\n"
    "/start — প্রধান মেনু\n"
    "/verify — নতুন অ্যাকাউন্ট যাচাইয়ের নির্দেশনা\n"
    "/recovery — অ্যাকাউন্ট পুনরুদ্ধারের নির্দেশনা\n"
    "/status — বর্তমান অবস্থা\n"
    "/history — কার্যক্রমের ইতিহাস\n"
    "/profile — আমার প্রোফাইল\n"
    "/cancel — চলমান প্রক্রিয়া বাতিল\n"
    "/help — সাহায্য"
)

ADMIN_HELP = (
    "\n\n🛠️ <b>অ্যাডমিন কমান্ড:</b>\n"
    "/admin — অ্যাডমিন ড্যাশবোর্ড\n"
    "/stats — Bot পরিসংখ্যান\n"
    "/users — ব্যবহারকারী পরিচালনা\n"
    "/health — Server স্বাস্থ্য\n"
    "/logs — Application Logs\n"
    "/backup — Backup অবস্থা\n"
    "/maintenance — Maintenance নিয়ন্ত্রণ\n"
    "/restart — অনুমোদিত Server Restart"
)

OTP_WARNING = (
    "⚠️ <b>সতর্কতা!</b>\n\n"
    "আপনার পাঠানো বার্তাটি OTP-এর মতো দেখাচ্ছে, তাই সেটি মুছে ফেলা হয়েছে।\n"
    "এই Bot-কে কখনো OTP বা পাসওয়ার্ড দেবেন না — শুধু Facebook-এর অফিসিয়াল পেজে দিন।"
)


@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext) -> None:
    await state.clear()
    await message.answer(WELCOME, reply_markup=main_menu())


@router.callback_query(F.data == "menu:main")
async def cb_main(cb: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await show(cb, WELCOME, main_menu())


@router.message(Command("help"))
@router.callback_query(F.data == "help:view")
async def help_view(event: Message | CallbackQuery, is_admin: bool = False) -> None:
    await show(event, HELP_TEXT + (ADMIN_HELP if is_admin else ""), home_kb())


@router.message(Command("status"))
async def cmd_status(message: Message) -> None:
    await message.answer(await overall_status_text(message.from_user.id), reply_markup=home_kb())


@router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext) -> None:
    await state.clear()
    count = await db.cancel_all_active(message.from_user.id)
    if count:
        await db.log_activity(message.from_user.id, "processes_cancelled")
        text = "❌ চলমান প্রক্রিয়া বাতিল করা হয়েছে।"
    else:
        text = "ℹ️ বাতিল করার মতো কোনো চলমান প্রক্রিয়া নেই।"
    await message.answer(text, reply_markup=main_menu())


async def _settings_text(db_user) -> str:
    state = "চালু ✅" if db_user["notifications_enabled"] else "বন্ধ ❌"
    return (
        "⚙️ <b>সেটিংস</b>\n\n"
        f"🔔 Bot-এর বিজ্ঞপ্তি পাওয়া: {state}\n\n"
        "বিজ্ঞপ্তি বন্ধ থাকলে অ্যাডমিনের পাঠানো সাধারণ বিজ্ঞপ্তি আপনি পাবেন না।"
    )


@router.callback_query(F.data == "settings:view")
async def settings_view(cb: CallbackQuery, db_user) -> None:
    await show(cb, await _settings_text(db_user), settings_kb(db_user["notifications_enabled"]))


@router.callback_query(F.data == "settings:toggle_notify")
async def settings_toggle(cb: CallbackQuery, db_user) -> None:
    new_value = not db_user["notifications_enabled"]
    await db.set_notifications(cb.from_user.id, new_value)
    fresh = await db.upsert_user(cb.from_user.id, cb.from_user.username, cb.from_user.full_name)
    await show(cb, await _settings_text(fresh), settings_kb(fresh["notifications_enabled"]))


# ---------------------------------------------------------------- fallback
@fallback_router.message(F.text.regexp(OTP_LIKE_PATTERN))
async def otp_guard(message: Message) -> None:
    try:
        await message.delete()
    except Exception:
        pass
    await message.answer(OTP_WARNING, reply_markup=main_menu())


@fallback_router.message()
async def unknown_message(message: Message) -> None:
    await message.answer("ℹ️ অনুগ্রহ করে নিচের মেনু বাটন ব্যবহার করুন।", reply_markup=main_menu())


@fallback_router.callback_query()
async def stale_button(cb: CallbackQuery) -> None:
    await cb.answer("এই বাটনটি আর সক্রিয় নেই। /start দিয়ে নতুন মেনু খুলুন।", show_alert=True)
