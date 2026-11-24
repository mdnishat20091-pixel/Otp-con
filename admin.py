"""Admin Dashboard ও Admin Commands। প্রতিটি Handler AdminOnlyMiddleware দ্বারা সুরক্ষিত।"""
from __future__ import annotations

import asyncio
import logging
import os
import signal
from datetime import datetime

from aiogram import Bot, F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message

from app import database as db
from app.config import get_settings
from app.keyboards import admin as kb
from app.services import status as health
from app.services.backup import run_backup
from app.services.app_logging import recent_logs
from app.services.notifications import broadcast, notify_admins
from app.services.security import (
    AdminOnlyMiddleware,
    invalidate_setting,
    is_admin,
    is_owner,
    parse_telegram_id,
)
from app.services.ui import ACTION_LABELS, esc, fmt_dt, show
from app.states.admin import AdminStates

log = logging.getLogger(__name__)

router = Router(name="admin")
router.message.middleware(AdminOnlyMiddleware())
router.callback_query.middleware(AdminOnlyMiddleware())

TOGGLES = {
    "broadcast_enabled": "📨 ব্যবহারকারীদের Notification",
    "admin_alerts": "🔔 অ্যাডমিন অ্যালার্ট",
}

DASHBOARD = "🛠️ <b>অ্যাডমিন ড্যাশবোর্ড</b>\n\nনিচের মেনু থেকে একটি অপশন বেছে নিন।"


# --------------------------------------------------------------- dashboard
@router.message(Command("admin"))
@router.callback_query(F.data == "adm:home")
async def dashboard(event: Message | CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await show(event, DASHBOARD, kb.admin_menu())


# ------------------------------------------------------------------- stats
@router.message(Command("stats"))
@router.callback_query(F.data == "adm:stats")
async def stats(event: Message | CallbackQuery) -> None:
    s = await db.stats()
    text = (
        "📊 <b>Bot Statistics</b>\n\n"
        f"👥 মোট ব্যবহারকারী: {s['users']}\n"
        f"🆕 গত ২৪ ঘণ্টায় নতুন: {s['new24']}\n"
        f"🟢 গত ২৪ ঘণ্টায় সক্রিয়: {s['active24']}\n"
        f"🚫 ব্লক করা: {s['banned']}\n\n"
        f"🆕 যাচাই সেশন: মোট {s['ver_total']} • সম্পন্ন {s['ver_done']}\n"
        f"🔑 Recovery সেশন: মোট {s['rec_total']} • সম্পন্ন {s['rec_done']}"
    )
    await show(event, text, kb.refresh_kb("adm:stats"))


# ------------------------------------------------------------------- users
@router.message(Command("users"))
@router.callback_query(F.data == "adm:users")
async def users_menu(event: Message | CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    total = await db.count_users()
    await show(event, f"👥 <b>ব্যবহারকারী পরিচালনা</b>\n\nমোট ব্যবহারকারী: {total}", kb.users_kb())


@router.callback_query(F.data == "adm:users:list")
async def users_list(cb: CallbackQuery) -> None:
    rows = await db.list_recent_users(15)
    lines = [
        f"{'🚫' if r['is_banned'] else '👤'} <code>{r['telegram_id']}</code> — "
        f"{esc(r['full_name'] or '-')} • {fmt_dt(r['registered_at'])}"
        for r in rows
    ] or ["কোনো ব্যবহারকারী নেই।"]
    await show(cb, "📋 <b>সাম্প্রতিক ব্যবহারকারী</b>\n\n" + "\n".join(lines), kb.users_kb())


@router.callback_query(F.data.in_({"adm:users:ban", "adm:users:unban"}))
async def ask_user_id(cb: CallbackQuery, state: FSMContext) -> None:
    mode = cb.data.split(":")[-1]
    await state.set_state(AdminStates.waiting_user_id)
    await state.update_data(mode=mode)
    action = "ব্লক" if mode == "ban" else "আনব্লক"
    await show(cb, f"🆔 যে ব্যবহারকারীকে <b>{action}</b> করবেন তার Telegram ID পাঠান (শুধু সংখ্যা)।", kb.cancel_kb())


@router.message(AdminStates.waiting_user_id, F.text)
async def got_user_id(message: Message, state: FSMContext) -> None:
    uid = parse_telegram_id(message.text)
    if uid is None:
        await message.answer("❌ সঠিক Telegram ID দিন (শুধু সংখ্যা)।", reply_markup=kb.cancel_kb())
        return
    mode = (await state.get_data()).get("mode", "ban")
    await state.clear()
    if mode == "ban" and await is_admin(uid):
        await message.answer("⛔ অ্যাডমিনকে ব্লক করা যায় না।", reply_markup=kb.users_kb())
        return
    found = await db.set_banned(uid, mode == "ban")
    if not found:
        await message.answer("❌ এই ID-র কোনো ব্যবহারকারী পাওয়া যায়নি।", reply_markup=kb.users_kb())
        return
    await db.log_activity(message.from_user.id, f"admin_{mode}", str(uid))
    done = "ব্লক করা হয়েছে 🚫" if mode == "ban" else "আনব্লক করা হয়েছে ✅"
    await message.answer(f"<code>{uid}</code> — {done}", reply_markup=kb.users_kb())


# ------------------------------------------------------------ notification
@router.callback_query(F.data == "adm:notify")
async def notify_ask(cb: CallbackQuery, state: FSMContext) -> None:
    if await db.get_setting("broadcast_enabled", "on") != "on":
        await show(cb, "ℹ️ Notification পাঠানো বর্তমানে বন্ধ। Bot Settings থেকে চালু করুন।", kb.admin_back())
        return
    await state.set_state(AdminStates.waiting_broadcast)
    await show(cb, "📨 সব ব্যবহারকারীকে যে বার্তা পাঠাতে চান তা লিখে পাঠান (সর্বোচ্চ ৩০০০ অক্ষর)।", kb.cancel_kb())


@router.message(AdminStates.waiting_broadcast, F.text)
async def notify_preview(message: Message, state: FSMContext) -> None:
    text = message.text.strip()
    if not text or len(text) > 3000:
        await message.answer("❌ বার্তা ফাঁকা বা অতিরিক্ত লম্বা। আবার লিখুন।", reply_markup=kb.cancel_kb())
        return
    await state.update_data(text=text)
    await message.answer(
        "👁️ <b>প্রিভিউ</b>\n\n📨 <b>বিজ্ঞপ্তি</b>\n\n" + esc(text) + "\n\nপাঠাবেন?",
        reply_markup=kb.notify_confirm(),
    )


@router.callback_query(F.data == "adm:notify:send")
async def notify_send(cb: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    text = (await state.get_data()).get("text")
    await state.clear()
    if not text:
        await show(cb, "❌ কোনো বার্তা পাওয়া যায়নি। আবার শুরু করুন।", kb.admin_back())
        return
    await show(cb, "⏳ বার্তা পাঠানো হচ্ছে…", None)
    sent, failed = await broadcast(bot, text, cb.from_user.id)
    await db.log_activity(cb.from_user.id, "admin_broadcast", f"sent={sent} failed={failed}")
    await show(cb, f"✅ পাঠানো শেষ।\n\nসফল: {sent}\nব্যর্থ: {failed}", kb.admin_back())


# -------------------------------------------------------------------- logs
@router.callback_query(F.data == "adm:logs")
async def activity_logs(cb: CallbackQuery) -> None:
    rows = await db.recent_activity(15)
    lines = [
        f"{fmt_dt(r['created_at'])} • <code>{r['user_id']}</code> • "
        f"{ACTION_LABELS.get(r['action'], esc(r['action']))}"
        for r in rows
    ] or ["কোনো কার্যক্রম নেই।"]
    await show(cb, "📝 <b>Activity Logs</b> (সর্বশেষ ১৫টি)\n\n" + "\n".join(lines), kb.logs_kb())


def _app_logs_text() -> str:
    body = esc("\n".join(recent_logs(20)) or "কোনো লগ নেই।")
    if len(body) > 3400:
        body = body[-3400:]
    return f"🧾 <b>Application Logs</b> (সর্বশেষ ২০ লাইন)\n\n<pre>{body}</pre>"


@router.message(Command("logs"))
async def cmd_logs(message: Message) -> None:
    await message.answer(_app_logs_text(), reply_markup=kb.applogs_kb())


@router.callback_query(F.data == "adm:applogs")
async def app_logs(cb: CallbackQuery) -> None:
    await show(cb, _app_logs_text(), kb.applogs_kb())


# ------------------------------------------------------------ cloud / db
@router.message(Command("health"))
@router.callback_query(F.data == "adm:cloud")
async def cloud_status(event: Message | CallbackQuery, bot: Bot) -> None:
    await show(event, health.format_health(await health.collect(bot)), kb.cloud_kb())


@router.callback_query(F.data == "adm:db")
async def db_status(cb: CallbackQuery) -> None:
    latency = await db.ping()
    if latency is None:
        await show(cb, "🗄️ <b>Database Status</b>\n\n❌ সংযোগে সমস্যা।", kb.refresh_kb("adm:db"))
        return
    info = await db.db_info()
    rows = "\n".join(f"• {name}: {count}" for name, count in info["counts"].items())
    text = (
        "🗄️ <b>Database Status</b>\n\n"
        f"অবস্থা: ✅ ঠিক আছে ({latency:.0f} ms)\n"
        f"PostgreSQL: {esc(info['version'])}\n"
        f"আকার: {esc(info['size'])}\n\n"
        f"<b>টেবিলের রেকর্ড:</b>\n{rows}"
    )
    await show(cb, text, kb.refresh_kb("adm:db"))


# ------------------------------------------------------------------ backup
@router.message(Command("backup"))
@router.callback_query(F.data == "adm:backup")
async def backup_menu(event: Message | CallbackQuery) -> None:
    raw = await db.get_setting("last_backup_at")
    last = fmt_dt(datetime.fromisoformat(raw)) if raw else "এখনো হয়নি"
    hours = get_settings().backup_interval_hours
    text = (
        "💾 <b>Backup Management</b>\n\n"
        f"শেষ সফল Backup: {last}\n"
        f"স্বয়ংক্রিয় Backup: প্রতি {hours} ঘণ্টা\n\n"
        "Backup ফাইল (.json.gz) অ্যাডমিনদের Telegram-এ পাঠানো হয়। এতে কোনো Password/OTP থাকে না।"
    )
    await show(event, text, kb.backup_kb())


@router.callback_query(F.data == "adm:backup:now")
async def backup_now(cb: CallbackQuery, bot: Bot) -> None:
    await cb.answer("⏳ Backup তৈরি হচ্ছে…")
    ok = await run_backup(bot, [cb.from_user.id])
    if ok:
        await db.log_activity(cb.from_user.id, "admin_backup")
    await show(
        cb,
        "✅ Backup তৈরি হয়ে আপনার কাছে পাঠানো হয়েছে।" if ok else "❌ Backup তৈরি বা পাঠানো যায়নি। Logs দেখুন।",
        kb.backup_kb(),
    )


# ---------------------------------------------------------------- settings
async def _render_settings(event: Message | CallbackQuery) -> None:
    toggles = [(k, label, await db.get_setting(k, "on") == "on") for k, label in TOGGLES.items()]
    await show(event, "⚙️ <b>Bot Settings</b>\n\nচালু/বন্ধ করতে বাটনে চাপুন।", kb.settings_kb(toggles))


@router.callback_query(F.data == "adm:settings")
async def settings(cb: CallbackQuery) -> None:
    await _render_settings(cb)


@router.callback_query(F.data.startswith("adm:set:"))
async def settings_toggle(cb: CallbackQuery) -> None:
    key = cb.data.split(":", 2)[2]
    if key not in TOGGLES:
        await cb.answer("অজানা সেটিং", show_alert=True)
        return
    new = "off" if await db.get_setting(key, "on") == "on" else "on"
    await db.set_setting(key, new)
    await db.log_activity(cb.from_user.id, "admin_setting", f"{key}={new}")
    await _render_settings(cb)


# -------------------------------------------------------------- admin mgmt
async def _render_admins(event: Message | CallbackQuery) -> None:
    owners = sorted(get_settings().admin_ids)
    extra = await db.list_db_admins()
    lines = ["🔐 <b>Admin Management</b>\n", "<b>প্রধান অ্যাডমিন (ADMIN_IDS):</b>"]
    lines += [f"• <code>{i}</code>" for i in owners] or ["• নেই"]
    lines += ["", "<b>যোগ করা অ্যাডমিন:</b>"]
    lines += [f"• <code>{r['telegram_id']}</code>" for r in extra] or ["• নেই"]
    lines += ["", "ℹ️ শুধু প্রধান অ্যাডমিনরা অ্যাডমিন যোগ/অপসারণ করতে পারেন।"]
    await show(event, "\n".join(lines), kb.admins_kb())


@router.callback_query(F.data == "adm:admins")
async def admins_menu(cb: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await _render_admins(cb)


@router.callback_query(F.data.in_({"adm:admins:add", "adm:admins:remove"}))
async def ask_admin_id(cb: CallbackQuery, state: FSMContext) -> None:
    if not is_owner(cb.from_user.id):
        await cb.answer("⛔ শুধু প্রধান অ্যাডমিন এটি করতে পারেন।", show_alert=True)
        return
    mode = cb.data.split(":")[-1]
    await state.set_state(AdminStates.waiting_admin_id)
    await state.update_data(mode=mode)
    action = "যোগ" if mode == "add" else "অপসারণ"
    await show(cb, f"🆔 যে ব্যক্তিকে অ্যাডমিন হিসেবে <b>{action}</b> করবেন তার Telegram ID পাঠান।", kb.cancel_kb())


@router.message(AdminStates.waiting_admin_id, F.text)
async def got_admin_id(message: Message, state: FSMContext) -> None:
    uid = parse_telegram_id(message.text)
    if uid is None:
        await message.answer("❌ সঠিক Telegram ID দিন (শুধু সংখ্যা)।", reply_markup=kb.cancel_kb())
        return
    if not is_owner(message.from_user.id):
        await state.clear()
        return
    mode = (await state.get_data()).get("mode", "add")
    await state.clear()
    if mode == "add":
        await db.add_db_admin(uid, message.from_user.id)
        await db.log_activity(message.from_user.id, "admin_add", str(uid))
        await message.answer(f"✅ <code>{uid}</code> এখন অ্যাডমিন।", reply_markup=kb.admins_kb())
    else:
        if is_owner(uid):
            await message.answer("⛔ প্রধান অ্যাডমিন সরানো যায় না (ADMIN_IDS থেকে সরাতে হবে)।", reply_markup=kb.admins_kb())
            return
        removed = await db.remove_db_admin(uid)
        if removed:
            await db.log_activity(message.from_user.id, "admin_remove", str(uid))
        await message.answer(
            f"✅ <code>{uid}</code> সরানো হয়েছে।" if removed else "❌ এই ID অ্যাডমিন তালিকায় নেই।",
            reply_markup=kb.admins_kb(),
        )


# -------------------------------------------------------------- maintenance
async def _render_maintenance(event: Message | CallbackQuery) -> None:
    enabled = await db.get_setting("maintenance_mode", "off") == "on"
    state = "🔧 চালু (সাধারণ ব্যবহারকারীরা Bot ব্যবহার করতে পারবে না)" if enabled else "🟢 বন্ধ (Bot স্বাভাবিক)"
    await show(event, f"🔧 <b>Maintenance Mode</b>\n\nবর্তমান অবস্থা: {state}", kb.maintenance_kb(enabled))


@router.message(Command("maintenance"))
@router.callback_query(F.data == "adm:maint")
async def maintenance(event: Message | CallbackQuery) -> None:
    await _render_maintenance(event)


@router.callback_query(F.data == "adm:maint:toggle")
async def maintenance_toggle(cb: CallbackQuery, bot: Bot) -> None:
    new = "off" if await db.get_setting("maintenance_mode", "off") == "on" else "on"
    await db.set_setting("maintenance_mode", new)
    invalidate_setting("maintenance_mode")
    await db.log_activity(cb.from_user.id, "admin_maintenance", new)
    await notify_admins(bot, f"🔧 Maintenance Mode {'চালু' if new == 'on' else 'বন্ধ'} করা হয়েছে।")
    await _render_maintenance(cb)


# ----------------------------------------------------------------- restart
@router.message(Command("restart"))
@router.callback_query(F.data == "adm:restart")
async def restart_ask(event: Message | CallbackQuery) -> None:
    await show(
        event,
        "♻️ <b>Server Restart</b>\n\nBot নিরাপদে বন্ধ হয়ে Cloud প্ল্যাটফর্ম স্বয়ংক্রিয়ভাবে আবার চালু করবে। "
        "কয়েক সেকেন্ড Bot সাড়া দেবে না।\n\nনিশ্চিত?",
        kb.restart_confirm(),
    )


@router.callback_query(F.data == "adm:restart:yes")
async def restart_yes(cb: CallbackQuery, bot: Bot) -> None:
    await db.log_activity(cb.from_user.id, "admin_restart")
    await notify_admins(bot, f"♻️ অ্যাডমিন <code>{cb.from_user.id}</code> Server Restart শুরু করেছেন।")
    await show(cb, "♻️ Restart হচ্ছে… কয়েক সেকেন্ড পর /admin দিন।", None)
    # SIGTERM → স্বাভাবিক shutdown; Railway/Docker restart policy আবার চালু করবে
    asyncio.get_running_loop().call_later(2, os.kill, os.getpid(), signal.SIGTERM)
