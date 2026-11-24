"""নেটওয়ার্ক সেটিংস: শুধু বৈধ কানেক্টিভিটি ও Server সংযোগ পরীক্ষা।"""
from __future__ import annotations

import asyncio
import os
import time
from urllib.parse import urlparse

import aiohttp
from aiogram import Bot, F, Router
from aiogram.types import CallbackQuery

from app.config import get_settings
from app.keyboards.main_menu import network_back, network_menu
from app.services.status import collect, format_user_server_status
from app.services.ui import show

router = Router(name="network")

TARGETS = (
    ("Telegram API", "https://api.telegram.org"),
    ("Facebook (অফিসিয়াল সাইট)", "https://www.facebook.com"),
)


@router.callback_query(F.data == "net:menu")
async def menu(cb: CallbackQuery) -> None:
    await show(cb, "🌐 <b>নেটওয়ার্ক সেটিংস</b>\n\nসংযোগ ও Server অবস্থা পরীক্ষা করুন।", network_menu())


@router.callback_query(F.data == "net:conn")
async def connection_status(cb: CallbackQuery, bot: Bot) -> None:
    h = await collect(bot)
    text = (
        "🌐 <b>Connection Status</b>\n\n"
        f"🤖 Bot ↔ Telegram: {'✅ সংযুক্ত' if h['telegram_ok'] else '❌ সমস্যা'}\n"
        f"🗄️ Database: {'✅ সংযুক্ত' if h['db_ok'] else '❌ সমস্যা'}"
    )
    await show(cb, text, network_back())


async def _probe(session: aiohttp.ClientSession, name: str, url: str) -> str:
    start = time.perf_counter()
    try:
        async with session.get(url, allow_redirects=True) as resp:
            ms = (time.perf_counter() - start) * 1000
            ok = resp.status < 500
            return f"{'✅' if ok else '❌'} {name}: {resp.status} ({ms:.0f} ms)"
    except asyncio.TimeoutError:
        return f"❌ {name}: সময় শেষ (Timeout)"
    except Exception:
        return f"❌ {name}: সংযোগ করা যায়নি"


@router.callback_query(F.data == "net:test")
async def network_test(cb: CallbackQuery) -> None:
    await show(cb, "⏳ <b>Network Test চলছে…</b>\n\nঅনুগ্রহ করে কয়েক সেকেন্ড অপেক্ষা করুন।", None)
    timeout = aiohttp.ClientTimeout(total=8)
    async with aiohttp.ClientSession(timeout=timeout, trust_env=True) as session:
        lines = await asyncio.gather(*(_probe(session, n, u) for n, u in TARGETS))
    await show(cb, "🔍 <b>Network Test ফলাফল</b>\n\n" + "\n".join(lines), network_back())


def _mask(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.hostname or "?"
    return f"{host}:{parsed.port}" if parsed.port else host


@router.callback_query(F.data == "net:proxy")
async def proxy_settings(cb: CallbackQuery) -> None:
    url = get_settings().proxy_url
    env_proxy = os.getenv("HTTPS_PROXY") or os.getenv("https_proxy") or ""
    if url:
        state = f"✅ Bot সংযোগে প্রক্সি সক্রিয়: <code>{_mask(url)}</code>"
    elif env_proxy:
        state = f"ℹ️ সিস্টেম প্রক্সি পাওয়া গেছে: <code>{_mask(env_proxy)}</code> (Network Test-এ ব্যবহার হয়)"
    else:
        state = "ℹ️ কোনো প্রক্সি কনফিগার করা নেই। সরাসরি সংযোগ ব্যবহার হচ্ছে।"
    text = (
        "🔐 <b>Proxy Settings</b>\n\n"
        f"{state}\n\n"
        "প্রক্সি শুধু বৈধ নেটওয়ার্ক কানেক্টিভিটি ও Server সংযোগ পরীক্ষার জন্য। "
        "পরিবর্তন করতে Server-এর <code>PROXY_URL</code> Environment Variable আপডেট করুন (অ্যাডমিন)।\n"
        "প্রক্সির ঠিকানা/পাসওয়ার্ড এখানে দেখানো হয় না।"
    )
    await show(cb, text, network_back())


@router.callback_query(F.data == "net:server")
async def server_status(cb: CallbackQuery, bot: Bot) -> None:
    await show(cb, format_user_server_status(await collect(bot)), network_back())
