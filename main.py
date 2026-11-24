"""Facebook Verification Assistant — প্রবেশ বিন্দু।  চালানো: python -m app.main"""
from __future__ import annotations

import asyncio
import logging
import signal
import sys

import asyncpg
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import BotCommand, BotCommandScopeChat, BotCommandScopeDefault, ErrorEvent
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web

from app import database as db
from app.config import get_settings
from app.handlers import admin, history, network, profile, recovery, start, verification
from app.services.backup import backup_loop
from app.services.app_logging import setup_logging
from app.services.notifications import admin_targets, notify_admins, notify_admins_throttled
from app.services.security import AccessMiddleware
from app.services.status import monitor_loop

log = logging.getLogger("app")
_tasks: list[asyncio.Task] = []
_bot: Bot | None = None

TEMP_ERROR = "⚠️ সাময়িক সমস্যা হয়েছে। অনুগ্রহ করে কিছুক্ষণ পর আবার চেষ্টা করুন।"

USER_COMMANDS = [
    BotCommand(command="start", description="প্রধান মেনু"),
    BotCommand(command="help", description="সাহায্য"),
    BotCommand(command="profile", description="আমার প্রোফাইল"),
    BotCommand(command="history", description="কার্যক্রমের ইতিহাস"),
    BotCommand(command="verify", description="নতুন অ্যাকাউন্ট যাচাইয়ের নির্দেশনা"),
    BotCommand(command="recovery", description="অ্যাকাউন্ট পুনরুদ্ধারের নির্দেশনা"),
    BotCommand(command="status", description="বর্তমান অবস্থা"),
    BotCommand(command="cancel", description="চলমান প্রক্রিয়া বাতিল"),
]
ADMIN_COMMANDS = USER_COMMANDS + [
    BotCommand(command="admin", description="অ্যাডমিন ড্যাশবোর্ড"),
    BotCommand(command="stats", description="Bot পরিসংখ্যান"),
    BotCommand(command="users", description="ব্যবহারকারী পরিচালনা"),
    BotCommand(command="health", description="Server স্বাস্থ্য"),
    BotCommand(command="logs", description="Application Logs"),
    BotCommand(command="backup", description="Backup অবস্থা"),
    BotCommand(command="maintenance", description="Maintenance নিয়ন্ত্রণ"),
    BotCommand(command="restart", description="অনুমোদিত Server Restart"),
]


async def connect_db_with_retry(dsn: str) -> None:
    for attempt in range(1, 7):
        try:
            await db.connect(dsn)
            return
        except Exception:
            log.exception("Database সংযোগ ব্যর্থ (চেষ্টা %s/6)", attempt)
            await asyncio.sleep(min(5 * attempt, 20))
    raise RuntimeError("Database-এর সঙ্গে সংযোগ করা যায়নি")


async def set_commands(bot: Bot) -> None:
    await bot.set_my_commands(USER_COMMANDS, scope=BotCommandScopeDefault())
    for uid in await admin_targets():
        try:
            await bot.set_my_commands(ADMIN_COMMANDS, scope=BotCommandScopeChat(chat_id=uid))
        except Exception:
            log.warning("অ্যাডমিন কমান্ড সেট করা যায়নি: %s", uid)


async def on_startup(bot: Bot) -> None:
    await connect_db_with_retry(get_settings().database_url)
    await set_commands(bot)
    _tasks.append(asyncio.create_task(monitor_loop(bot), name="monitor"))
    _tasks.append(asyncio.create_task(backup_loop(bot), name="backup"))
    me = await bot.get_me()
    log.info("Bot চালু হয়েছে: @%s", me.username)
    await notify_admins(bot, f"🟢 <b>Bot চালু হয়েছে</b>\n@{me.username}")


async def on_shutdown(bot: Bot) -> None:
    log.info("Bot বন্ধ হচ্ছে…")
    try:
        await notify_admins(bot, "🔴 <b>Bot বন্ধ হচ্ছে</b>")
    except Exception:
        pass
    for task in _tasks:
        task.cancel()
    await asyncio.gather(*_tasks, return_exceptions=True)
    await db.close()


async def on_error(event: ErrorEvent) -> bool:
    exc = event.exception
    log.error("Unhandled error: %s", exc, exc_info=exc)
    update = event.update
    bot = _bot
    if isinstance(exc, (asyncpg.PostgresError, asyncpg.InterfaceError, OSError)) and bot is not None:
        await notify_admins_throttled(
            bot, "db-error", f"🚨 <b>Database/Server ত্রুটি</b>\n<code>{type(exc).__name__}</code>"
        )
    try:
        if update.callback_query:
            await update.callback_query.answer(TEMP_ERROR, show_alert=True)
        elif update.message:
            await update.message.answer(TEMP_ERROR)
    except Exception:
        pass
    return True


def build_dispatcher() -> Dispatcher:
    dp = Dispatcher(storage=MemoryStorage())
    access = AccessMiddleware()
    dp.message.outer_middleware(access)
    dp.callback_query.outer_middleware(access)
    # ক্রম গুরুত্বপূর্ণ: fallback সবার শেষে
    for router in (
        admin.router,
        verification.router,
        recovery.router,
        profile.router,
        history.router,
        network.router,
        start.router,
        start.fallback_router,
    ):
        dp.include_router(router)
    dp.errors.register(on_error)
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    return dp


async def health(_: web.Request) -> web.Response:
    latency = await db.ping()
    ok = latency is not None
    return web.json_response({"status": "ok" if ok else "degraded", "database": ok}, status=200 if ok else 503)


async def main() -> None:
    global _bot
    settings = get_settings()
    setup_logging(settings.log_level)

    session = AiohttpSession(proxy=settings.proxy_url) if settings.proxy_url else AiohttpSession()
    bot = Bot(
        settings.bot_token,
        session=session,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML, link_preview_is_disabled=True),
    )
    _bot = bot
    dp = build_dispatcher()

    app = web.Application()
    app.router.add_get("/health", health)
    app.router.add_get("/", health)
    runner = web.AppRunner(app)

    try:
        if settings.use_webhook:
            SimpleRequestHandler(dispatcher=dp, bot=bot, secret_token=settings.webhook_secret).register(
                app, path=settings.webhook_path
            )
            setup_application(app, dp, bot=bot)
            await runner.setup()  # on_startup এখানে চলে
            await web.TCPSite(runner, "0.0.0.0", settings.port).start()
            await bot.set_webhook(
                settings.webhook_url + settings.webhook_path,
                secret_token=settings.webhook_secret,
                allowed_updates=dp.resolve_used_update_types(),
            )
            log.info("Webhook মোডে চালু (port %s)", settings.port)
            stop = asyncio.Event()
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, stop.set)
            await stop.wait()
        else:
            await runner.setup()
            await web.TCPSite(runner, "0.0.0.0", settings.port).start()
            await bot.delete_webhook(drop_pending_updates=False)
            log.info("Polling মোডে চালু (health port %s)", settings.port)
            await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    except Exception as exc:
        log.exception("Deployment/Startup ত্রুটি")
        try:
            await notify_admins(bot, f"🚨 <b>Deployment Error</b>\n<code>{type(exc).__name__}</code>", force=True)
        except Exception:
            pass
        raise
    finally:
        await runner.cleanup()
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
    except RuntimeError as err:
        print(f"ত্রুটি: {err}", file=sys.stderr)
        sys.exit(1)
