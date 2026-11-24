"""স্কিমা তৈরি/আপডেট। বারবার চালালেও নিরাপদ (idempotent)।"""
from __future__ import annotations

import logging

from database.models import DEFAULT_SETTINGS, SCHEMA

log = logging.getLogger(__name__)


async def run_migrations(pool) -> None:
    async with pool.acquire() as conn:
        async with conn.transaction():
            # একাধিক ইনস্ট্যান্স একসঙ্গে চালু হলে রেস কন্ডিশন এড়াতে
            await conn.execute("SELECT pg_advisory_xact_lock(727274)")
            await conn.execute(SCHEMA)
            for key, value in DEFAULT_SETTINGS.items():
                await conn.execute(
                    "INSERT INTO bot_settings (key, value) VALUES ($1, $2) "
                    "ON CONFLICT (key) DO NOTHING",
                    key,
                    value,
                )
    log.info("Database migration সম্পন্ন")
