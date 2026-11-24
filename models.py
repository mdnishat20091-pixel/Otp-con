"""PostgreSQL স্কিমা। Password বা OTP কোনো টেবিলেই রাখা হয় না।"""

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    telegram_id           BIGINT PRIMARY KEY,
    username              TEXT,
    full_name             TEXT,
    registered_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen             TIMESTAMPTZ NOT NULL DEFAULT now(),
    is_banned             BOOLEAN NOT NULL DEFAULT FALSE,
    notifications_enabled BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS admins (
    telegram_id BIGINT PRIMARY KEY,
    added_by    BIGINT,
    added_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS activity_logs (
    id         BIGSERIAL PRIMARY KEY,
    user_id    BIGINT NOT NULL,
    action     TEXT NOT NULL,
    details    TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_activity_user ON activity_logs (user_id, id DESC);

CREATE TABLE IF NOT EXISTS verification_sessions (
    id          BIGSERIAL PRIMARY KEY,
    user_id     BIGINT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'started',
    started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_ver_user ON verification_sessions (user_id, id DESC);

CREATE TABLE IF NOT EXISTS recovery_sessions (
    id          BIGSERIAL PRIMARY KEY,
    user_id     BIGINT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'started',
    started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_rec_user ON recovery_sessions (user_id, id DESC);

CREATE TABLE IF NOT EXISTS bot_settings (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS notifications (
    id           BIGSERIAL PRIMARY KEY,
    audience     TEXT NOT NULL,
    message      TEXT NOT NULL,
    sent_by      BIGINT,
    sent_count   INTEGER NOT NULL DEFAULT 0,
    failed_count INTEGER NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS system_health (
    id             BIGSERIAL PRIMARY KEY,
    db_ok          BOOLEAN NOT NULL,
    telegram_ok    BOOLEAN NOT NULL,
    cpu_percent    REAL,
    memory_percent REAL,
    uptime_seconds BIGINT,
    error_count    INTEGER NOT NULL DEFAULT 0,
    checked_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

DEFAULT_SETTINGS = {
    "maintenance_mode": "off",
    "broadcast_enabled": "on",
    "admin_alerts": "on",
}
