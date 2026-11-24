from aiogram.types import InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder


def main_menu() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🆕 নতুন অ্যাকাউন্ট যাচাই", callback_data="ver:menu")
    kb.button(text="🔑 অ্যাকাউন্ট পুনরুদ্ধার", callback_data="rec:menu")
    kb.button(text="🌐 নেটওয়ার্ক সেটিংস", callback_data="net:menu")
    kb.button(text="📋 কার্যক্রমের ইতিহাস", callback_data="hist:menu")
    kb.button(text="👤 আমার প্রোফাইল", callback_data="profile:view")
    kb.button(text="📖 সাহায্য ও নির্দেশনা", callback_data="help:view")
    kb.button(text="⚙️ সেটিংস", callback_data="settings:view")
    kb.adjust(2, 2, 2, 1)
    return kb.as_markup()


def home_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🏠 প্রধান মেনু", callback_data="menu:main")
    return kb.as_markup()


def settings_kb(notifications_enabled: bool) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    label = "🔔 নোটিফিকেশন: চালু ✅" if notifications_enabled else "🔕 নোটিফিকেশন: বন্ধ ❌"
    kb.button(text=label, callback_data="settings:toggle_notify")
    kb.button(text="🏠 প্রধান মেনু", callback_data="menu:main")
    kb.adjust(1)
    return kb.as_markup()


def profile_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="👤 Telegram ID", callback_data="profile:id")
    kb.button(text="📅 Registration Date", callback_data="profile:reg")
    kb.button(text="📊 মোট কার্যক্রম", callback_data="profile:total")
    kb.button(text="⚙️ Account Settings", callback_data="settings:view")
    kb.button(text="🏠 প্রধান মেনু", callback_data="menu:main")
    kb.adjust(1, 2, 1, 1)
    return kb.as_markup()


def history_menu() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🆕 Verification History", callback_data="hist:ver")
    kb.button(text="🔑 Recovery History", callback_data="hist:rec")
    kb.button(text="📅 আজকের কার্যক্রম", callback_data="hist:today")
    kb.button(text="📅 আগের কার্যক্রম", callback_data="hist:old")
    kb.button(text="🗑️ History Clear", callback_data="hist:clear")
    kb.button(text="🏠 প্রধান মেনু", callback_data="menu:main")
    kb.adjust(2, 2, 1, 1)
    return kb.as_markup()


def history_clear_confirm() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ হ্যাঁ, মুছে দিন", callback_data="hist:clear_yes")
    kb.button(text="↩️ ফিরে যান", callback_data="hist:menu")
    kb.adjust(1)
    return kb.as_markup()


def network_menu() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🌐 Connection Status", callback_data="net:conn")
    kb.button(text="🔍 Network Test", callback_data="net:test")
    kb.button(text="🔐 Proxy Settings", callback_data="net:proxy")
    kb.button(text="📊 Server Status", callback_data="net:server")
    kb.button(text="↩️ ফিরে যান", callback_data="menu:main")
    kb.adjust(2, 2, 1)
    return kb.as_markup()


def network_back() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="↩️ ফিরে যান", callback_data="net:menu")
    kb.button(text="🏠 প্রধান মেনু", callback_data="menu:main")
    kb.adjust(2)
    return kb.as_markup()
