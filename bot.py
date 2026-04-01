import asyncio
import datetime
import html
import json
import os
import random
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

try:
    from telegram import Bot
    from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, Filters
    from telegram import Update
    from telegram.ext import ContextTypes
    TELEGRAM_LIB = "python-telegram-bot"
except ImportError:
    try:
        from aiogram import Bot, Dispatcher, F
        from aiogram.dispatcher.middlewares.base import BaseMiddleware
        from aiogram.filters import Command
        from aiogram.types import (
            BotCommand,
            BotCommandScopeAllGroupChats,
            BotCommandScopeAllPrivateChats,
            CallbackQuery,
            ChatMemberUpdated,
            ChatPermissions,
            FSInputFile,
            InputMediaPhoto,
            InlineKeyboardButton,
            InlineKeyboardMarkup,
            LabeledPrice,
            Message,
            PreCheckoutQuery,
        )
        TELEGRAM_LIB = "aiogram"
    except ImportError:
        print("❌ Neither python-telegram-bot nor aiogram available!")
        exit(1)

TOKEN = "8635930527:AAHPRDuYQK1SQRK6V6G2WpjcYA-fu_O3VAY"  # Token to'g'ridan yozilgan

# Initialize based on available library
if TELEGRAM_LIB == "python-telegram-bot":
    application = Application.builder().token(TOKEN).build()
    bot = application.bot
else:
    bot = Bot(token=TOKEN)

# TOKEN = os.getenv("BOT_TOKEN", "")  # Environment variable o'rniga
# if not TOKEN:
#     raise SystemExit(
#         "BOT_TOKEN topilmadi. PowerShell: $env:BOT_TOKEN=\"xxxx\" qilib qayta ishga tushiring."
#     )

ADMIN_CONTACT = os.getenv("ADMIN_CONTACT", "@admin")
PAYMENT_PROVIDER_TOKEN = os.getenv("PAYMENT_PROVIDER_TOKEN", "")

ADMIN_MANUAL_PAY_ID = 6716178883

try:
    ADMIN_CONTACT_ID = int(os.getenv("ADMIN_CONTACT_ID", "") or 0) or int(ADMIN_MANUAL_PAY_ID)
except Exception:
    ADMIN_CONTACT_ID = int(ADMIN_MANUAL_PAY_ID)

RUNTIME_STATE_PATH = "runtime_state.json"

# Telegram Payments UZS diamond packs (amounts in UZS, exponent=0)
_DIAMOND_BASE_UZS = 1000
DIAMOND_PACKS_UZS: Dict[int, int] = {d: int(d) * int(_DIAMOND_BASE_UZS) for d in (1, 5, 10, 15, 30, 50, 250, 1000)}


def _runtime_load() -> Dict[str, Any]:
    try:
        return _load_json(RUNTIME_STATE_PATH, {})
    except Exception:
        return {}


def _runtime_save(state: Dict[str, Any]) -> None:
    try:
        _save_json(RUNTIME_STATE_PATH, state)
    except Exception:
        return

    return


def _runtime_set(chat_id: int, payload: Dict[str, Any]) -> None:
    state = _runtime_load()
    state[str(chat_id)] = payload
    _runtime_save(state)


def _runtime_clear(chat_id: int) -> None:
    state = _runtime_load()
    state.pop(str(chat_id), None)
    _runtime_save(state)


async def _runtime_recover_on_startup() -> None:
    state = _runtime_load()
    if not isinstance(state, dict) or not state:
        return

    to_clear: List[str] = []
    for cid_s, payload in state.items():
        try:
            chat_id = int(cid_s)
        except Exception:
            continue

        try:
            phase = str((payload or {}).get("phase", ""))
            reg_mid = (payload or {}).get("reg_message_id")
            reg_deadline = float((payload or {}).get("reg_deadline", 0.0) or 0.0)
            if phase == "reg" and reg_mid and reg_deadline > 0:
                now = asyncio.get_event_loop().time()
                if reg_deadline > now:
                    # Restore registration and restart ticker task
                    game = get_game(chat_id)
                    game["phase"] = PHASE_REG
                    game["started"] = False
                    game["reg_message_id"] = int(reg_mid)
                    game["reg_deadline"] = float(reg_deadline)
                    t = game.get("reg_task")
                    if t:
                        try:
                            t.cancel()
                        except Exception:
                            pass
                    game["reg_task"] = asyncio.create_task(_registration_timeout(chat_id, float(reg_deadline)))
                    continue
                else:
                    try:
                        await _safe_delete_message(chat_id, int(reg_mid))
                    except Exception:
                        pass
        except Exception:
            pass

        to_clear.append(cid_s)

    for cid_s in to_clear:
        try:
            state.pop(cid_s, None)
        except Exception:
            pass
    _runtime_save(state)


def silence_item_kb(chat_id: int, key: str) -> InlineKeyboardMarkup:
    g = get_group(chat_id)
    on = bool(g.get(key, False))
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=("🟢 ON" if on else "🔴 OFF"), callback_data=f"silence:toggle:{chat_id}:{key}")],
            [InlineKeyboardButton(text="orqaga", callback_data=f"adminset:silence:{chat_id}")],
        ]
    )


def silence_main_kb(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="O'lganlar", callback_data=f"silence:open:{chat_id}:dead")],
            [InlineKeyboardButton(text="Uxlayotganlar", callback_data=f"silence:open:{chat_id}:sleep")],
            [InlineKeyboardButton(text="O'ynamayotganlar", callback_data=f"silence:open:{chat_id}:inactive")],
            [InlineKeyboardButton(text="⬅️ Orqaga", callback_data=f"adminset:back:{chat_id}")],
        ]
    )


def start_lang_kb(cur: str = "uz") -> InlineKeyboardMarkup:
    langs = [
        ("az", "🇦🇿 Azerbaaycanca"),
        ("tr", "🇹🇷 Türkçe"),
        ("en", "🇺🇸 English"),
        ("ru", "🇷🇺 Русский"),
        ("uk", "🇺🇦 Український"),
        ("kk", "🇰🇿 Қазақ"),
        ("uz", "🇺🇿 O'zbek tili"),
        ("id", "🇮🇩 Indonesia"),
    ]
    rows: List[List[InlineKeyboardButton]] = []
    for code, title in langs:
        prefix = "✅ " if code == cur else ""
        rows.append([InlineKeyboardButton(text=prefix + title, callback_data=f"setlang:{code}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def roles_settings_kb(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅️ Orqaga", callback_data=f"adminset:back:{chat_id}")]]
    )


def admin_roles_kb(chat_id: int) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for role in ROLES_LIST:
        on = _role_enabled(chat_id, role)
        badge = "🟢" if on else "🔴"
        rows.append([InlineKeyboardButton(text=f"{badge} {ROLE_EMOJI.get(role, '🎭')} {role}", callback_data=f"adminrole:{chat_id}:{role}")])
    rows.append([InlineKeyboardButton(text="⬅️ Orqaga", callback_data=f"adminset:back:{chat_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def rolecfg_max_players_kb(chat_id: int) -> InlineKeyboardMarkup:
    g = get_group(chat_id)
    cur = int(g.get("max_players", 30))
    grid = [
        [15, 16, 17, 18, 19],
        [20, 21, 22, 23, 24],
        [25, 26, 27, 28, 29],
        [30],
    ]
    rows: List[List[InlineKeyboardButton]] = []
    for row in grid:
        rows.append(
            [
                InlineKeyboardButton(
                    text=(f"{n} ⬛" if n == cur else f"{n} ⬜"),
                    callback_data=f"rolecfg:set:max:{chat_id}:{n}",
                )
                for n in row
            ]
        )
    rows.append([InlineKeyboardButton(text="orqaga", callback_data=f"rolecfg:back:{chat_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def rolecfg_ratio_kb(chat_id: int) -> InlineKeyboardMarkup:
    g = get_group(chat_id)
    cur = str(g.get("mafia_ratio", "1/3"))
    more = "1/3"
    less = "1/4"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=("Ko'proq (1/3) ⬛" if cur == more else "Ko'proq (1/3) ⬜"),
                    callback_data=f"rolecfg:set:ratio:{chat_id}:{more}",
                ),
                InlineKeyboardButton(
                    text=("Kamroq (1/4) ⬛" if cur == less else "Kamroq (1/4) ⬜"),
                    callback_data=f"rolecfg:set:ratio:{chat_id}:{less}",
                ),
            ],
            [InlineKeyboardButton(text="orqaga", callback_data=f"rolecfg:back:{chat_id}")],
        ]
    )

bot = Bot(token=TOKEN)
dp = Dispatcher()

BOT_USERNAME: Optional[str] = None


async def _bot_url() -> str:
    me = await bot.get_me()
    return f"https://t.me/{me.username}"


async def bot_link_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Bot-ga o'tish", url=await _bot_url())]]
    )


async def reg_join_kb(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="💡 Qo'shilish", url=f"{await _bot_url()}?start=join_{chat_id}")]]
    )

USERS_DIR = "users"
GROUPS_DIR = "groups"

GIVEAWAYS: Dict[Tuple[int, int], Dict[str, Any]] = defaultdict(lambda: {"claimed": set(), "amount": 0})


def _ensure_dir(path: str) -> None:
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        pass


def _load_json_file(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_json_file(path: str, data: Dict[str, Any]) -> None:
    try:
        _ensure_dir(os.path.dirname(path))
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


class _DeleteGroupSlashCommandsMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        result = await handler(event, data)
        try:
            if not isinstance(event, Message):
                return result
            if event.chat.type not in {"group", "supergroup"}:
                return result
            txt = event.text or ""
            if not txt.startswith("/"):
                return result
            g = get_group(event.chat.id)
            if not bool(g.get("delete_commands", True)):
                return result
            await _safe_delete_message(event.chat.id, event.message_id)
        except Exception:
            pass
        return result


DEFAULT_USER: Dict[str, Any] = {
    "name": "User",
    "language": "uz",
    "money": 250,
    "diamonds": 0,
    "wins": 0,
    "games": 0,
    "group_stats": {},
    "daily_claimed": 0,
    "premium": 0,
    "total_earned": 0,
    "total_spent": 0,
    "most_played_role": "-",
    "play_time": 0,
    "protect": 0,
    "anti_killer": 0,
    "vote_protect": 0,
    "gun": 0,
    "mask": 0,
    "fake_docs": 0,
    "next_role": "-",
}

DEFAULT_GROUP: Dict[str, Any] = {
    "registration_seconds": 60,
    "night_seconds": 60,
    "day_seconds": 60,
    "discussion_seconds": 60,
    "magic_seconds": 30,
    "vote_seconds": 45,
    "confirm_seconds": 45,
    "lastword_seconds": 30,
    "afsongar_seconds": 30,
    "allow_teamgame": True,
    "silence_night": False,
    "silence_dead": False,
    "silence_inactive": False,
    "pin_registration": True,
    "delete_commands": True,
    "allow_fake_docs": True,
    "allow_protect": True,
    "allow_mask": True,
    "allow_gun": True,
    "allow_anti_killer": True,
    "allow_vote_protect": True,

    "other_group_roles": False,
    "other_skip_day_vote": False,
    "other_skip_night_vote": False,
    "other_media_messages": True,
    "other_show_roles_in_reg": False,
    "other_advokat_mafia": False,
    "other_don_vote": False,
    "other_anonymous_vote": False,
    "other_leave_cmd": True,
    "other_show_emoji": True,
    "other_action_announces": True,

    "other_anyone_can_reg": False,
    "other_anyone_can_start": False,

    "give_games": 0,
    "give_diamonds": 0,
    "give_mask": 0,
    "give_gun": 0,
    "give_docs": 0,
    "give_protect": 0,
    "give_anti_killer": 0,
    "give_vote_protect": 0,
    "give_min_games_7d": 0,
    "language": "uz",

    "max_players": 30,
    "mafia_ratio": "1/3",

    "disabled_roles": [],
}


TIME_LABELS: Dict[str, str] = {
    "registration_seconds": "Ro'yxatdan o'tish",
    "night_seconds": "Tun",
    "day_seconds": "Kun",
    "vote_seconds": "Mafia/pages.vote",
    "confirm_seconds": "Tasdiqlash",
    "lastword_seconds": "So'nggi so'zni aytish vaqti",
    "afsongar_seconds": "Afsungar",
}


def time_main_kb(chat_id: int) -> InlineKeyboardMarkup:
    keys = [
        "registration_seconds",
        "night_seconds",
        "day_seconds",
        "vote_seconds",
        "confirm_seconds",
        "lastword_seconds",
        "afsongar_seconds",
    ]
    rows: List[List[InlineKeyboardButton]] = []
    for k in keys:
        rows.append([InlineKeyboardButton(text=TIME_LABELS.get(k, k), callback_data=f"time:open:{chat_id}:{k}")])
    rows.append([InlineKeyboardButton(text="orqaga", callback_data=f"adminset:back:{chat_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def time_preset_kb(chat_id: int, key: str) -> InlineKeyboardMarkup:
    g = get_group(chat_id)
    cur = int(g.get(key, 0))
    presets = [30, 45, 60, 75, 90, 120, 180, 240, 300, 360]
    rows: List[List[InlineKeyboardButton]] = []
    for i in range(0, len(presets), 2):
        a = presets[i]
        b = presets[i + 1]
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{a} Sekund {'■' if cur == a else '□'}",
                    callback_data=f"time:set:{chat_id}:{key}:{a}",
                ),
                InlineKeyboardButton(
                    text=f"{b} Sekund {'■' if cur == b else '□'}",
                    callback_data=f"time:set:{chat_id}:{key}:{b}",
                ),
            ]
        )
    rows.append([InlineKeyboardButton(text="orqaga", callback_data=f"adminset:times:{chat_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def weapons_main_kb(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📁 Hujjatlar", callback_data=f"weap:open:{chat_id}:allow_fake_docs")],
            [InlineKeyboardButton(text="🛡 Himoya", callback_data=f"weap:open:{chat_id}:allow_protect")],
            [InlineKeyboardButton(text="🎭 Maska", callback_data=f"weap:open:{chat_id}:allow_mask")],
            [InlineKeyboardButton(text="🔫 Miltiq", callback_data=f"weap:open:{chat_id}:allow_gun")],
            [InlineKeyboardButton(text="⛑️ Qotildan himoya", callback_data=f"weap:open:{chat_id}:allow_anti_killer")],
            [InlineKeyboardButton(text="⚖️ Ovoz berishni himoya qilish", callback_data=f"weap:open:{chat_id}:allow_vote_protect")],
            [InlineKeyboardButton(text="orqaga", callback_data=f"adminset:back:{chat_id}")],
        ]
    )


def yesno_kb(back_cb: str, yes_cb: str, no_cb: str, cur: bool) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text=f"Xa {'■' if cur else '□'}", callback_data=yes_cb),
                InlineKeyboardButton(text=f"Yo'q {'■' if not cur else '□'}", callback_data=no_cb),
            ],
            [InlineKeyboardButton(text="orqaga", callback_data=back_cb)],
        ]
    )


def other_main_kb(chat_id: int) -> InlineKeyboardMarkup:
    g = get_group(chat_id)

    def onoff(key: str, default: bool = False) -> str:
        return "🟢" if bool(g.get(key, default)) else "🔴"

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"{onoff('other_group_roles')} Rollarni guruhlash", callback_data=f"other:open:{chat_id}:other_group_roles")],
            [InlineKeyboardButton(text=f"{onoff('other_skip_day_vote')} 🚫 Navbatni o'tkazib yuborish (kunduzgi ovoz berish)", callback_data=f"other:open:{chat_id}:other_skip_day_vote")],
            [InlineKeyboardButton(text=f"{onoff('other_skip_night_vote')} 🚫 Burilishni o'tkazib yuborish (tungi)", callback_data=f"other:open:{chat_id}:other_skip_night_vote")],
            [InlineKeyboardButton(text=f"{onoff('other_action_announces', True)} Tun harakatlari xabarlari", callback_data=f"other:open:{chat_id}:other_action_announces")],
            [InlineKeyboardButton(text=f"{onoff('other_media_messages', True)} Media xabarlar", callback_data=f"other:open:{chat_id}:other_media_messages")],
            [InlineKeyboardButton(text=f"{onoff('other_show_roles_in_reg')} Ro'yxatdan o'tish paytida rollar", callback_data=f"other:open:{chat_id}:other_show_roles_in_reg")],
            [InlineKeyboardButton(text="Mafiya soni", callback_data=f"rolecfg:open:ratio:{chat_id}")],
            [InlineKeyboardButton(text=f"{onoff('other_advokat_mafia')} Advokat-Mafiya", callback_data=f"other:open:{chat_id}:other_advokat_mafia")],
            [InlineKeyboardButton(text=f"{onoff('other_don_vote')} Mafia Don ovoz berish", callback_data=f"other:open:{chat_id}:other_don_vote")],
            [InlineKeyboardButton(text="O'yinchilar soni", callback_data=f"rolecfg:open:max:{chat_id}")],
            [InlineKeyboardButton(text=f"{onoff('pin_registration', True)} Auto pin", callback_data=f"other:open:{chat_id}:pin_registration")],
            [InlineKeyboardButton(text=f"{onoff('other_anonymous_vote')} Anonim ovoz berish", callback_data=f"other:open:{chat_id}:other_anonymous_vote")],
            [InlineKeyboardButton(text=f"{onoff('other_anyone_can_reg')} Barcha foydalanuvchilar ro'yxatdan o'tishni boshlashi", callback_data=f"other:open:{chat_id}:other_anyone_can_reg")],
            [InlineKeyboardButton(text=f"{onoff('other_anyone_can_start')} Barcha foydalanuvchilar o'yinni boshlashi", callback_data=f"other:open:{chat_id}:other_anyone_can_start")],
            [InlineKeyboardButton(text="Ro'yxatdan o'tish", callback_data=f"other:do:{chat_id}:reg")],
            [InlineKeyboardButton(text="O'yinni boshlash", callback_data=f"other:do:{chat_id}:start")],
            [InlineKeyboardButton(text="Ro'yxatdan o'tishni uzaytiring", callback_data=f"other:do:{chat_id}:extend")],
            [InlineKeyboardButton(text="O'yinni qoldirish", callback_data=f"other:do:{chat_id}:pause")],
            [InlineKeyboardButton(text=f"{onoff('other_leave_cmd', True)} Buyruq /leave ?", callback_data=f"other:open:{chat_id}:other_leave_cmd")],
            [InlineKeyboardButton(text=f"{onoff('other_show_emoji', True)} Pokazivat emoji?", callback_data=f"other:open:{chat_id}:other_show_emoji")],
            [InlineKeyboardButton(text="orqaga", callback_data=f"adminset:back:{chat_id}")],
        ]
    )


def giveaway_kb(chat_id: int) -> InlineKeyboardMarkup:
    g = get_group(chat_id)
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=f"Min o'yin (7 kun): {int(g.get('give_min_games_7d', 0))}",
                    callback_data=f"givecfg:open:{chat_id}:min7",
                )
            ],
            [InlineKeyboardButton(text=f"🎲 Kolichestvo igr ({int(g.get('give_games', 0))} oyun)", callback_data=f"givecfg:open:{chat_id}:give_games")],
            [InlineKeyboardButton(text=f"💎 ({int(g.get('give_diamonds', 0))} add)", callback_data=f"givecfg:open:{chat_id}:give_diamonds")],
            [InlineKeyboardButton(text=f"🎭 Maska ({int(g.get('give_mask', 0))} adad)", callback_data=f"givecfg:open:{chat_id}:give_mask")],
            [InlineKeyboardButton(text=f"🔫 Miltiq ({int(g.get('give_gun', 0))} adad)", callback_data=f"givecfg:open:{chat_id}:give_gun")],
            [InlineKeyboardButton(text=f"📁 Hujjatlar ({int(g.get('give_docs', 0))} adad)", callback_data=f"givecfg:open:{chat_id}:give_docs")],
            [InlineKeyboardButton(text=f"🛡 Himoya ({int(g.get('give_protect', 0))} adad)", callback_data=f"givecfg:open:{chat_id}:give_protect")],
            [InlineKeyboardButton(text=f"⛑️ Qotildan himoya ({int(g.get('give_anti_killer', 0))} adad)", callback_data=f"givecfg:open:{chat_id}:give_anti_killer")],
            [InlineKeyboardButton(text=f"⚖️ Ovoz himoyasi ({int(g.get('give_vote_protect', 0))} adad)", callback_data=f"givecfg:open:{chat_id}:give_vote_protect")],
            [InlineKeyboardButton(text="orqaga", callback_data=f"adminset:back:{chat_id}")],
        ]
    )


def giveaway_min_games_kb(chat_id: int) -> InlineKeyboardMarkup:
    g = get_group(chat_id)
    cur = int(g.get("give_min_games_7d", 0))
    vals = [0, 1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50]
    rows: List[List[InlineKeyboardButton]] = []
    for i in range(0, len(vals), 3):
        chunk = vals[i : i + 3]
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{v} {'■' if cur == v else '□'}",
                    callback_data=f"givecfg:set:{chat_id}:min7:{v}",
                )
                for v in chunk
            ]
        )
    rows.append([InlineKeyboardButton(text="orqaga", callback_data=f"adminset:giveaway:{chat_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def giveaway_item_kb(chat_id: int, key: str, values: List[int]) -> InlineKeyboardMarkup:
    g = get_group(chat_id)
    cur = int(g.get(key, 0))
    rows: List[List[InlineKeyboardButton]] = []
    for i in range(0, len(values), 3):
        chunk = values[i : i + 3]
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{v} {'■' if cur == v else '□'}",
                    callback_data=f"givecfg:set:{chat_id}:{key}:{v}",
                )
                for v in chunk
            ]
        )
    rows.append([InlineKeyboardButton(text="orqaga", callback_data=f"adminset:giveaway:{chat_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def load_users() -> Dict[str, Any]:
    _ensure_dir(USERS_DIR)
    users = {}
    try:
        for filename in os.listdir(USERS_DIR):
            if filename.endswith(".json"):
                uid = filename[:-5]  # Remove .json
                user_path = os.path.join(USERS_DIR, filename)
                users[uid] = _load_json_file(user_path)
    except Exception:
        pass
    return users


def save_users(users: Dict[str, Any]) -> None:
    _ensure_dir(USERS_DIR)
    for uid, user_data in users.items():
        user_path = os.path.join(USERS_DIR, f"{uid}.json")
        _save_json_file(user_path, user_data)


def load_groups() -> Dict[str, Any]:
    _ensure_dir(GROUPS_DIR)
    groups = {}
    try:
        for filename in os.listdir(GROUPS_DIR):
            if filename.endswith(".json"):
                gid = filename[:-5]  # Remove .json
                group_path = os.path.join(GROUPS_DIR, filename)
                groups[gid] = _load_json_file(group_path)
    except Exception:
        pass
    return groups


def save_groups(groups: Dict[str, Any]) -> None:
    _ensure_dir(GROUPS_DIR)
    for gid, group_data in groups.items():
        group_path = os.path.join(GROUPS_DIR, f"{gid}.json")
        _save_json_file(group_path, group_data)


def get_user(uid: int, name: str) -> Dict[str, Any]:
    suid = str(uid)
    user_path = os.path.join(USERS_DIR, f"{suid}.json")
    u = _load_json_file(user_path)
    
    if not u:  # New user
        u = DEFAULT_USER.copy()
        u["name"] = name or "User"
        u["id"] = uid
        _save_json_file(user_path, u)
        return u

    # Update existing user
    changed = False
    for k, v in DEFAULT_USER.items():
        if k not in u:
            u[k] = v
            changed = True
    # Update name and last_seen
    if name and u.get("name") != name:
        u["name"] = name
        changed = True
    u["last_seen"] = asyncio.get_event_loop().time()
    if "id" not in u:
        u["id"] = uid
        changed = True
    if changed:
        _save_json_file(user_path, u)
    return u


def update_user(uid: int, user_data: Dict[str, Any]) -> None:
    suid = str(uid)
    user_path = os.path.join(USERS_DIR, f"{suid}.json")
    _save_json_file(user_path, user_data)


def _top_n_from_text(txt: str, default_n: int = 10) -> int:
    try:
        s = (txt or "").strip()
        s = s.split("@", 1)[0]
        parts = s.split()
        cmd = parts[0] if parts else ""
        n: Optional[int] = None

        # /top10 /top20 /top30
        if cmd.startswith("/top") and len(cmd) > 4:
            tail = cmd[4:]
            if tail.isdigit():
                n = int(tail)

        # /top 20
        if n is None and len(parts) >= 2 and str(parts[1]).isdigit():
            n = int(parts[1])

        if n is None:
            n = int(default_n)
        n = max(1, min(30, int(n)))
        return n
    except Exception:
        return int(default_n)


def update_user(uid: int, user_data: Dict[str, Any]) -> None:
    suid = str(uid)
    user_path = os.path.join(USERS_DIR, f"{suid}.json")
    _save_json_file(user_path, user_data)


def get_group(gid: int, title: str = None) -> Dict[str, Any]:
    sgid = str(gid)
    group_path = os.path.join(GROUPS_DIR, f"{sgid}.json")
    g = _load_json_file(group_path)
    
    if not g:  # New group
        g = DEFAULT_GROUP.copy()
        g["title"] = title or f"Group {gid}"
        g["id"] = gid
        _save_json_file(group_path, g)
        return g

    # Update existing group
    changed = False
    for k, v in DEFAULT_GROUP.items():
        if k not in g:
            g[k] = v
            changed = True
    # Update title if provided
    if title and g.get("title") != title:
        g["title"] = title
        changed = True
    if "id" not in g:
        g["id"] = gid
        changed = True
    if changed:
        _save_json_file(group_path, g)
    return g


def update_group(gid: int, group_data: Dict[str, Any]) -> None:
    sgid = str(gid)
    group_path = os.path.join(GROUPS_DIR, f"{sgid}.json")
    _save_json_file(group_path, group_data)


def fmt_money(n: int) -> str:
    try:
        return f"{int(n):,}".replace(",", " ")
    except Exception:
        return str(n)


async def safe_dm(
    user_id: int,
    text: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    parse_mode: Optional[str] = None,
) -> bool:
    try:
        await bot.send_message(user_id, text, reply_markup=reply_markup, parse_mode=parse_mode)
        return True
    except Exception:
        return False


@dp.callback_query(F.data.startswith("grouplang:set:"))
async def cb_grouplang_set(cb: CallbackQuery) -> None:
    parts = cb.data.split(":")
    if len(parts) != 4:
        return await cb.answer()
    chat_id = int(parts[2])
    code = parts[3]
    if code not in {"uz", "ru", "az", "tr"}:
        return await cb.answer("❌", show_alert=True)
    if not await is_admin(chat_id, cb.from_user.id):
        return await cb.answer("❌ Faqat admin", show_alert=True)
    g = get_group(chat_id)
    g["language"] = code
    update_group(chat_id, g)
    await cb.answer("✅")
    try:
        await cb.message.edit_reply_markup(reply_markup=group_lang_kb(chat_id))
    except Exception:
        pass


async def try_dm(
    user_id: int,
    text: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    parse_mode: Optional[str] = None,
) -> bool:
    try:
        await bot.send_message(user_id, text, reply_markup=reply_markup, parse_mode=parse_mode)
        return True
    except Exception:
        return False


async def _admin_open_registration(chat_id: int, admin_id: int) -> None:
    if not await is_admin(chat_id, admin_id):
        await try_dm(admin_id, "❌ Faqat admin")
        return

    if not await bot_has_required_rights(chat_id):
        try:
            await bot.send_message(chat_id, bot_rights_text())
        except Exception:
            pass
        return

    game = get_game(chat_id)
    if game.get("phase") == PHASE_REG:
        kb = await reg_join_kb(chat_id)
        try:
            await _cleanup_reg_announces(chat_id)
            old_mid = game.get("reg_message_id")
            if old_mid:
                try:
                    await _safe_delete_message(chat_id, int(old_mid))
                except Exception:
                    pass
            reg_msg = await bot.send_message(chat_id, _group_status_text(chat_id), reply_markup=kb, parse_mode="HTML")
            game["reg_message_id"] = reg_msg.message_id
            if bool(get_group(chat_id).get("pin_registration", True)):
                await _safe_pin_message(chat_id, reg_msg.message_id)
        except Exception:
            pass

        t = game.get("reg_task")
        if t:
            try:
                t.cancel()
            except Exception:
                pass
        game["reg_task"] = asyncio.create_task(_registration_timeout(chat_id, float(game.get("reg_deadline", 0))))
        await try_dm(admin_id, "🔁 Ro'yxat posti yangilandi")
        return

    g = get_group(chat_id)
    game["started"] = False
    game["phase"] = PHASE_REG
    game["players"].clear()
    game["roles"].clear()
    game["alive"].clear()
    game["teams"] = None
    game["reg_deadline"] = asyncio.get_event_loop().time() + int(g.get("registration_seconds", 60))

    kb = await reg_join_kb(chat_id)
    try:
        reg_msg = await bot.send_message(chat_id, _group_status_text(chat_id), reply_markup=kb, parse_mode="HTML")
        game["reg_message_id"] = reg_msg.message_id
        if bool(get_group(chat_id).get("pin_registration", True)):
            await _safe_pin_message(chat_id, reg_msg.message_id)
    except Exception:
        pass

    try:
        _runtime_set(
            chat_id,
            {
                "phase": "reg",
                "reg_message_id": game.get("reg_message_id"),
                "reg_deadline": float(game.get("reg_deadline", 0.0) or 0.0),
            },
        )
    except Exception:
        pass

    t = game.get("reg_task")
    if t:
        try:
            t.cancel()
        except Exception:
            pass
    game["reg_task"] = asyncio.create_task(_registration_timeout(chat_id, float(game.get("reg_deadline", 0))))

    await try_dm(admin_id, f"✅ Guruh {chat_id} da ro'yxat boshlandi!")


async def _admin_start_game(chat_id: int, admin_id: int) -> None:
    if not await is_admin(chat_id, admin_id):
        await try_dm(admin_id, "❌ Faqat admin")
        return
    if not await bot_has_required_rights(chat_id):
        try:
            await bot.send_message(chat_id, bot_rights_text())
        except Exception:
            pass
        return

    game = get_game(chat_id)
    if game.get("phase") != PHASE_REG or game.get("started"):
        await try_dm(admin_id, "❌ Avval ro'yxatni oching")
        return
    if len(game.get("players", {})) < 4:
        await try_dm(admin_id, "❌ Kamida 4 o'yinchi kerak")
        return

    # Ensure DM is available for all players; drop those who cannot be reached.
    dropped: List[str] = []
    for uid, nm in list(game.get("players", {}).items()):
        ok = False
        try:
            ok = await safe_dm(int(uid), "✅")
        except Exception:
            ok = False
        if not ok:
            game["players"].pop(str(uid), None)
            game.get("roles", {}).pop(str(uid), None)
            if str(uid) in game.get("alive", []):
                try:
                    game["alive"].remove(str(uid))
                except Exception:
                    pass
            dropped.append(str(nm or "User"))

    if dropped:
        try:
            await bot.send_message(
                chat_id,
                "⚠️ DM yopiq bo'lgani uchun ro'yxatdan chiqarildi:\n" + "\n".join(f"- {html.escape(n)}" for n in dropped),
                parse_mode="HTML",
            )
        except Exception:
            pass

    if len(game.get("players", {})) < 4:
        await try_dm(admin_id, "❌ Kamida 4 o'yinchi kerak")
        return

    players = list(game["players"].keys())
    roles = assign_roles(len(players), chat_id)
    for uid, role in zip(players, roles):
        game["roles"][uid] = role

    game["started"] = True
    game["phase"] = PHASE_NIGHT
    game["round"] = 1
    game["started_ts"] = asyncio.get_event_loop().time()
    game["alive"] = list(game["players"].keys())
    game["participants"] = list(game.get("players", {}).keys())

    rt = game.get("reg_task")
    if rt:
        try:
            rt.cancel()
        except Exception:
            pass
    game["reg_task"] = None

    for uid, role in game["roles"].items():
        desc = ROLE_DESC_UZ.get(role, "")
        role_text = f"🎭 Sizning roliz: {ROLE_EMOJI.get(role,'🎭')} {role}" + (f"\n\n{desc}" if desc else "")
        if group_url(chat_id):
            await safe_dm(
                int(uid),
                role_text,
                reply_markup=group_open_kb(chat_id),
            )
        else:
            await safe_dm(int(uid), role_text)

    try:
        mid = game.get("reg_message_id")
        if mid:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=int(mid),
                text="O'yin boshlandi!",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[[InlineKeyboardButton(text="Sizning rolingiz", callback_data="menu:profile")]]
                ),
            )
    except Exception:
        pass

    alive = _alive_ids(game)
    alive_lines = "\n".join(f"{i+1}. {_player_link(game, uid)}" for i, uid in enumerate(alive))
    try:
        me = await bot.get_me()
        await bot.send_message(
            chat_id,
            (
                "Tirik o'yinchilar:\n"
                f"{alive_lines}\n\n"
                f"Ulardan: {_role_counts_line(game)}\n"
                f"Jami: {len(alive)}"
            ),
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="Bot-ga o'tish", url=f"https://t.me/{me.username}")]]
            ),
            parse_mode="HTML",
        )
    except Exception:
        pass

    t = game.get("loop_task")
    if t:
        try:
            t.cancel()
        except Exception:
            pass
    game["loop_task"] = asyncio.create_task(_game_loop(chat_id))
    await apply_silence_rules(chat_id)


async def _admin_extend_reg(chat_id: int, admin_id: int) -> None:
    if not await is_admin(chat_id, admin_id):
        await try_dm(admin_id, "❌ Faqat admin")
        return
    game = get_game(chat_id)
    if game.get("phase") != PHASE_REG:
        await try_dm(admin_id, "❌ Faqat ro'yxat vaqtini uzaytirish mumkin")
        return
    g = get_group(chat_id)
    game["reg_deadline"] = asyncio.get_event_loop().time() + int(g.get("registration_seconds", 60))
    await try_dm(admin_id, "✅ Ro'yxat vaqti uzaytirildi")
    try:
        await _edit_group_status(
            chat_id,
            reply_markup=await reg_join_kb(chat_id),
        )
    except Exception:
        pass


async def _admin_stop_game(chat_id: int, admin_id: int) -> None:
    if not await is_admin(chat_id, admin_id):
        await try_dm(admin_id, "❌ Faqat admin")
        return
    game = get_game(chat_id)
    t = game.get("loop_task")
    if t:
        try:
            t.cancel()
        except Exception:
            pass
    game["loop_task"] = None
    game["started"] = False
    game["phase"] = PHASE_IDLE

    rt = game.get("reg_task")
    if rt:
        try:
            rt.cancel()
        except Exception:
            pass
    game["reg_task"] = None
    try:
        await _edit_group_status(chat_id, reply_markup=None)
    except Exception:
        pass
    await clear_silence(chat_id)
    await try_dm(admin_id, "✅ O'yin to'xtatildi")


async def is_admin(chat_id: int, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        return member.status in {"administrator", "creator"}
    except Exception:
        return False


async def _safe_delete_message(chat_id: int, message_id: int) -> None:
    try:
        await bot.delete_message(chat_id, message_id)
    except Exception:
        return


async def _safe_pin_message(chat_id: int, message_id: int) -> None:
    try:
        await bot.pin_chat_message(chat_id, message_id, disable_notification=True)
    except Exception:
        return


async def _restrict_user(chat_id: int, user_id: int, can_send: bool) -> None:
    try:
        perms = ChatPermissions(
            can_send_messages=can_send,
            can_send_audios=can_send,
            can_send_documents=can_send,
            can_send_photos=can_send,
            can_send_videos=can_send,
            can_send_video_notes=can_send,
            can_send_voice_notes=can_send,
            can_send_polls=can_send,
            can_send_other_messages=can_send,
            can_add_web_page_previews=can_send,
        )
        await bot.restrict_chat_member(chat_id, user_id, permissions=perms)
    except Exception:
        return


async def apply_silence_rules(chat_id: int) -> None:
    game = get_game(chat_id)
    if not game.get("started"):
        return
    g = get_group(chat_id)
    if not await bot_has_required_rights(chat_id):
        return

    silence_night = bool(g.get("silence_night", False))
    silence_dead = bool(g.get("silence_dead", False))
    phase = str(game.get("phase"))

    alive = set(_alive_ids(game))
    all_players = list(game.get("players", {}).keys())
    for uid in all_players:
        is_alive = uid in alive
        can_send = True
        if silence_dead and not is_alive:
            can_send = False
        if silence_night and phase == PHASE_NIGHT and is_alive:
            can_send = False
        await _restrict_user(chat_id, int(uid), can_send)


async def clear_silence(chat_id: int) -> None:
    game = get_game(chat_id)
    if not await bot_has_required_rights(chat_id):
        return
    for uid in list(game.get("players", {}).keys()):
        await _restrict_user(chat_id, int(uid), True)
    for uid in list(game.get("restricted_extra", set())):
        await _restrict_user(chat_id, int(uid), True)
    game["restricted_extra"] = set()


async def bot_has_required_rights(chat_id: int) -> bool:
    try:
        me = await bot.get_me()
        member = await bot.get_chat_member(chat_id, me.id)
        if member.status not in {"administrator", "creator"}:
            return False
        # aiogram may not expose all fields if not administrator
        perms = getattr(member, "can_delete_messages", None)
        can_delete = bool(getattr(member, "can_delete_messages", False))
        can_restrict = bool(getattr(member, "can_restrict_members", False))
        can_pin = bool(getattr(member, "can_pin_messages", False))
        # If permission fields aren't present, assume not granted
        if perms is None:
            return False
        return can_delete and can_restrict and can_pin
    except Exception:
        return False


def bot_rights_text() -> str:
    return (
        "Salom!\n"
        "Men 🤵🏻Mafia o'yini rasmiy botiman.\n\n"
        "Salom! Men Mafia o'yini rasmiy botiman:\n"
        "☑️ Xabarlarn o'chirish\n"
        "☑️ O'yinchilarni bloklash\n"
        "☑️ Xabarlarni pin qilish"
    )


@dp.my_chat_member()
async def on_my_chat_member(update: ChatMemberUpdated) -> None:
    chat = update.chat
    if chat.type not in {"group", "supergroup"}:
        return

    # When bot is added / rights changed
    try:
        if not await bot_has_required_rights(chat.id):
            await bot.send_message(chat.id, bot_rights_text())
    except Exception:
        return


def group_url(chat_id: int) -> Optional[str]:
    sid = str(chat_id)
    if sid.startswith("-100"):
        return f"https://t.me/c/{sid[4:]}"
    return None


def group_open_kb(chat_id: int) -> InlineKeyboardMarkup:
    url = group_url(chat_id)
    if not url:
        return InlineKeyboardMarkup(inline_keyboard=[])
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Guruhga o'tish", url=url)]])


def _disabled_roles_set(chat_id: int) -> set:
    g = get_group(chat_id)
    raw = g.get("disabled_roles", [])
    if not isinstance(raw, list):
        return set()
    return {str(r) for r in raw}


def _role_enabled(chat_id: int, role: str) -> bool:
    if role in {"Tinch", "Don", "Mafia"}:
        return True
    return role not in _disabled_roles_set(chat_id)


def _toggle_role(chat_id: int, role: str) -> None:
    if role in {"Tinch", "Don", "Mafia"}:
        return
    g = get_group(chat_id)
    cur = _disabled_roles_set(chat_id)
    if role in cur:
        cur.remove(role)
    else:
        cur.add(role)
    g["disabled_roles"] = sorted(cur)
    update_group(chat_id, g)


def with_group_button(chat_id: int, kb: InlineKeyboardMarkup) -> InlineKeyboardMarkup:
    url = group_url(chat_id)
    if not url:
        return kb
    rows = list(kb.inline_keyboard) if kb and kb.inline_keyboard else []
    rows.append([InlineKeyboardButton(text="Guruhga o'tish", url=url)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def bot_url() -> Optional[str]:
    try:
        if BOT_USERNAME:
            return f"https://t.me/{BOT_USERNAME}"
    except Exception:
        pass
    return None


def bot_open_kb() -> InlineKeyboardMarkup:
    url = bot_url()
    if not url:
        return InlineKeyboardMarkup(inline_keyboard=[])
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Bot-ga o'tish", url=url)]])


def admin_manual_pay_kb(back_cb: str = "menu:profile") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Admin-ga yozish", url=f"tg://user?id={ADMIN_MANUAL_PAY_ID}")],
            [InlineKeyboardButton(text="⬅️ Orqaga", callback_data=back_cb)],
        ]
    )


SHOP_ITEMS: Dict[str, Dict[str, Any]] = {
    "protect": {"title": "🛡 Himoya", "currency": "money", "price": 120, "field": "protect", "qty": 1},
    "vote_protect": {"title": "⚖️ Ovoz himoyasi", "currency": "money", "price": 150, "field": "vote_protect", "qty": 1},
    "mask": {"title": "🎭 Maska", "currency": "money", "price": 90, "field": "mask", "qty": 1},
    "fake_docs": {"title": "📁 Soxta hujjat", "currency": "money", "price": 110, "field": "fake_docs", "qty": 1},
    "anti_killer": {"title": "⛑️ Qotildan himoya", "currency": "diamonds", "price": 3, "field": "anti_killer", "qty": 1},
    "gun": {"title": "🔫 Miltiq", "currency": "diamonds", "price": 5, "field": "gun", "qty": 1},
}

ROLE_EMOJI = {
    "Tinch": "👨🏼",
    "Kezuvchi": "💃",
    "Serjant": "👮🏻‍♂",
    "Komissar": "🕵🏻‍♂",
    "Doktor": "👨🏻‍⚕",
    "Daydi": "🧙‍♂",
    "Afsungar": "🧞‍♂️",
    "Don": "🤵🏻",
    "Mafia": "🤵🏼",
    "Advokat": "👨‍💼",
    "Qotil": "🔪",
    "Suidsid": "🤦🏼",
    "Omadli": "🤞🏼",
    "G'azabkor": "🧟",
    "Jurnalist": "📰",
    "Sehrgar": "🧙‍♀️",
    "Janob": "🤵🏻‍♂️",
    "Sotqin": "🕳️",
    "Aferist": "🃏",
    "убийца": "🔪",
    "Bo'ri": "🐺",
}

ROLES_LIST = [
    "Tinch",
    "Kezuvchi",
    "Serjant",
    "Komissar",
    "Doktor",
    "Daydi",
    "Afsungar",
    "Don",
    "Mafia",
    "Advokat",
    "Qotil",
    "Suidsid",
    "Omadli",
    "G'azabkor",
    "Jurnalist",
    "Sehrgar",
    "Janob",
    "Sotqin",
    "Aferist",
    "убийца",
    "Bo'ri",
]

ROLE_DESC_UZ = {
    "Komissar": (
        "🔴 🕵🏻‍♂ Komissar Kattanining maqsadi - shaharning asosiy himoyachisi va mafiya kushandasi. "
        "Sizning vazifangiz - mafiyani topish va ovoz berish paytida ularni osish."
    ),
    "Tinch": (
        "🔴 👨🏼 Tinch axolini maqsadi - Sizning vazifangiz mafiyani topish va ularni shahar yig'ilishida osishdur."
    ),
    "Kezuvchi": (
        "🔴 💃 Kezuvchi mashuqani maqsadi - Sizning vazifangiz bu qattiq dunyoda omon qolishdir. "
        "Bir kecha davomida shaharni zararsizlantirish uchun mahoratingizdan foydalaning. "
        "Kezuvchi Ma'shuqa qasddan Komissar bilan birga bo'lmasligi kerak, komisarni uxlatmasligi kerak."
    ),
    "Serjant": (
        "🔴 👮🏻‍♂ Serjantni maqsadi - Sizning vazifangiz 🕵🏻‍♂ Komissar Kattaniga yordam berish. "
        "U sizni o'z harakatlaringiz to'g'risida xabardor qiladi va sizni voqealar to'g'risida xabardor qiladi. "
        "Agar komissar vafot etsa, uning o'rnini egallaysiz."
    ),
    "Doktor": (
        "🔴 👨🏻‍⚕ Doktorning maqsadi - Sizning vazifangiz - 🕵🏻‍♂ Komissar o'zini e'lon qilganidan keyin Komissarni davolash. "
        "Agar kerak bo'lsa, 👨🏻‍⚕ Doktor o'zini faqat bir marta davolay oladi."
    ),
    "Daydi": (
        "🔴 🧙‍♂ Daydini maqsadi - Sizning vazifangiz 🌃 tunda shisha butilka uchun har qanday o'yinchi oldiga borish va "
        "qotillikka guvoh bo'lishdir."
    ),
    "Don": (
        "🔴 🤵🏻 Don va 🤵🏼 Mafiyaning maqsadi - Ularning vazifasi shu 🌃 tunda kimni uyg'onmasligini hal qilish, "
        "ya'ni o'ldirish ..."
    ),
    "Mafia": (
        "🔴 🤵🏻 Don va 🤵🏼 Mafiyaning maqsadi - Ularning vazifasi shu 🌃 tunda kimni uyg'onmasligini hal qilish, "
        "ya'ni o'ldirish ..."
    ),
    "Advokat": (
        "🔴 👨‍💼 Advokatning maqsadi - Sizning vazifangiz 🌃 tunda kimni himoya qilishni tanlashdir. "
        "Agar siz Mafiyani tanlasangiz, u holda Komissar Kattani uni taniy olmaydi va 👨🏼 Tinch axoli ro'lini namoyish etadi. "
        "Sizning vazifangiz Mafiya g'alaba qozonishi."
    ),
    "Qotil": (
        "🔴 🔪 Qotilning maqsadi - Sizning vazifangiz atrofdagilarni o'ldirishdir. Bir o'zingiz qolsangiz g'olib bo'lasiz."
    ),
    "Suidsid": (
        "🔴 🤦🏼 Suicid – agar osib o'ldirilsa, yutadi."
    ),
    "Omadli": (
        "🔴 🤞🏼 Omadli – oddiy tinch aholi, lekin unga hujum bo‘lsa, omad kelsa tirik qolishi mumkin. Vazifasi mafiyaga qarshi kurashish."
    ),
    "G'azabkor": (
        "🔴 🧟 G‘azabkor – har tun odam tanlaydi, 3 ta tanlovdan keyin ularni o‘zi bilan olib ketadi."
    ),
    "Jurnalist": (
        "🔴 📰 Jurnalist – mafiyaga ishlaydi. Har tun kimnidir tekshiradi va ma’lumotni mafiyaga yetkazadi."
    ),
    "Sehrgar": (
        "🔴 🧙‍♂️ Sehrgar – o‘z qonunlari bilan yashaydi. Uni o‘ldirmoqchi bo‘lishsa, o‘zi qaror qiladi: kechirish yoki o‘ldirish."
    ),
    "Janob": (
        "🔴 🎖 Janob – kunduzgi ovoz berishda ovozi 2 taga teng."
    ),
    "Sotqin": (
        "🔴 🤓 Sotqin – komissar tekshirgan rolni bilib olib, hammaga aytadi."
    ),
    "Aferist": (
        "🔴 🤹🏻 Aferist – boshqa odam nomidan ovoz berishi mumkin. Maqsadi – tirik qolish."
    ),
    "убийца": (
        "🔴 🕴️ Uddaburon (qotil) – mafiyaning asosiy quroli, istagan odamni o‘ldira oladi."
    ),
    "Afsungar": (
        "🔴 🧞‍♂️ Afsungarning maqsadi - Juda xavfli o'yinchi. Agar kimdir uni 🌃 tunda o'ldirmoqchi bo'lsa, "
        "Afsungar o'yinni tark etayotganda o'ldirganni o'zi bilan olib ketadi. "
        "Agar u 🌇 kun davomida ovoz berishda o'ldirilsa, Afsungar o'zi xohlagan bitta o'yinchini o'ldiradi."
    ),
    "Bo'ri": (
        "🔴 🐺 Bo'rining maqsadi - reenkarnatsiya qilish. Komissar otsa Serjantga aylanadi. Don/Mafiya otsa Mafiyaga aylanadi. "
        "Qotil o'ldirsa - Bo'ri o'ladi."
    ),
}

PHASE_IDLE = "idle"
PHASE_REG = "registration"
PHASE_NIGHT = "night"
PHASE_DAY = "day"
PHASE_DISCUSSION = "discussion"
PHASE_LYNCH = "lynch"
PHASE_CONFIRM = "confirm"

GAMES: Dict[int, Dict[str, Any]] = defaultdict(
    lambda: {
        "phase": PHASE_IDLE,
        "started": False,
        "players": {},  # uid -> name
        "roles": {},  # uid -> role
        "alive": [],
        "teams": None,
        "reg_message_id": None,
        "reg_deadline": 0.0,
        "round": 0,
        "started_ts": 0.0,
        "loop_task": None,
        "deadline": 0.0,
        "blocked": set(),
        "restricted_extra": set(),
        "night": {
            "mafia_kill": None,
            "qotil_kill": None,
            "udda_kill": None,
            "doctor_save": None,
            "doctor_self_used": set(),
            "kom_check": None,
            "kom_shoot": None,
            "adv_protect": None,
            "kezuvchi_block": None,
            "daydi_visit": None,
            "journalist_check": None,
            "gazabkor_mark": {},
        },
        "votes": {},
        "confirm": {"target": None, "votes": {}},
        "afsungar_day_revenge": None,
    }
)

NIGHT_MEDIA = "https://images.unsplash.com/photo-1509822929063-6b6cfc9b42f2?auto=format&fit=crop&w=1280&q=70"
DAY_MEDIA = "https://images.unsplash.com/photo-1500530855697-b586d89ba3ee?auto=format&fit=crop&w=1280&q=70"

NIGHT_LOCAL_PATH = r"C:\Users\ANUBIS PC\Desktop\Mafia\night.jpg"
DAY_LOCAL_PATH = r"C:\Users\ANUBIS PC\Desktop\Mafia\day.jpg"


def _photo_input(local_path: str, fallback_url: str) -> Any:
    try:
        if os.path.exists(local_path):
            return FSInputFile(local_path)
    except Exception:
        pass
    return fallback_url


def get_game(chat_id: int) -> Dict[str, Any]:
    return GAMES[chat_id]


def _alive_ids(game: Dict[str, Any]) -> List[str]:
    return [uid for uid in game.get("alive", []) if uid in game.get("players", {})]


def _reset_night(game: Dict[str, Any]) -> None:
    game["blocked"] = set()
    game["night_announced"] = set()
    game["night_acted"] = set()
    game["night"].update(
        {
            "mafia_kill": None,
            "qotil_kill": None,
            "udda_kill": None,
            "doctor_save": None,
            "kom_check": None,
            "kom_shoot": None,
            "adv_protect": None,
            "kezuvchi_block": None,
            "daydi_visit": None,
            "journalist_check": None,
            "gazabkor_mark": {},
        }
    )

    # day-only transient state
    game.pop("aferist_used", None)
    game.pop("sehrgar_decision", None)
    game.pop("_suidsid_winner", None)
    game.pop("sleep_day", None)


async def _maybe_promote_don(chat_id: int, game: Dict[str, Any]) -> None:
    try:
        alive = _alive_ids(game)
        if any(_role_of(game, uid) == "Don" for uid in alive):
            return
        mafia_side = [
            uid
            for uid in alive
            if _role_of(game, uid) in {"Mafia", "Advokat", "Jurnalist", "убийца"}
        ]
        if not mafia_side:
            return

        # Prefer a plain Mafia to become Don, else anyone mafia-side
        pick = next((uid for uid in mafia_side if _role_of(game, uid) == "Mafia"), mafia_side[0])
        game["roles"][pick] = "Don"

        try:
            text = "🤵🏻 Don o'ldi. Siz endi Donsiz!"
            if group_url(chat_id):
                await safe_dm(int(pick), text, reply_markup=group_open_kb(chat_id))
            else:
                await safe_dm(int(pick), text)
        except Exception:
            pass

        # Notify mafia-side players
        try:
            for uid in mafia_side:
                if uid == pick:
                    continue
                msg = f"🤵🏻 Yangi Don: {_player_link(game, pick)}"
                if group_url(chat_id):
                    await safe_dm(int(uid), msg, reply_markup=group_open_kb(chat_id), parse_mode="HTML")
                else:
                    await safe_dm(int(uid), msg, parse_mode="HTML")
        except Exception:
            pass
    except Exception:
        return


async def _cleanup_reg_announces(chat_id: int) -> None:
    game = get_game(chat_id)
    ids_raw = game.get("reg_announce_ids")
    if not ids_raw:
        return
    try:
        ids = [int(x) for x in (ids_raw or []) if x]
    except Exception:
        ids = []
    game["reg_announce_ids"] = []
    for mid in ids:
        try:
            await _safe_delete_message(chat_id, mid)
        except Exception:
            pass


def _parse_extend_seconds(raw: str) -> Optional[int]:
    s = (raw or "").strip().lower()
    if not s:
        return None

    if ":" in s:
        parts = s.split(":")
        if len(parts) == 2:
            try:
                mm = int(parts[0])
                ss = int(parts[1])
            except Exception:
                return None
            if mm < 0 or ss < 0 or ss >= 60:
                return None
            return mm * 60 + ss

    total = 0
    num = ""
    had_unit = False
    for ch in s:
        if ch.isdigit():
            num += ch
            continue
        if ch in {"m", "s"}:
            if not num:
                return None
            v = int(num)
            num = ""
            had_unit = True
            if ch == "m":
                total += v * 60
            else:
                total += v
            continue
        return None

    if num:
        if had_unit:
            total += int(num)
        else:
            total = int(num)

    if total <= 0:
        return None
    return int(total)


async def _auto_start_game_from_registration(chat_id: int) -> bool:
    game = get_game(chat_id)
    if game.get("phase") != PHASE_REG or game.get("started"):
        return False
    if len(game.get("players", {}) or {}) < 4:
        return False

    # Ensure DM is available for all players; drop those who cannot be reached.
    dropped: List[str] = []
    for uid, nm in list((game.get("players", {}) or {}).items()):
        ok = False
        try:
            ok = await safe_dm(int(uid), "✅")
        except Exception:
            ok = False
        if not ok:
            game["players"].pop(str(uid), None)
            game.get("roles", {}).pop(str(uid), None)
            try:
                if str(uid) in game.get("alive", []):
                    game["alive"].remove(str(uid))
            except Exception:
                pass
            dropped.append(str(nm or "User"))

    if dropped:
        try:
            await bot.send_message(
                chat_id,
                "⚠️ DM yopiq bo'lgani uchun ro'yxatdan chiqarildi:\n" + "\n".join(f"- {html.escape(n)}" for n in dropped),
                parse_mode="HTML",
            )
        except Exception:
            pass

    if len(game.get("players", {}) or {}) < 4:
        return False

    try:
        await _cleanup_reg_announces(chat_id)
    except Exception:
        pass

    players = list((game.get("players", {}) or {}).keys())
    roles = assign_roles(len(players), chat_id)
    for uid, role in zip(players, roles):
        game.setdefault("roles", {})[uid] = role

    game["started"] = True
    game["phase"] = PHASE_NIGHT
    game["round"] = 1
    game["started_ts"] = asyncio.get_event_loop().time()
    game["alive"] = list(players)
    game["participants"] = list(players)
    game["reg_task"] = None

    # send roles
    for uid, role in game.get("roles", {}).items():
        try:
            desc = ROLE_DESC_UZ.get(role, "")
            mates = _teammates_block_html(game, uid)
            role_text = f"🎭 Sizning roliz: {ROLE_EMOJI.get(role,'🎭')} {role}" + (f"\n\n{desc}" if desc else "")
            if mates:
                role_text += f"\n\n{mates}"
            if group_url(chat_id):
                await safe_dm(
                    int(uid),
                    role_text,
                    reply_markup=group_open_kb(chat_id),
                    parse_mode="HTML",
                )
            else:
                await safe_dm(int(uid), role_text, parse_mode="HTML")
        except Exception:
            pass

    # edit registration post
    try:
        mid = game.get("reg_message_id")
        if mid:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=int(mid),
                text="O'yin boshlandi!",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[[InlineKeyboardButton(text="Sizning rolingiz", callback_data="menu:profile")]]
                ),
            )
    except Exception:
        pass

    # group alive list + role counts
    try:
        alive = _alive_ids(game)
        alive_lines = "\n".join(f"{i+1}. {_player_link(game, uid)}" for i, uid in enumerate(alive))
        await bot.send_message(
            chat_id,
            (
                "Tirik o'yinchilar:\n"
                f"{alive_lines}\n\n"
                f"Ulardan: {_role_counts_line(game)}\n"
                f"Jami: {len(alive)}"
            ),
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="Bot-ga o'tish", url=f"https://t.me/{(await bot.get_me()).username}")]]
            ),
            parse_mode="HTML",
        )
    except Exception:
        pass

    # start game loop
    try:
        t = game.get("loop_task")
        if t:
            try:
                t.cancel()
            except Exception:
                pass
        game["loop_task"] = asyncio.create_task(_game_loop(chat_id))
    except Exception:
        pass

    try:
        await apply_silence_rules(chat_id)
    except Exception:
        pass

    return True


async def _registration_timeout(chat_id: int, expected_deadline: float) -> None:
    try:
        last_tick = 0.0
        while True:
            game = get_game(chat_id)
            if game.get("phase") != PHASE_REG or game.get("started"):
                return
            now = asyncio.get_event_loop().time()
            left = float(game.get("reg_deadline", 0)) - now
            if left <= 0:
                break

            tick_every = 1.0 if left <= 10.0 else 5.0
            if now - last_tick >= tick_every:
                last_tick = now
                try:
                    await _edit_group_status(chat_id, reply_markup=await reg_join_kb(chat_id))
                except Exception:
                    pass
            await asyncio.sleep(min(1.0, max(0.1, left)))

        game = get_game(chat_id)
        if float(game.get("reg_deadline", 0)) != float(expected_deadline):
            return
        if game.get("phase") != PHASE_REG or game.get("started"):
            return

        try:
            started = await _auto_start_game_from_registration(chat_id)
            if started:
                return
        except Exception:
            pass

        mid = game.get("reg_message_id")
        try:
            if mid:
                await _safe_delete_message(chat_id, int(mid))
        except Exception:
            pass

        players_cnt = len(game.get("players", {}) or {})

        try:
            await _cleanup_reg_announces(chat_id)
        except Exception:
            pass

        game.update(
            {
                "phase": PHASE_IDLE,
                "started": False,
                "players": {},
                "roles": {},
                "alive": [],
                "teams": None,
                "reg_message_id": None,
                "reg_deadline": 0.0,
                "blocked": set(),
                "votes": {},
                "afsungar_day_revenge": None,
            }
        )
        game["reg_task"] = None
        _runtime_clear(chat_id)
        await clear_silence(chat_id)
        await _edit_group_status(chat_id, reply_markup=None)
        try:
            if players_cnt < 4:
                await bot.send_message(chat_id, "O'yinni boshlash uchun o'yinchilar yetarli emas....")
            else:
                await bot.send_message(chat_id, "Ro'yxatdan o'tish vaqti tugadi.")
        except Exception:
            pass
    except Exception:
        return


def _reset_votes(game: Dict[str, Any]) -> None:
    game["votes"] = {}
    game["confirm"] = {"target": None, "votes": {}}


def _role_of(game: Dict[str, Any], uid: str) -> str:
    return str(game.get("roles", {}).get(uid, "Tinch"))


def _emoji_role(role: str) -> str:
    return f"{ROLE_EMOJI.get(role, '🎭')} {role}"


def _player_label(game: Dict[str, Any], uid: str) -> str:
    name = game.get("players", {}).get(uid)
    if not name:
        name = "User"
    return f"{name}"


def _marker_for_teammate(viewer_role: str, target_role: str) -> str:
    mafia_core = {"Don", "Mafia"}
    if viewer_role in mafia_core and target_role in mafia_core:
        return {"Mafia": "S", "Don": "𝐴"}.get(target_role, "")
    if viewer_role in {"Komissar", "Serjant"} and target_role in {"Komissar", "Serjant"}:
        return {"Komissar": "K", "Serjant": "S"}.get(target_role, "")
    return ""


def _player_button_text(game: Dict[str, Any], viewer_uid: str, target_uid: str) -> str:
    vr = _role_of(game, str(viewer_uid))
    tr = _role_of(game, str(target_uid))
    m = _marker_for_teammate(vr, tr)
    base = _player_label(game, str(target_uid))
    return f"{m} - {base}" if m else base


def _teammates_block_html(game: Dict[str, Any], uid: str) -> str:
    role = _role_of(game, str(uid))
    mafia_core = {"Don", "Mafia"}

    if role in mafia_core:
        mates = [u for u in _alive_ids(game) if _role_of(game, u) in mafia_core]
        if len(mates) <= 1:
            return ""
        header = "Sheriklaringizni eslab qoling!\nS - 🤵🏼 Mafia\n𝐴 - 🤵🏻 Don\n\n"
        lines: List[str] = []
        for tuid in mates:
            if tuid == str(uid):
                continue
            marker = _marker_for_teammate(role, _role_of(game, tuid))
            lines.append(f"{marker} - {_player_link(game, tuid)}")
        return header + "\n".join(lines) if lines else ""

    if role in {"Komissar", "Serjant"}:
        mates = [u for u in _alive_ids(game) if _role_of(game, u) in {"Komissar", "Serjant"}]
        if len(mates) <= 1:
            return ""
        header = "Sheriklaringizni eslab qoling!\nK - 🕵🏻‍♂ Komissar\nS - 👮🏻‍♂ Serjant\n\n"
        lines = []
        for tuid in mates:
            if tuid == str(uid):
                continue
            marker = _marker_for_teammate(role, _role_of(game, tuid))
            lines.append(f"{marker} - {_player_link(game, tuid)}")
        return header + "\n".join(lines) if lines else ""

    return ""


def _player_link(game: Dict[str, Any], uid: str) -> str:
    name = game.get("players", {}).get(uid)
    if not name:
        name = "User"
    safe_name = html.escape(str(name))
    safe_uid = html.escape(str(uid))
    return f"<a href=\"tg://user?id={safe_uid}\">{safe_name}</a>"


def _win_state(game: Dict[str, Any]) -> Optional[str]:
    alive = set(_alive_ids(game))
    if not alive:
        return "none"

    roles_alive = {uid: _role_of(game, uid) for uid in alive}

    mafia_roles = {"Don", "Mafia", "Advokat", "Jurnalist", "убийца"}
    mafia = [uid for uid, r in roles_alive.items() if r in mafia_roles]
    qotil = [uid for uid, r in roles_alive.items() if r == "Qotil"]
    peaceful = [uid for uid, r in roles_alive.items() if r not in mafia_roles and r != "Qotil"]

    # If only one survivor remains, that survivor's side wins
    if len(alive) == 1:
        only_uid = next(iter(alive))
        if roles_alive.get(only_uid) == "Qotil":
            return "qotil"
        if roles_alive.get(only_uid) in mafia_roles:
            return "mafia"
        return "civ"

    # If Qotil is alive, the game must not end (unless he is sole survivor)
    if qotil:
        return None

    # Civilians win only when all mafia-side are eliminated.
    if not mafia:
        return "civ"

    # Mafia wins when mafia count >= peaceful-side count.
    if len(mafia) >= len(peaceful):
        return "mafia"

    return None


def _winners_text(game: Dict[str, Any], outcome: str) -> str:
    players = dict(game.get("players", {}) or {})
    winners = _winners_list(game, str(outcome))
    winners = [uid for uid in winners if str(uid) in players]
    winners = sorted(winners, key=lambda u: _player_label(game, u))

    wlines = "\n".join(f"{i+1}. {players.get(str(uid), 'User')}" for i, uid in enumerate(winners))
    if not wlines:
        wlines = "—"

    duration_min = game.get("_duration_min")
    dur_line = ""
    try:
        if duration_min is not None:
            dur_line = f"\n\nO'yin: {int(duration_min)} minut davom etdi"
    except Exception:
        dur_line = ""

    return (
        "O'yin tugadi!\n\n"
        "G'oliblar:\n"
        f"{wlines}"
        f"{dur_line}"
    )


def _winners_list(game: Dict[str, Any], outcome: str) -> List[str]:
    alive = _alive_ids(game)
    mafia_roles = {"Don", "Mafia", "Advokat", "Jurnalist", "убийца"}

    suid = game.get("_suidsid_winner")
    suid_uid = str(suid) if suid else None

    if outcome == "civ":
        winners = [uid for uid in alive if _role_of(game, uid) not in mafia_roles and _role_of(game, uid) != "Qotil"]
        if suid_uid and suid_uid not in winners:
            winners.append(suid_uid)
        return winners
    if outcome == "mafia":
        winners = [uid for uid in alive if _role_of(game, uid) in mafia_roles]
        if suid_uid and suid_uid not in winners:
            winners.append(suid_uid)
        return winners
    if outcome == "qotil":
        winners = [uid for uid in alive if _role_of(game, uid) == "Qotil"]
        if suid_uid and suid_uid not in winners:
            winners.append(suid_uid)
        return winners
    if outcome == "suidsid":
        return [suid_uid] if suid_uid else []
    if suid_uid:
        return [suid_uid]
    return []


def _profile_text_plain(uid: int, full_name: str) -> str:
    u = get_user(uid, full_name)
    name = str(full_name or u.get("name", "User"))
    return (
        f"⭐ ID: {uid}\n\n"
        f"👤 {name}\n\n"
        f"💵 Dollar: {fmt_money(u.get('money', 0))}\n"
        f"💎 Olmos: {u.get('diamonds', 0)}\n\n"
        f"🛡 Himoya: {u.get('protect', 0)}\n"
        f"⛑️ Qotildan himoya: {u.get('anti_killer', 0)}\n"
        f"⚖️ Ovoz berishni himoya qilish: {u.get('vote_protect', 0)}\n"
        f"🔫 Miltiq: {u.get('gun', 0)}\n\n"
        f"🎭 Maska: {u.get('mask', 0)}\n"
        f"📁 Soxta hujjat: {u.get('fake_docs', 0)}\n"
        f"🃏 Keyingi o'yindagi rolingiz: {u.get('next_role', '-')}\n\n"
        f"🎯 Побед: {u.get('wins', 0)}\n"
        f"🎲 Всего игр: {u.get('games', 0)}"
    )


async def _finish_game(chat_id: int, outcome: str) -> None:
    game = get_game(chat_id)
    WIN_REWARD_MONEY = 30
    WIN_REWARD_DIAMONDS = 0
    winners = _winners_list(game, str(outcome))

    all_players = list(game.get("participants", []) or [])
    if not all_players:
        all_players = list(game.get("players", {}).keys())

    for uid_s in all_players:
        try:
            uid_i = int(uid_s)
        except Exception:
            continue
        u = get_user(uid_i, str(game.get("players", {}).get(uid_s, "User")))
        u["games"] = int(u.get("games", 0)) + 1

        # per-group stats
        try:
            gs = dict(u.get("group_stats", {}) or {})
            gk = str(chat_id)
            cur = dict(gs.get(gk, {}) or {})
            cur["games"] = int(cur.get("games", 0)) + 1
            if uid_s in winners:
                cur["wins"] = int(cur.get("wins", 0)) + 1
            else:
                cur["wins"] = int(cur.get("wins", 0))
            gs[gk] = cur
            u["group_stats"] = gs
        except Exception:
            pass

        if uid_s in winners:
            u["wins"] = int(u.get("wins", 0)) + 1
            u["money"] = int(u.get("money", 0)) + int(WIN_REWARD_MONEY)
            u["diamonds"] = int(u.get("diamonds", 0)) + int(WIN_REWARD_DIAMONDS)
        update_user(uid_i, u)

    for uid_s in winners:
        try:
            uid_i = int(uid_s)
        except Exception:
            continue
        name = str(game.get("players", {}).get(uid_s, "User"))
        text = (
            "Siz yutdingiz!\n"
            f"Yutganingiz uchun sizga 💵 {WIN_REWARD_MONEY}, 💎 {WIN_REWARD_DIAMONDS} berildi!\n\n"
            + _profile_text_plain(uid_i, name)
        )
        if group_url(chat_id):
            await safe_dm(uid_i, text, reply_markup=group_open_kb(chat_id))
        else:
            await safe_dm(uid_i, text)

    try:
        started_ts = game.get("started_ts")
        if started_ts is not None:
            seconds = max(0, float(asyncio.get_event_loop().time()) - float(started_ts))
            minutes = max(1, int(seconds // 60))
            game["_duration_min"] = minutes
    except Exception:
        game.pop("_duration_min", None)

    game["started"] = False
    game["phase"] = PHASE_IDLE
    await _edit_group_status(chat_id, reply_markup=None)
    try:
        await bot.send_message(chat_id, _winners_text(game, str(outcome)), parse_mode="HTML")
    except Exception:
        pass
    await clear_silence(chat_id)


def _group_status_text(chat_id: int) -> str:
    game = get_game(chat_id)
    phase = game.get("phase")
    phase_txt = {
        PHASE_REG: "📝 Ro'yxatdan o'tish",
        PHASE_NIGHT: "🌃 Tun",
        PHASE_DISCUSSION: "🗣 Muhokama",
        PHASE_LYNCH: "🗳 Ovoz berish",
        PHASE_DAY: "☀️ Kun",
        PHASE_IDLE: "🔴 Faol emas",
    }.get(phase, str(phase))

    alive = _alive_ids(game)
    players_lines = []
    for i, uid in enumerate(alive, 1):
        players_lines.append(f"{i}. {_player_link(game, uid)}")

    if phase == PHASE_REG:
        now = asyncio.get_event_loop().time()
        left = max(0, int(float(game.get("reg_deadline", 0)) - now))
        mm = left // 60
        ss = left % 60
        left_txt = f"{mm:02d}:{ss:02d}"
        pids = list(game.get("players", {}).keys())
        if not pids:
            players_text = "Hali hech kim qo'shilmadi"
        else:
            players_text = "\n".join(f"{i}. {_player_link(game, uid)}" for i, uid in enumerate(pids, 1))
        return (
            "📝 Ro'yxatdan o'tish davom etmoqda\n\n"
            f"⏳ Qolgan vaqt: {left_txt}\n\n"
            "📋 Ro'yxatdan o'tganlar:\n"
            f"{players_text}\n\n"
            f"Jami {len(pids)}ta odam."
        )

    if not players_lines:
        players_text = "—"
    else:
        players_text = "\n".join(players_lines)

    return (
        f"🎮 O'yin holati: {phase_txt}\n"
        f"🌙 Raund: {game.get('round', 0)}\n\n"
        f"👥 Tiriklar ({len(alive)}):\n{players_text}"
    )


async def _edit_group_status(chat_id: int, reply_markup: Optional[InlineKeyboardMarkup] = None) -> None:
    game = get_game(chat_id)
    mid = game.get("reg_message_id")
    if not mid:
        return
    try:
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=mid,
            text=_group_status_text(chat_id),
            reply_markup=reply_markup,
            parse_mode="HTML",
        )
    except Exception as e:
        try:
            emsg = str(e).lower()
            if "message is not modified" in emsg:
                return
        except Exception:
            pass
        try:
            if game.get("phase") != PHASE_REG or game.get("started"):
                return
            sent = await bot.send_message(chat_id, _group_status_text(chat_id), reply_markup=reply_markup, parse_mode="HTML")
            game["reg_message_id"] = int(sent.message_id)
            if bool(get_group(chat_id).get("pin_registration", True)):
                try:
                    await _safe_pin_message(chat_id, int(sent.message_id))
                except Exception:
                    pass
            try:
                _runtime_set(
                    chat_id,
                    {
                        "phase": "reg",
                        "reg_message_id": game.get("reg_message_id"),
                        "reg_deadline": float(game.get("reg_deadline", 0.0) or 0.0),
                    },
                )
            except Exception:
                pass
        except Exception:
            return


def _targets_kb(chat_id: int, action: str, actor_uid: str, allow_self: bool = False) -> InlineKeyboardMarkup:
    game = get_game(chat_id)
    alive = _alive_ids(game)
    rows: List[List[InlineKeyboardButton]] = []
    for uid in alive:
        if not allow_self and uid == actor_uid:
            continue
        rows.append(
            [
                InlineKeyboardButton(
                    text=_player_button_text(game, actor_uid, uid),
                    callback_data=f"act:{chat_id}:{action}:{actor_uid}:{uid}",
                )
            ]
        )
    rows.append([InlineKeyboardButton(text="❌ Bekor", callback_data=f"act:{chat_id}:cancel:{actor_uid}:0")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _send_night_actions(chat_id: int) -> None:
    game = get_game(chat_id)
    alive = _alive_ids(game)

    # Group-visible night narration (no targets), as in original Baku2 UX
    try:
        roles_present = {_role_of(game, uid) for uid in alive}
        role_lines = {
            "Daydi": "🧙‍♂️ Daydi kimnikigadir ichkilik butilka olish uchun ketdi...",
            "Doktor": "🧑‍⚕️ Doktor tungi navbatchilikka ketdi...",
            "Kezuvchi": "🕵️ Kezuvchining qandaydir mehmoni bor ekan...",
            "Komissar": "🕵🏻‍♂ Komissar katani katani ayovuzlarni qidirishga ketdi...",
            "Serjant": "👮🏻‍♂ Serjant shubhali odamlarga ko'z tashlamoqda...",
            "Qotil": "🔪 Qotil butalar orasiga yashirinib oldi va pichoqni qinidan chiqardi...",
            "Don": "🤵🏼 Mafia kechasi ko'chaga chiqdi...",
            "Mafia": "🤵🏼 Mafia kechasi ko'chaga chiqdi...",
            "убийца": "🩸 убийца ovga chiqdi...",
            "Jurnalist": "📰 Jurnalist tungi tekshiruvga ketdi...",
            "Advokat": "🧑‍⚖️ Advokat kimnidir himoya qilish uchun yo'lga chiqdi...",
            "G'azabkor": "🧟 G'azabkor kimnidir kuzatmoqda...",
            "Sotqin": "🤓 Sotqin shubhali ma'lumotlar izlamoqda...",
            "Sehrgar": "🧙‍♂️ Sehrgar qorong'uda sehrini tayyorlamoqda...",
        }
        for r, t in role_lines.items():
            if r in roles_present:
                try:
                    await bot.send_message(chat_id, t)
                except Exception:
                    pass
    except Exception:
        pass

    async def _night_dm_or_group(uid: str, text: str, reply_markup: InlineKeyboardMarkup) -> None:
        ok = await safe_dm(int(uid), text, reply_markup=reply_markup)
        if ok:
            return
        try:
            await bot.send_message(
                chat_id,
                "⬇️",
                reply_markup=await bot_link_kb(),
            )
        except Exception:
            pass

    for uid in alive:
        role = _role_of(game, uid)

        if role in {"Don", "Mafia"}:
            await _night_dm_or_group(uid, "🌃 Tun: Mafiya kimni o'ldiradi?", with_group_button(chat_id, _targets_kb(chat_id, "mafia_kill", uid)))
        elif role == "убийца":
            await _night_dm_or_group(uid, "🌃 Tun: убийца kimni o'ldiradi?", with_group_button(chat_id, _targets_kb(chat_id, "udda_kill", uid)))
        elif role == "Qotil":
            await _night_dm_or_group(uid, "🌃 Tun: Qotil kimni o'ldiradi?", with_group_button(chat_id, _targets_kb(chat_id, "qotil_kill", uid)))
        elif role == "Doktor":
            allow_self = uid not in game["night"].get("doctor_self_used", set())
            await _night_dm_or_group(
                uid,
                "🌃 Tun: Doktor kimni davolaydi?",
                with_group_button(chat_id, _targets_kb(chat_id, "doctor_save", uid, allow_self=allow_self)),
            )
        elif role == "Komissar":
            await _night_dm_or_group(uid, "🌃 Tun: Komissar kimni tekshiradi?", with_group_button(chat_id, _targets_kb(chat_id, "kom_check", uid)))
            await _night_dm_or_group(uid, "🌃 Tun: Komissar kimni otadi?", with_group_button(chat_id, _targets_kb(chat_id, "kom_shoot", uid)))
        elif role == "Advokat":
            await _night_dm_or_group(uid, "🌃 Tun: Advokat kimni himoya qiladi?", with_group_button(chat_id, _targets_kb(chat_id, "adv_protect", uid)))
        elif role == "Kezuvchi":
            await _night_dm_or_group(uid, "🌃 Tun: Kezuvchi kimni uxlatadi?", with_group_button(chat_id, _targets_kb(chat_id, "kezuvchi_block", uid)))
        elif role == "Daydi":
            await _night_dm_or_group(uid, "🌃 Tun: Daydi kimning oldiga boradi?", with_group_button(chat_id, _targets_kb(chat_id, "daydi_visit", uid)))
        elif role == "Jurnalist":
            await _night_dm_or_group(uid, "🌃 Tun: Jurnalist kimni tekshiradi?", with_group_button(chat_id, _targets_kb(chat_id, "journalist_check", uid)))
        elif role == "G'azabkor":
            await _night_dm_or_group(uid, "🌃 Tun: G'azabkor kimni tanlaydi?", with_group_button(chat_id, _targets_kb(chat_id, "gazabkor_mark", uid)))


async def _resolve_night(chat_id: int) -> None:
    game = get_game(chat_id)
    ndata = game.get("night", {})

    # Apply block from Kezuvchi first
    b = ndata.get("kezuvchi_block")
    if b:
        game["blocked"].add(str(b))
        game.setdefault("sleep_day", set()).add(str(b))

    # Komissar check result (affected by Advokat)
    kom_target = ndata.get("kom_check")
    kom_uid = None
    for uid in _alive_ids(game):
        if _role_of(game, uid) == "Komissar":
            kom_uid = uid
            break
    if kom_target and kom_uid and str(kom_uid) not in game.get("blocked", set()):
        adv_prot = ndata.get("adv_protect")
        real_role = _role_of(game, str(kom_target))
        mafia_side = {"Don", "Mafia", "Advokat", "Jurnalist", "убийца"}
        shown_role = (
            "Tinch"
            if adv_prot
            and str(adv_prot) == str(kom_target)
            and real_role in mafia_side
            else real_role
        )
        check_text = f"🕵🏻‍♂ Tekshiruv: {_player_link(game, str(kom_target))} — {_emoji_role(shown_role)}"
        if group_url(chat_id):
            await safe_dm(int(kom_uid), check_text, reply_markup=group_open_kb(chat_id))
        else:
            await safe_dm(int(kom_uid), check_text)

        # Serjant receives the same investigation info
        for suid in _alive_ids(game):
            if _role_of(game, suid) == "Serjant":
                # Also inform Serjant who the Komissar is (once per game)
                try:
                    if not bool(game.get("serjant_knows_kom", False)):
                        game["serjant_knows_kom"] = True
                        kom_info = (
                            "Sheriklaringizni eslab qoling!\n"
                            "K - 🕵🏻‍♂ Komissar\n"
                            "S - 👮🏻‍♂ Serjant\n\n"
                            f"K - {_player_link(game, str(kom_uid))}"
                        )
                        if group_url(chat_id):
                            await safe_dm(int(suid), kom_info, reply_markup=group_open_kb(chat_id), parse_mode="HTML")
                        else:
                            await safe_dm(int(suid), kom_info, parse_mode="HTML")
                except Exception:
                    pass
                if group_url(chat_id):
                    await safe_dm(int(suid), check_text, reply_markup=group_open_kb(chat_id))
                else:
                    await safe_dm(int(suid), check_text)
                break

        # Sotqin reveals the checked role to the group
        try:
            for suid in _alive_ids(game):
                if _role_of(game, suid) == "Sotqin" and suid not in game.get("blocked", set()):
                    await bot.send_message(
                        chat_id,
                        f"🤓 Sotqin xabar berdi: {_player_link(game, str(kom_target))} — {_emoji_role(shown_role)}",
                        parse_mode="HTML",
                    )
                    break
        except Exception:
            pass

    # Journalist check: send target role to mafia-side players
    j_target = ndata.get("journalist_check")
    try:
        j_uid = None
        for uid in _alive_ids(game):
            if _role_of(game, uid) == "Jurnalist":
                j_uid = uid
                break
        if j_target and j_uid and str(j_uid) not in game.get("blocked", set()):
            role_seen = _role_of(game, str(j_target))
            j_text = f"📰 Tekshiruv: {_player_link(game, str(j_target))} — {_emoji_role(role_seen)}"
            mafia_side = {"Don", "Mafia", "Advokat", "Jurnalist", "убийца"}
            for muid in _alive_ids(game):
                if _role_of(game, muid) in mafia_side:
                    if group_url(chat_id):
                        await safe_dm(int(muid), j_text, reply_markup=group_open_kb(chat_id))
                    else:
                        await safe_dm(int(muid), j_text)
    except Exception:
        pass

    # Determine kills
    mafia_target = ndata.get("mafia_kill")
    qotil_target = ndata.get("qotil_kill")
    udda_target = ndata.get("udda_kill")
    kom_shoot_target = ndata.get("kom_shoot")

    doctor_save = ndata.get("doctor_save")
    # doctor self used
    if doctor_save is not None:
        # mark self-used
        # find doctor uid
        for duid in _alive_ids(game):
            if _role_of(game, duid) == "Doktor":
                if str(doctor_save) == duid:
                    game["night"].setdefault("doctor_self_used", set()).add(duid)
                break

    killed: List[Tuple[str, str]] = []  # (target_uid, killer_role)

    def add_kill(target: Optional[str], killer_role: str) -> None:
        if not target:
            return
        tuid = str(target)
        if tuid not in _alive_ids(game):
            return
        if doctor_save and str(doctor_save) == tuid:
            return
        # Omadli: first night attack is always survived; later attacks keep 50% chance
        if _role_of(game, tuid) == "Omadli":
            used = set(game.get("omadli_used", set()) or set())
            if tuid not in used:
                used.add(tuid)
                game["omadli_used"] = used
                asyncio.create_task(
                    safe_dm(
                        int(tuid),
                        "🤞🏼 Omadli bo'lib omon qoldingiz!",
                        reply_markup=group_open_kb(chat_id) if group_url(chat_id) else None,
                    )
                )
                return
            try:
                if random.random() < 0.5:
                    asyncio.create_task(
                        safe_dm(
                            int(tuid),
                            "🤞🏼 Omadli bo'lib omon qoldingiz!",
                            reply_markup=group_open_kb(chat_id) if group_url(chat_id) else None,
                        )
                    )
                    return
            except Exception:
                pass
        killed.append((tuid, killer_role))

    # blocked killers cannot act
    # For 2-kill mafia rules: Don/Mafia kill and убийца kill are independent.
    mafia_killer_blocked = any(uid in game["blocked"] for uid in _alive_ids(game) if _role_of(game, uid) in {"Don", "Mafia"})
    if mafia_target and not mafia_killer_blocked:
        add_kill(str(mafia_target), "Mafia")

    # Uddaburon (убийца) kill counts as mafia-side attack
    udda_blocked = any(uid in game["blocked"] for uid in _alive_ids(game) if _role_of(game, uid) == "убийца")
    if udda_target and not udda_blocked:
        add_kill(str(udda_target), "Mafia")

    qotil_blocked = any(uid in game["blocked"] for uid in _alive_ids(game) if _role_of(game, uid) == "Qotil")
    if qotil_target and not qotil_blocked:
        add_kill(str(qotil_target), "Qotil")

    kom_uid = None
    for uid in _alive_ids(game):
        if _role_of(game, uid) == "Komissar":
            kom_uid = uid
            break
    kom_blocked = bool(kom_uid and kom_uid in game.get("blocked", set()))
    if kom_shoot_target and not kom_blocked:
        add_kill(str(kom_shoot_target), "Komissar")

    # Doctor feedback: whom he saved and from which role (best-effort)
    try:
        doctor_uid = None
        for duid in _alive_ids(game):
            if _role_of(game, duid) == "Doktor":
                doctor_uid = duid
                break
        if doctor_uid and doctor_save:
            attackers: List[str] = []
            if str(doctor_save) == str(mafia_target) and mafia_target and not mafia_killer_blocked:
                attackers.append("Mafia")
            if str(doctor_save) == str(udda_target) and udda_target and not udda_blocked:
                attackers.append("Mafia")
            if str(doctor_save) == str(qotil_target) and qotil_target and not qotil_blocked:
                attackers.append("Qotil")
            if str(doctor_save) == str(kom_shoot_target) and kom_shoot_target and not kom_blocked:
                attackers.append("Komissar")
            if attackers:
                atk = attackers[0]
                t = f"Siz- {_player_link(game, str(doctor_save))} ni davoladingiz:)\nUni mehmoni {_emoji_role(atk)} edi."
                if group_url(chat_id):
                    await safe_dm(int(doctor_uid), t, reply_markup=group_open_kb(chat_id), parse_mode="HTML")
                else:
                    await safe_dm(int(doctor_uid), t, parse_mode="HTML")
    except Exception:
        pass

    # G'azabkor: after 3 choices against same target => takes them away (both die)
    try:
        marks: Dict[str, str] = dict(ndata.get("gazabkor_mark", {}) or {})
        for actor_uid, tgt_uid in marks.items():
            if actor_uid not in _alive_ids(game):
                continue
            if _role_of(game, actor_uid) != "G'azabkor":
                continue
            if actor_uid in game.get("blocked", set()):
                continue
            if tgt_uid not in _alive_ids(game):
                continue
            counter = game.setdefault("gazabkor_counter", {}).setdefault(actor_uid, {})
            counter[str(tgt_uid)] = int(counter.get(str(tgt_uid), 0)) + 1
            if int(counter.get(str(tgt_uid), 0)) >= 3:
                if str(tgt_uid) in game.get("alive", []):
                    game["alive"].remove(str(tgt_uid))
                if str(actor_uid) in game.get("alive", []):
                    game["alive"].remove(str(actor_uid))
                try:
                    await bot.send_message(
                        chat_id,
                        f"🧟 G'azabkor olib ketdi: {_player_link(game, str(tgt_uid))}",
                        parse_mode="HTML",
                    )
                except Exception:
                    pass
    except Exception:
        pass

    # Sehrgar: if attacked, he decides forgive/kill attacker; he survives either way.
    sehrgar_pending: Dict[str, List[str]] = {}

    # Apply Bo'ri special rule: if mafia tries to kill wolf, wolf transforms to Mafia (does not die)
    final_kills: List[Tuple[str, str]] = []
    for tuid, killer in killed:
        # Sehrgar survives any attack; can retaliate
        if _role_of(game, tuid) == "Sehrgar":
            sehrgar_pending.setdefault(str(tuid), []).append(str(killer))
            continue
        if _role_of(game, tuid) == "Bo'ri" and killer == "Mafia":
            game["roles"][tuid] = "Mafia"
            if group_url(chat_id):
                await safe_dm(int(tuid), "🐺 Sizga Mafiya hujum qildi, lekin siz Mafiyaga aylandingiz!", reply_markup=group_open_kb(chat_id))
            else:
                await safe_dm(int(tuid), "🐺 Sizga Mafiya hujum qildi, lekin siz Mafiyaga aylandingiz!")
            continue
        if _role_of(game, tuid) == "Bo'ri" and killer == "Komissar":
            game["roles"][tuid] = "Serjant"
            if group_url(chat_id):
                await safe_dm(int(tuid), "🐺 Sizga Komissar hujum qildi, siz Serjantga aylandingiz!", reply_markup=group_open_kb(chat_id))
            else:
                await safe_dm(int(tuid), "🐺 Sizga Komissar hujum qildi, siz Serjantga aylandingiz!")
            continue
        final_kills.append((tuid, killer))

    # Resolve Sehrgar decisions (short wait)
    for sg_uid, killers in list(sehrgar_pending.items()):
        try:
            game.setdefault("sehrgar_pending", {})[sg_uid] = killers
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(text="Kechiraman", callback_data=f"sehrgar:{chat_id}:{sg_uid}:forgive"),
                        InlineKeyboardButton(text="O'ldiraman", callback_data=f"sehrgar:{chat_id}:{sg_uid}:kill"),
                    ]
                ]
            )
            await safe_dm(
                int(sg_uid),
                "🧙‍♂️ Sizga hujum bo'ldi. Qaror qiling:",
                reply_markup=with_group_button(chat_id, kb),
            )
        except Exception:
            pass

    if sehrgar_pending:
        end_wait = asyncio.get_event_loop().time() + 20
        while asyncio.get_event_loop().time() < end_wait:
            decided = game.get("sehrgar_decision", {}) or {}
            if all(uid in decided for uid in sehrgar_pending.keys()):
                break
            await asyncio.sleep(1)

        decided = dict(game.get("sehrgar_decision", {}) or {})
        for sg_uid, killers in sehrgar_pending.items():
            choice = str(decided.get(sg_uid, "forgive"))
            if choice != "kill":
                continue

            # retaliate for each killer type (best-effort)
            for killer in killers:
                if killer == "Qotil":
                    for ku in _alive_ids(game):
                        if _role_of(game, ku) == "Qotil":
                            if ku in game.get("alive", []):
                                game["alive"].remove(ku)
                            break
                elif killer == "Komissar":
                    for ku in _alive_ids(game):
                        if _role_of(game, ku) == "Komissar":
                            if ku in game.get("alive", []):
                                game["alive"].remove(ku)
                            break
                else:
                    # Mafia-side retaliation: Don if alive else any mafia-side
                    mafia_side = [
                        ku
                        for ku in _alive_ids(game)
                        if _role_of(game, ku) in {"Don", "Mafia", "Advokat", "Jurnalist", "убийца"}
                    ]
                    don = next((ku for ku in mafia_side if _role_of(game, ku) == "Don"), None)
                    victim = don or (mafia_side[0] if mafia_side else None)
                    if victim and victim in game.get("alive", []):
                        game["alive"].remove(victim)

            try:
                await bot.send_message(chat_id, f"🧙‍♂️ Sehrgar qasos oldi!",)
            except Exception:
                pass

    # Daydi witness mechanic: if Daydi visited a victim, he learns who came to that house
    daydi_target = ndata.get("daydi_visit")
    if daydi_target:
        daydi_uid = None
        for uid in _alive_ids(game):
            if _role_of(game, uid) == "Daydi":
                daydi_uid = uid
                break
        if daydi_uid and daydi_uid not in game.get("blocked", set()):
            for tuid, killer in final_kills:
                if str(tuid) == str(daydi_target):
                    witness_text = (
                        "🧙‍♂ Guvoh bo'ldingiz:\n"
                        f"Tunda {_player_link(game, str(tuid))} oldiga {_emoji_role(str(killer))} kelgan."
                    )
                    if group_url(chat_id):
                        await safe_dm(int(daydi_uid), witness_text, reply_markup=group_open_kb(chat_id))
                    else:
                        await safe_dm(int(daydi_uid), witness_text)
                    break

    # Execute deaths + Afsungar revenge on night-kill
    extra_deaths: List[str] = []
    for tuid, killer in final_kills:
        role = _role_of(game, tuid)
        if role == "Afsungar":
            # revenge: kill the killer (if we can determine). For mafia => kill one mafia (Don if alive else Mafia).
            if killer == "Qotil":
                # find qotil
                for ku in _alive_ids(game):
                    if _role_of(game, ku) == "Qotil":
                        extra_deaths.append(ku)
                        break
            elif killer == "Mafia":
                for ku in _alive_ids(game):
                    if _role_of(game, ku) == "Don":
                        extra_deaths.append(ku)
                        break
                if not extra_deaths:
                    for ku in _alive_ids(game):
                        if _role_of(game, ku) == "Mafia":
                            extra_deaths.append(ku)
                            break

        if tuid in game["alive"]:
            game["alive"].remove(tuid)

    for uid in extra_deaths:
        if uid in game["alive"]:
            game["alive"].remove(uid)

    await _maybe_promote_don(chat_id, game)

    # Serjant inherits Komissar if Komissar died
    kom_dead = True
    for uid in _alive_ids(game):
        if _role_of(game, uid) == "Komissar":
            kom_dead = False
            break
    if kom_dead:
        for uid in _alive_ids(game):
            if _role_of(game, uid) == "Serjant":
                game["roles"][uid] = "Komissar"
                if group_url(chat_id):
                    await safe_dm(int(uid), "👮🏻‍♂➡️🕵🏻‍♂ Komissar o'ldi. Siz endi Komissarsiz!", reply_markup=group_open_kb(chat_id))
                else:
                    await safe_dm(int(uid), "👮🏻‍♂➡️🕵🏻‍♂ Komissar o'ldi. Siz endi Komissarsiz!")
                break

    # Prepare day summary text
    died_names = [
        _player_link(game, uid) for uid, _ in final_kills if uid not in _alive_ids(game)
    ]
    if extra_deaths:
        died_names += [_player_link(game, uid) for uid in extra_deaths]

    # Daybreak narration (Baku style)
    game["round"] = int(game.get("round", 0))
    alive = _alive_ids(game)
    alive_lines = "\n".join(f"{i+1}. {_player_link(game, uid)}" for i, uid in enumerate(alive))
    caption = (
        "Xayrli tong🌝\n"
        f"🌄Kun: {game.get('round', 1)}\n"
        "Shamollar tundagi mish-mishlarni butun shaharga yetkazmoqda.. 🌚"
    )
    day_followup = "Endi kechaning natijalarini muhokama qilish, sabablari va oqibatlarini tushunish vaqti keldi ..."
    try:
        await bot.send_photo(chat_id, _photo_input(DAY_LOCAL_PATH, DAY_MEDIA), caption=caption, reply_markup=bot_open_kb())
    except Exception:
        try:
            await bot.send_message(chat_id, caption)
        except Exception:
            pass

    try:
        await bot.send_message(chat_id, day_followup)
    except Exception:
        pass

    # Show all night deaths in ONE post (Baku2 style)
    try:
        lines: List[str] = []
        for tuid, killer in final_kills:
            if str(tuid) in _alive_ids(game):
                continue
            vrole = _role_of(game, str(tuid))
            lines.append(
                (
                    f"Tunda {_emoji_role(vrole)} {_player_link(game, str(tuid))}...\n"
                    "vaxshiylarcha o'ldirildi. Aytishlaricha unikiga "
                    f"{_emoji_role(str(killer))} kelgan"
                )
            )
        for tuid in extra_deaths:
            if str(tuid) in _alive_ids(game):
                continue
            vrole = _role_of(game, str(tuid))
            lines.append(
                (
                    f"Tunda {_emoji_role(vrole)} {_player_link(game, str(tuid))}...\n"
                    "vaxshiylarcha o'ldirildi."
                )
            )
        if not lines:
            lines.append("Ishonish qiyin, lekin bu tunda hech kim o'lmadi...")
        await bot.send_message(chat_id, "\n\n".join(lines), parse_mode="HTML")
    except Exception:
        pass

    # Alive players list as separate message
    try:
        await bot.send_message(
            chat_id,
            (
                "Tirik o'yinchilar:\n"
                f"{alive_lines}\n\n"
                f"Ulardan: {_role_counts_line(game)}\n"
                f"Jami: {len(alive)}"
            ),
            parse_mode="HTML",
        )
    except Exception:
        pass

def _vote_kb(chat_id: int, voter_uid: str) -> InlineKeyboardMarkup:
    game = get_game(chat_id)
    rows: List[List[InlineKeyboardButton]] = []
    for uid in _alive_ids(game):
        if uid == voter_uid:
            continue
        rows.append(
            [
                InlineKeyboardButton(
                    text=_player_button_text(game, voter_uid, uid),
                    callback_data=f"vote:{chat_id}:{voter_uid}:{uid}",
                )
            ]
        )
    rows.append([InlineKeyboardButton(text="🚫 O'tkazib yuborish", callback_data=f"vote:{chat_id}:{voter_uid}:0")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _send_vote_menus(chat_id: int) -> None:
    game = get_game(chat_id)
    alive = _alive_ids(game)
    try:
        me = await bot.get_me()
        url = f"https://t.me/{me.username}?start=vote_{chat_id}"
        m = await bot.send_message(
            chat_id,
            (
                "Aybdorlarni aniqlash va jazolash vaqti keldi.\n"
                f"Ovoz berish uchun {int(get_group(chat_id).get('day_seconds', 60))} sekund"
            ),
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="Ovoz berish", url=url)]]
            ),
        )
        game["vote_message_id"] = m.message_id
    except Exception:
        game["vote_message_id"] = None

    # Auto-send vote menu to ALL alive players (no need to press group button)
    any_fail = False
    for uid in alive:
        ok = await safe_dm(
            int(uid),
            "Aybdorlarni topish va jazolash vaqti keldi!\nKimni osish kerak deb xisoblaysiz?",
            reply_markup=with_group_button(chat_id, _vote_kb(chat_id, uid)),
        )
        if not ok:
            any_fail = True

    if any_fail:
        try:
            await bot.send_message(chat_id, "⬇️", reply_markup=await bot_link_kb())
        except Exception:
            pass


async def _send_confirm(chat_id: int, target_uid: str) -> None:
    game = get_game(chat_id)
    game["confirm"] = {"target": target_uid, "votes": {}}
    try:
        await bot.send_message(
            chat_id,
            f"Rostdan xam {_player_link(game, target_uid)}ni osmoqchimisiz?",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(text="👍 0", callback_data=f"confirm:{chat_id}:up"),
                        InlineKeyboardButton(text="👎 0", callback_data=f"confirm:{chat_id}:down"),
                    ]
                ]
            ),
            parse_mode="HTML",
        )
    except Exception:
        pass


async def _resolve_votes(chat_id: int) -> bool:
    game = get_game(chat_id)
    votes: Dict[str, str] = game.get("votes", {})
    tally: Dict[str, int] = {}
    for v, t in votes.items():
        if not t or t == "0":
            continue
        if t not in _alive_ids(game):
            continue
        weight = 2 if _role_of(game, str(v)) == "Janob" else 1
        tally[t] = tally.get(t, 0) + int(weight)

    if not tally:
        try:
            await bot.send_message(
                chat_id,
                "Ovoz berish yakunlandi:\nAxoli kelisha olmadi...\n\nKelisha olmaslik oqibatida xech kim osilmadi...",
            )
        except Exception:
            pass
        return False

    maxv = max(tally.values())
    top = [uid for uid, c in tally.items() if c == maxv]
    if len(top) != 1:
        try:
            await bot.send_message(
                chat_id,
                "Ovoz berish yakunlandi:\nAxoli kelisha olmadi...\n\nKelisha olmaslik oqibatida xech kim osilmadi...",
            )
        except Exception:
            pass
        return False

    await _send_confirm(chat_id, top[0])
    return True


async def _game_loop(chat_id: int) -> None:
    game = get_game(chat_id)
    while game.get("started"):
        outcome = _win_state(game)
        if outcome:
            await _finish_game(chat_id, str(outcome))
            return

        g = get_group(chat_id)

        if game.get("phase") == PHASE_NIGHT:
            _reset_night(game)
            game["deadline"] = asyncio.get_event_loop().time() + int(g.get("night_seconds", 60))
            await _edit_group_status(chat_id, reply_markup=None)
            await apply_silence_rules(chat_id)
            try:
                await bot.send_photo(
                    chat_id,
                    _photo_input(NIGHT_LOCAL_PATH, NIGHT_MEDIA),
                    caption=(
                        "🌃Tun\n"
                        "Ko'chaga faqat jasur va qo'rqmas odamlar chiqishdi. Ertalab tirik qolganlarni sanaymiz..."
                    ),
                    reply_markup=bot_open_kb(),
                )
            except Exception:
                pass
            await _send_night_actions(chat_id)
            while asyncio.get_event_loop().time() < float(game.get("deadline", 0)):
                await asyncio.sleep(1)
                if not game.get("started"):
                    return

            # inactivity auto-leave: if player with night action misses 2 nights => remove
            action_roles = {"Don", "Mafia", "Komissar", "Doktor", "Advokat", "Kezuvchi", "Daydi", "Qotil", "убийца", "Jurnalist", "G'azabkor"}
            acted = set(game.get("night_acted", set()) or set())
            missed: List[str] = []
            for uid in list(_alive_ids(game)):
                if _role_of(game, uid) not in action_roles:
                    continue
                if uid in game.get("blocked", set()):
                    continue
                if uid in acted:
                    game.setdefault("missed_nights", {})[uid] = 0
                    continue
                cnt = int(game.setdefault("missed_nights", {}).get(uid, 0)) + 1
                game["missed_nights"][uid] = cnt
                if cnt >= 2:
                    missed.append(uid)

            for uid in missed:
                try:
                    gone_role = _role_of(game, uid)
                except Exception:
                    gone_role = "Tinch"
                try:
                    gone_name = _player_label(game, uid)
                except Exception:
                    gone_name = "User"

                game.get("players", {}).pop(uid, None)
                game.get("roles", {}).pop(uid, None)
                if uid in game.get("alive", []):
                    try:
                        game["alive"].remove(uid)
                    except Exception:
                        pass
                try:
                    await bot.send_message(
                        chat_id,
                        (
                            f"Aholidan kimdir {_emoji_role(str(gone_role))} {gone_name} o'limidan oldin:\n"
                            "Men o'yin paytida boshqa uxlamayma-a-a-a-a-a-an! deb qichqirganini eshitgan."
                        ),
                    )
                except Exception:
                    pass

            if missed and game.get("started"):
                try:
                    await _edit_group_status(chat_id, reply_markup=None)
                except Exception:
                    pass

            await _resolve_night(chat_id)
            game["phase"] = PHASE_DISCUSSION
            await apply_silence_rules(chat_id)
            continue

        if game.get("phase") == PHASE_DISCUSSION:
            game["deadline"] = asyncio.get_event_loop().time() + int(g.get("discussion_seconds", 60))
            await _edit_group_status(chat_id, reply_markup=None)
            try:
                await bot.send_message(chat_id, "🗣 Muhokama boshlandi!")
            except Exception:
                pass
            while asyncio.get_event_loop().time() < float(game.get("deadline", 0)):
                await asyncio.sleep(1)
                if not game.get("started"):
                    return
            game["phase"] = PHASE_LYNCH
            await apply_silence_rules(chat_id)
            continue

        if game.get("phase") == PHASE_LYNCH:
            _reset_votes(game)
            game["deadline"] = asyncio.get_event_loop().time() + int(g.get("day_seconds", 60))
            await _edit_group_status(chat_id, reply_markup=None)
            await _send_vote_menus(chat_id)

            while asyncio.get_event_loop().time() < float(game.get("deadline", 0)):
                await asyncio.sleep(1)
                if not game.get("started"):
                    return
                # Real-time countdown update on the vote announcement message
                try:
                    mid = game.get("vote_message_id")
                    if mid:
                        left = max(0, int(float(game.get("deadline", 0)) - asyncio.get_event_loop().time()))
                        last_left = game.get("_vote_left_last")
                        # Reduce edit frequency to avoid freezes: every 3 seconds, and each second for last 5 sec
                        should_edit = (left <= 5) or (left % 3 == 0)
                        if should_edit and (last_left is None or int(last_left) != left):
                            game["_vote_left_last"] = left
                            await bot.edit_message_text(
                                chat_id=chat_id,
                                message_id=int(mid),
                                text=(
                                    "Aybdorlarni aniqlash va jazolash vaqti keldi.\n"
                                    f"Ovoz berish uchun  {left} sekund"
                                ),
                                reply_markup=InlineKeyboardMarkup(
                                    inline_keyboard=[[InlineKeyboardButton(text="Ovoz berish", url=url)]]
                                ),
                            )
                except Exception:
                    pass
                # early stop if everyone voted
                if len(game.get("votes", {})) >= len(_alive_ids(game)):
                    break

            confirm_started = await _resolve_votes(chat_id)
            if confirm_started:
                game["phase"] = PHASE_CONFIRM
                game["deadline"] = asyncio.get_event_loop().time() + 45
            else:
                game["phase"] = PHASE_NIGHT
                game["round"] = int(game.get("round", 0)) + 1
            await apply_silence_rules(chat_id)
            continue

        if game.get("phase") == PHASE_CONFIRM:
            while asyncio.get_event_loop().time() < float(game.get("deadline", 0)):
                await asyncio.sleep(1)
                if not game.get("started"):
                    return

            target = game.get("confirm", {}).get("target")
            cvotes: Dict[str, str] = game.get("confirm", {}).get("votes", {})
            up = 0
            down = 0
            for voter, v in cvotes.items():
                weight = 2 if _role_of(game, str(voter)) == "Janob" else 1
                if v == "up":
                    up += int(weight)
                elif v == "down":
                    down += int(weight)
            try:
                await bot.send_message(chat_id, f"Ovoz berish natijalari:\n{up} 👍  |  {down} 👎")
            except Exception:
                pass
            if target and up > down and target in game.get("alive", []):
                lynched = str(target)
                lynch_role = _role_of(game, lynched)
                game["alive"].remove(lynched)
                try:
                    await bot.send_message(
                        chat_id,
                        f"{_player_link(game, lynched)} O'tkazilgan kunduzgi yiģilishda osildi!\nU edi {lynch_role}..",
                        parse_mode="HTML",
                    )
                except Exception:
                    pass

                try:
                    await _maybe_promote_don(chat_id, game)
                except Exception:
                    pass

                if lynch_role == "Afsungar":
                    game["afsungar_day_revenge"] = lynched
                    await safe_dm(
                        int(lynched),
                        "🧞‍♂️ Siz osildingiz. Endi 1 ta o'yinchini o'zingiz bilan olib ketasiz:",
                        reply_markup=with_group_button(chat_id, _targets_kb(chat_id, "afsun_revenge", lynched)),
                    )
                if lynch_role == "Suidsid":
                    game["_suidsid_winner"] = lynched
                    # Suidsid wins by being lynched, but the game continues
            else:
                try:
                    await bot.send_message(chat_id, "Hech kim osilmadi.")
                except Exception:
                    pass

            game["phase"] = PHASE_NIGHT
            game["round"] = int(game.get("round", 0)) + 1
            await apply_silence_rules(chat_id)
            continue

        await asyncio.sleep(1)


def assign_roles(n: int, chat_id: Optional[int] = None) -> List[str]:
    g = get_group(int(chat_id)) if chat_id is not None else DEFAULT_GROUP
    ratio = str(g.get("mafia_ratio", "1/3"))
    div = 3 if ratio == "1/3" else 4

    disabled = set()
    if chat_id is not None:
        disabled = _disabled_roles_set(int(chat_id))

    mafia_count = max(1, n // div)
    # For big lobbies (like your 30-player example) keep mafia a bit lower to match Black2 style
    if div == 3 and n >= 24:
        mafia_count = max(1, int(mafia_count) - 1)

    roles: List[str] = []

    # Mafia side: build EXACT mafia_count with Don + optional singletons + remaining Mafia
    mafia_team: List[str] = ["Don"]
    mafia_singletons: List[Tuple[int, str]] = [
        (7, "Advokat"),
        (8, "убийца"),
        (15, "Jurnalist"),
    ]
    for min_n, r in mafia_singletons:
        if len(mafia_team) >= mafia_count:
            break
        if n < min_n:
            continue
        if r in disabled:
            continue
        if r in mafia_team:
            continue
        mafia_team.append(r)

    while len(mafia_team) < mafia_count:
        mafia_team.append("Mafia")

    roles.extend(mafia_team[:mafia_count])

    # Peaceful core: Komissar always (if enabled), Doktor if enabled
    if "Komissar" not in disabled and "Komissar" not in roles:
        roles.append("Komissar")
    if "Doktor" not in disabled and n >= 5 and "Doktor" not in roles:
        roles.append("Doktor")

    # Repeatable specials (can increase with player count)
    # Scaling rule: add +1 extra copy for each `step` players after `min_n`.
    # Tuned to match your 30-player example: Serjant=2, Afsungar=3, Bo'ri=2, Sehrgar=1
    repeatable_specs: List[Tuple[str, int, int]] = [
        ("Serjant", 6, 16),
        ("Afsungar", 8, 8),
        ("Bo'ri", 9, 16),
        ("Sehrgar", 11, 20),
    ]
    for r, min_n, step in repeatable_specs:
        if r in disabled:
            continue
        if n < min_n:
            continue
        target = 1 + max(0, (n - min_n) // step)
        while roles.count(r) < target and len(roles) < n:
            roles.append(r)

    # Keep Tinch low but not zero in large lobbies
    reserve_tinch = 3 if n >= 30 else (2 if n >= 20 else 1)
    max_non_tinch = max(0, n - reserve_tinch)

    # Singleton specials (max 1 each)
    # For 30+ players, prefer a stable template close to your screenshot.
    if n >= 30:
        singleton_specs: List[Tuple[int, str]] = [
            (5, "Kezuvchi"),
            (5, "Omadli"),
            (7, "Daydi"),
            (10, "Qotil"),
            (10, "Janob"),
            (12, "Aferist"),
            (12, "Suidsid"),
            (13, "G'azabkor"),
        ]
    else:
        singleton_specs = [
            (5, "Kezuvchi"),
            (5, "Omadli"),
            (7, "Daydi"),
            (10, "Qotil"),
            (10, "Janob"),
            (10, "Sotqin"),
            (12, "Aferist"),
            (12, "Suidsid"),
            (13, "G'azabkor"),
        ]
        random.shuffle(singleton_specs)

    for min_n, r in singleton_specs:
        if len(roles) >= max_non_tinch:
            break
        if n < min_n:
            continue
        if r in disabled:
            continue
        if r in roles:
            continue
        roles.append(r)

    # Keep number of plain civilians low by filling as many specials as possible.
    # Only if we still have empty slots, fill with Tinch.
    while len(roles) < n:
        roles.append("Tinch")

    random.shuffle(roles)
    return roles


def main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="👤 Profil", callback_data="menu:profile")],
            [InlineKeyboardButton(text="🎮 O'yin", callback_data="menu:game")],
            [InlineKeyboardButton(text="🎭 Rollar", callback_data="menu:roles")],
            [InlineKeyboardButton(text="🏆 Top", callback_data="menu:top")],
            [InlineKeyboardButton(text="🛒 Do'kon", callback_data="menu:shop")],
            [InlineKeyboardButton(text="🌐 Til", callback_data="menu:lang")],
            [InlineKeyboardButton(text="🔙 Guruhga qaytish", callback_data="menu:back")],
            [InlineKeyboardButton(text="📚 Yordam", callback_data="menu:help")],
        ]
    )


def shop_kb() -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for key, it in SHOP_ITEMS.items():
        cur = "💵" if it["currency"] == "money" else "💎"
        rows.append([InlineKeyboardButton(text=f"{it['title']} — {it['price']}{cur}", callback_data=f"buy:{key}")])
    rows.append([InlineKeyboardButton(text="❌ Yopish", callback_data="menu:start")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def roles_kb() -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for role in ROLES_LIST:
        rows.append([InlineKeyboardButton(text=f"{ROLE_EMOJI.get(role, '🎭')} {role}", callback_data=f"role:{role}")])
    rows.append([InlineKeyboardButton(text="⬅️ Orqaga", callback_data="menu:start")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def roles_overview_text() -> str:
    return (
        "🎭 O'yindagi rollar:\n\n"
        "  1). 👨🏼 Tinch aholilar tomonda: 👨🏼 Tinch aholi, 🤞🏼 Omadli, 💃 Kezuvchi, 👮🏻‍♂ Serjant, 🕵🏻‍♂ Komissar katani, "
        "👨🏻‍⚕️ Doktor, 🧙‍♂ Daydi, 💣 Afsungar, 🐺 Bo'ri, 🎖 Janob.\n"
        "  2). 🤵🏼 Mafiya tomonda: 🤵🏻 Don, 🤵🏼 Mafiya, 👨‍💼 Advokat, 👩🏼‍💻 Jurnalist, 🕴️ убийца.\n"
        "  3). Yakka (neytral) rollar: 🔪 Qotil, 🧟 G'azabkor, 🧙‍♂️ Sehrgar, 🤦🏼 Suicid, 🤹🏻 Aferist.\n"
        "  4). Qo'shimcha: 🤓 Sotqin (komissar tekshirgan rolni ochib beradi).\n\n"
        "Rol haqida ma'lumot olish uchun pastdagi tugmalardan birini bosing."
    )


def profile_kb(u: Dict[str, Any]) -> InlineKeyboardMarkup:
    def onoff(flag: bool) -> str:
        return "🔵 ON" if flag else "🔴 OFF"

    protect_on = bool(u.get("use_protect", True))
    anti_on = bool(u.get("use_anti_killer", True))
    vote_on = bool(u.get("use_vote_protect", True))
    gun_on = bool(u.get("use_gun", True))

    rows: List[List[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(text=f"📁 {onoff(protect_on)}", callback_data="toggle:protect"),
            InlineKeyboardButton(text=f"🛡 {onoff(anti_on)}", callback_data="toggle:anti_killer"),
            InlineKeyboardButton(text=f"🧰 {onoff(vote_on)}", callback_data="toggle:vote_protect"),
        ],
        [
            InlineKeyboardButton(text=f"🔫 {onoff(gun_on)}", callback_data="toggle:gun"),
        ],
        [InlineKeyboardButton(text="Do'kon", callback_data="menu:shop")],
        [
            InlineKeyboardButton(text="Xarid qilish 💵", callback_data="shop:money"),
            InlineKeyboardButton(text="Xarid qilish 💎", callback_data="shop:diamond"),
        ],
        [InlineKeyboardButton(text="Yangiliklar", callback_data="news")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def buy_money_to_diamond_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="💎 1 - 💵 200", callback_data="buycoins:1:200"),
                InlineKeyboardButton(text="💎 2 - 💵 500", callback_data="buycoins:2:500"),
            ],
            [
                InlineKeyboardButton(text="💎 3 - 💵 750", callback_data="buycoins:3:750"),
                InlineKeyboardButton(text="💎 4 - 💵 1000", callback_data="buycoins:4:1000"),
            ],
            [
                InlineKeyboardButton(text="💎 18 - 💵 5000", callback_data="buycoins:18:5000"),
                InlineKeyboardButton(text="💎 30 - 💵 10000", callback_data="buycoins:30:10000"),
            ],
            [InlineKeyboardButton(text="orqaga", callback_data="menu:profile")],
        ]
    )


def buy_diamond_prices_kb() -> InlineKeyboardMarkup:
    def fmt_uzs(n: int) -> str:
        try:
            return f"{int(n):,}".replace(",", " ")
        except Exception:
            return str(n)

    order = [1, 5, 10, 15, 30, 50, 250, 1000]
    order = [d for d in order if d in DIAMOND_PACKS_UZS]
    rows: List[List[InlineKeyboardButton]] = []
    for i in range(0, len(order), 2):
        pair = order[i : i + 2]
        row: List[InlineKeyboardButton] = []
        for d in pair:
            price = int(DIAMOND_PACKS_UZS.get(d, 0))
            row.append(
                InlineKeyboardButton(
                    text=f"💎 {d} - {fmt_uzs(price)} so'm",
                    callback_data=f"buydiamond:{d}",
                )
            )
        rows.append(row)
    rows.append([InlineKeyboardButton(text="orqaga", callback_data="menu:profile")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def buy_diamond_methods_kb(diamonds: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💳 Karta orqali olish", callback_data=f"buydiamond_card:{diamonds}")],
            [InlineKeyboardButton(text="⬅️ Orqaga", callback_data="shop:diamond")],
        ]
    )


def bot_open_kb() -> InlineKeyboardMarkup:
    if not BOT_USERNAME:
        return InlineKeyboardMarkup(inline_keyboard=[])
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Bot-ga o'tish", url=f"https://t.me/{BOT_USERNAME}")]]
    )


def _role_counts_line(game: Dict[str, Any]) -> str:
    roles = [_role_of(game, uid) for uid in _alive_ids(game)]
    counts: Dict[str, int] = {}
    for r in roles:
        counts[r] = counts.get(r, 0) + 1
    order = ["Tinch", "Komissar", "Don", "Mafia", "Doktor", "Serjant", "Kezuvchi", "Advokat", "Qotil", "Afsungar", "Daydi", "Bo'ri"]
    parts = []
    for r in order:
        if r in counts:
            parts.append(f"{r} - {counts[r]}")
    return ", ".join(parts)


def admin_settings_main_kb(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Giveaway", callback_data=f"adminset:giveaway:{chat_id}")],
            [InlineKeyboardButton(text="Rollar", callback_data=f"adminset:roles:{chat_id}")],
            [InlineKeyboardButton(text="Vaqtlar", callback_data=f"adminset:times:{chat_id}")],
            [InlineKeyboardButton(text="Jimlik", callback_data=f"adminset:silence:{chat_id}")],
            [InlineKeyboardButton(text="Qurollar", callback_data=f"adminset:weapons:{chat_id}")],
            [InlineKeyboardButton(text="Boshqa sozlamalar", callback_data=f"adminset:other:{chat_id}")],
            [InlineKeyboardButton(text="Til", callback_data=f"adminset:lang:{chat_id}")],
            [InlineKeyboardButton(text="Boshqaruv paneli", callback_data=f"adminset:panel:{chat_id}")],
            [InlineKeyboardButton(text="Chiqish", callback_data="menu:start")],
        ]
    )


def group_admin_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🎮 Ro'yxat boshlash", callback_data="group:game")],
            [InlineKeyboardButton(text="🚀 O'yinni boshlash", callback_data="group:start")],
            [InlineKeyboardButton(text="🛑 O'yinni to'xtatish", callback_data="group:stop")],
            [InlineKeyboardButton(text="⏰ Vaqtni uzaytirish", callback_data="group:extend")],
            [InlineKeyboardButton(text="⚙️ Sozlamalar", callback_data="group:settings")],
        ]
    )


def group_lang_kb(chat_id: int) -> InlineKeyboardMarkup:
    g = get_group(chat_id)
    cur = str(g.get("language", "uz"))
    rows: List[List[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(text=("✅ " if cur == "uz" else "") + "O'zbek", callback_data=f"grouplang:set:{chat_id}:uz"),
            InlineKeyboardButton(text=("✅ " if cur == "ru" else "") + "Русский", callback_data=f"grouplang:set:{chat_id}:ru"),
        ],
        [
            InlineKeyboardButton(text=("✅ " if cur == "az" else "") + "Azərbaycanca", callback_data=f"grouplang:set:{chat_id}:az"),
            InlineKeyboardButton(text=("✅ " if cur == "tr" else "") + "Türkçe", callback_data=f"grouplang:set:{chat_id}:tr"),
        ],
        [InlineKeyboardButton(text="⬅️ Orqaga", callback_data=f"adminset:back:{chat_id}")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def settings_kb(chat_id: int) -> InlineKeyboardMarkup:
    g = get_group(chat_id)

    def row_time(title: str, key: str) -> List[InlineKeyboardButton]:
        return [
            InlineKeyboardButton(text=f"{title}: {g.get(key, 0)}s", callback_data=f"set:show:{chat_id}:{key}"),
        ]

    rows: List[List[InlineKeyboardButton]] = [
        row_time("⏰ Ro'yxat", "registration_seconds"),
        row_time("🌃 Tun", "night_seconds"),
        row_time("☀️ Kun", "day_seconds"),
        row_time("🗣 Muhokama", "discussion_seconds"),
        row_time("✨ Magic", "magic_seconds"),
        [
            InlineKeyboardButton(
                text=f"👥 TeamGame: {'✅' if g.get('allow_teamgame', True) else '❌'}",
                callback_data=f"set:toggle:{chat_id}:allow_teamgame",
            )
        ],
        [InlineKeyboardButton(text="🧹 Default", callback_data=f"set:default:{chat_id}")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def time_adjust_kb(chat_id: int, key: str) -> InlineKeyboardMarkup:
    g = get_group(chat_id)
    cur = int(g.get(key, 0))
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="-10", callback_data=f"set:adj:{chat_id}:{key}:-10"),
                InlineKeyboardButton(text="-5", callback_data=f"set:adj:{chat_id}:{key}:-5"),
                InlineKeyboardButton(text="-1", callback_data=f"set:adj:{chat_id}:{key}:-1"),
            ],
            [InlineKeyboardButton(text=f"Hozir: {cur}s", callback_data="noop")],
            [
                InlineKeyboardButton(text="+1", callback_data=f"set:adj:{chat_id}:{key}:+1"),
                InlineKeyboardButton(text="+5", callback_data=f"set:adj:{chat_id}:{key}:+5"),
                InlineKeyboardButton(text="+10", callback_data=f"set:adj:{chat_id}:{key}:+10"),
            ],
            [InlineKeyboardButton(text="⬅️ Orqaga", callback_data=f"set:main:{chat_id}")],
        ]
    )


async def setup_commands_menu() -> None:
    await bot.set_my_commands(
        commands=[
            BotCommand(command="start", description="🎮 Boshlash"),
            BotCommand(command="menu", description="📋 Menu"),
            BotCommand(command="profile", description="👤 Profil"),
            BotCommand(command="game", description="🎮 O'yin"),
            BotCommand(command="roles", description="🎭 Rollar"),
            BotCommand(command="shop", description="🛒 Do'kon"),
            BotCommand(command="top", description="🏆 Top"),
            BotCommand(command="lang", description="🌐 Til"),
            BotCommand(command="help", description="📚 Yordam"),
            BotCommand(command="back", description="🔙 Guruhga qaytish"),
        ],
        scope=BotCommandScopeAllPrivateChats(),
    )

    await bot.set_my_commands(
        commands=[
            BotCommand(command="game", description="🎮 Ro'yxat"),
            BotCommand(command="start", description="🚀 Boshlash"),
            BotCommand(command="stop", description="🛑 To'xtatish"),
            BotCommand(command="extend", description="⏰ Uzaytirish"),
            BotCommand(command="settings", description="⚙️ Sozlamalar"),
            BotCommand(command="profile", description="👤 Profil"),
            BotCommand(command="roles", description="🎭 Rollar"),
            BotCommand(command="shop", description="🛒 Do'kon"),
            BotCommand(command="top", description="🏆 Top"),
            BotCommand(command="help", description="📚 Yordam"),
            BotCommand(command="leave", description="🚪 Chiqish"),
        ],
        scope=BotCommandScopeAllGroupChats(),
    )


@dp.message(Command("start"), F.chat.type == "private")
async def cmd_start(msg: Message) -> None:
    get_user(msg.from_user.id, msg.from_user.full_name)
    u = get_user(msg.from_user.id, msg.from_user.full_name)

    parts = (msg.text or "").split(maxsplit=1)
    payload = parts[1].strip() if len(parts) > 1 else ""
    if payload.startswith("vote_"):
        try:
            chat_id = int(payload.split("_", 1)[1])
        except Exception:
            chat_id = 0
        if chat_id:
            game = get_game(chat_id)
            uid = str(msg.from_user.id)
            if game.get("started") and game.get("phase") == PHASE_LYNCH and uid in _alive_ids(game):
                try:
                    await msg.answer(
                        "Aybdorlarni topish va jazolash vaqti keldi!\nKimni osish kerak deb xisoblaysiz?",
                        reply_markup=with_group_button(chat_id, _vote_kb(chat_id, uid)),
                    )
                except Exception:
                    pass
                return
    if payload.startswith("join_"):
        try:
            chat_id = int(payload.split("_", 1)[1])
        except Exception:
            chat_id = 0
        if chat_id:
            game = get_game(chat_id)
            if game.get("phase") == PHASE_REG and not game.get("started"):
                now = asyncio.get_event_loop().time()
                if now <= float(game.get("reg_deadline", 0)):
                    uid = str(msg.from_user.id)
                    gcfg = get_group(chat_id)
                    max_players = int(gcfg.get("max_players", 30))
                    if uid not in game.get("players", {}) and len(game.get("players", {})) < max_players:
                        game["players"][uid] = msg.from_user.full_name
                        if uid not in game.get("alive", []):
                            game.setdefault("alive", []).append(uid)
                        try:
                            await _edit_group_status(chat_id, reply_markup=await reg_join_kb(chat_id))
                        except Exception:
                            pass

                    rt = game.get("reg_task")
                    if not rt or getattr(rt, "done", lambda: False)():
                        try:
                            if rt:
                                rt.cancel()
                        except Exception:
                            pass
                        try:
                            game["reg_task"] = asyncio.create_task(_registration_timeout(chat_id, float(game.get("reg_deadline", 0))))
                        except Exception:
                            pass

                    try:
                        await msg.answer(
                            "Siz o'yinga omadli qo'shildingiz :) ",
                            reply_markup=group_open_kb(chat_id) if group_url(chat_id) else None,
                        )
                    except Exception:
                        pass
                    return

    await msg.answer("Pilih bahasa", reply_markup=start_lang_kb(str(u.get("language", "uz"))))


@dp.message(Command("menu"))
async def cmd_menu(msg: Message) -> None:
    if msg.chat.type in {"group", "supergroup"}:
        return
    await msg.answer("📋 Menu:", reply_markup=main_menu_kb())


def _find_user_game(user_id: int) -> Optional[int]:
    suid = str(user_id)
    for cid, g in GAMES.items():
        try:
            if not g.get("started"):
                continue
            if suid in g.get("players", {}):
                return int(cid)
        except Exception:
            continue
    return None


@dp.message(Command("mafia"), F.chat.type == "private")
async def cmd_mafia_chat(msg: Message) -> None:
    cid = _find_user_game(msg.from_user.id)
    if not cid:
        return
    game = get_game(cid)
    uid = str(msg.from_user.id)
    role = _role_of(game, uid)
    if role not in {"Don", "Mafia", "Advokat"}:
        return
    parts = (msg.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        return
    payload = parts[1].strip()
    teammates = [u for u in _alive_ids(game) if _role_of(game, u) in {"Don", "Mafia", "Advokat"}]
    for tuid in teammates:
        if group_url(cid):
            await safe_dm(
                int(tuid),
                f"🤵 Mafia chat: {_player_label(game, uid)}\n\n{payload}",
                reply_markup=group_open_kb(cid),
            )
        else:
            await safe_dm(int(tuid), f"🤵 Mafia chat: {_player_label(game, uid)}\n\n{payload}")


@dp.message(Command("kom"), F.chat.type == "private")
async def cmd_kom_chat(msg: Message) -> None:
    cid = _find_user_game(msg.from_user.id)
    if not cid:
        return
    game = get_game(cid)
    uid = str(msg.from_user.id)
    role = _role_of(game, uid)
    if role not in {"Komissar", "Serjant"}:
        return
    parts = (msg.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        return
    payload = parts[1].strip()
    teammates = [u for u in _alive_ids(game) if _role_of(game, u) in {"Komissar", "Serjant"}]
    for tuid in teammates:
        if group_url(cid):
            await safe_dm(
                int(tuid),
                f"🕵🏻‍♂ Komissar chat: {_player_label(game, uid)}\n\n{payload}",
                reply_markup=group_open_kb(cid),
            )
        else:
            await safe_dm(int(tuid), f"🕵🏻‍♂ Komissar chat: {_player_label(game, uid)}\n\n{payload}")


@dp.message(Command("profile"))
async def cmd_profile(msg: Message) -> None:
    u = get_user(msg.from_user.id, msg.from_user.full_name)
    safe_name = html.escape(str(msg.from_user.full_name or "User"))
    text = (
        f"⭐️ ID: {msg.from_user.id}\n"
        f"👤 <a href=\"tg://user?id={msg.from_user.id}\">{safe_name}</a>\n\n"
        f"💵 Dollar: {fmt_money(u.get('money', 0))}\n"
        f"💎 Olmos: {u.get('diamonds', 0)}\n\n"
        f"🛡 Himoya: {u.get('protect', 0)}\n"
        f"⛑️ Qotildan himoya: {u.get('anti_killer', 0)}\n"
        f"⚖️ Ovoz berishni himoya qilish: {u.get('vote_protect', 0)}\n"
        f"🔫 Miltiq: {u.get('gun', 0)}\n\n"
        f"🎭 Maska: {u.get('mask', 0)}\n"
        f"📄 Soxta hujat: {u.get('fake_docs', 0)}\n"
        f"🃏 Keyingi o'yindagi rolingiz: {u.get('next_role', '-')}\n\n"
        f"🏅 G'alaba: {u.get('wins', 0)}\n"
        f"🎲 Jami o'yinlar: {u.get('games', 0)}"
    )
    if msg.chat.type in {"group", "supergroup"}:
        ok = await try_dm(msg.from_user.id, text, reply_markup=profile_kb(u))
        if not ok:
            try:
                await msg.answer("⬇️", reply_markup=await bot_link_kb())
            except Exception:
                pass
        return

    await msg.answer(text, reply_markup=profile_kb(u), parse_mode="HTML")


@dp.message(F.chat.type.in_({"group", "supergroup"}), F.text.regexp(r"^/profile(@\w+)?(\s|$)"))
async def alias_profile_group(msg: Message) -> None:
    return await cmd_profile(msg)


@dp.message(Command("shop"))
async def cmd_shop(msg: Message) -> None:
    get_user(msg.from_user.id, msg.from_user.full_name)
    if msg.chat.type in {"group", "supergroup"}:
        ok = await try_dm(msg.from_user.id, "🛒 Do'kon:", reply_markup=shop_kb())
        if not ok:
            try:
                await msg.answer("⬇️", reply_markup=await bot_link_kb())
            except Exception:
                pass
        return

    await msg.answer("🛒 Do'kon:", reply_markup=shop_kb())


@dp.message(F.chat.type.in_({"group", "supergroup"}), F.text.regexp(r"^/shop(@\w+)?(\s|$)"))
async def alias_shop_group(msg: Message) -> None:
    return await cmd_shop(msg)


@dp.message(Command("roles"))
async def cmd_roles(msg: Message) -> None:
    if msg.chat.type in {"group", "supergroup"}:
        ok = await try_dm(msg.from_user.id, roles_overview_text(), reply_markup=roles_kb())
        if not ok:
            try:
                await msg.answer("⬇️", reply_markup=await bot_link_kb())
            except Exception:
                pass
        return

    await msg.answer(roles_overview_text(), reply_markup=roles_kb())


@dp.message(F.chat.type.in_({"group", "supergroup"}), F.text.regexp(r"^/roles(@\w+)?(\s|$)"))
async def alias_roles_group(msg: Message) -> None:
    return await cmd_roles(msg)


@dp.message(Command("help"))
async def cmd_help(msg: Message) -> None:
    text = (
        "📚 Yordam:\n\n"
        "- Guruhda admin /game yozadi (ro'yxat ochiladi)\n"
        "- O'yinchilar guruhdagi 💡 Qo'shilish tugmasini bosadi\n"
        "- Admin /start bilan o'yinni boshlaydi\n\n"
        "Eslatma: Guruhda ko'p buyruqlar javobi adminning shaxsiy chatiga boradi."
    )
    if msg.chat.type in {"group", "supergroup"}:
        ok = await try_dm(msg.from_user.id, text)
        if not ok:
            try:
                await msg.answer("⬇️", reply_markup=await bot_link_kb())
            except Exception:
                pass
        return

    await msg.answer(text)


@dp.message(F.chat.type.in_({"group", "supergroup"}), F.text.regexp(r"^/help(@\w+)?(\s|$)"))
async def alias_help_group(msg: Message) -> None:
    return await cmd_help(msg)


@dp.message(Command("top"))
async def cmd_top(msg: Message) -> None:
    n = _top_n_from_text(msg.text or "", default_n=10)
    users = load_users()

    if msg.chat.type in {"group", "supergroup"}:
        chat_id = msg.chat.id
        gk = str(chat_id)
        scored: List[Tuple[int, Dict[str, Any]]] = []
        for u in (users or {}).values():
            try:
                gs = dict(u.get("group_stats", {}) or {})
                st = dict(gs.get(gk, {}) or {})
                w = int(st.get("wins", 0))
            except Exception:
                w = 0
            if w > 0:
                scored.append((w, u))
        scored.sort(key=lambda t: int(t[0]), reverse=True)
        arr = [u for _, u in scored[:n]]
        if not arr:
            text = "🏆 Top o'yinchilar: hali yo'q."
        else:
            text = f"🏆 Top {min(n, 30)}:\n\n" + "\n".join(
                f"{i+1}. {u.get('name','User')} — {int(dict((u.get('group_stats', {}) or {}).get(gk, {}) or {}).get('wins', 0))}"
                for i, u in enumerate(arr)
            )
        try:
            await msg.answer(text)
        except Exception:
            pass
        return

    # private/global
    arr = sorted((users or {}).values(), key=lambda u: int(u.get("wins", 0)), reverse=True)[:n]
    if not arr:
        text = "🏆 Top o'yinchilar: hali yo'q."
    else:
        text = f"🏆 Top {min(n, 30)}:\n\n" + "\n".join(
            f"{i+1}. {u.get('name','User')} — {u.get('wins',0)}" for i, u in enumerate(arr)
        )
    await msg.answer(text)


@dp.message(F.text.regexp(r"^/top(10|20|30)(@\w+)?(\s|$)"))
async def alias_top_10_20_30(msg: Message) -> None:
    return await cmd_top(msg)


@dp.message(Command("lang"))
async def cmd_lang(msg: Message) -> None:
    u = get_user(msg.from_user.id, msg.from_user.full_name)
    cur = u.get("language", "uz")
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=("✅ " if cur == "uz" else "") + "O'zbek", callback_data="setlang:uz")],
            [InlineKeyboardButton(text=("✅ " if cur == "ru" else "") + "Русский", callback_data="setlang:ru")],
            [InlineKeyboardButton(text="⬅️ Orqaga", callback_data="menu:start")],
        ]
    )
    await msg.answer("🌐 Tilni tanlang:", reply_markup=kb)


@dp.callback_query(F.data == "noop")
async def cb_noop(cb: CallbackQuery) -> None:
    await cb.answer()


@dp.callback_query(F.data.startswith("adminrole:"))
async def cb_admin_role_info(cb: CallbackQuery) -> None:
    parts = cb.data.split(":")
    if len(parts) != 3:
        return await cb.answer()
    chat_id = int(parts[1])
    role = parts[2]
    if role not in ROLES_LIST:
        return await cb.answer("❌", show_alert=True)
    if not await is_admin(chat_id, cb.from_user.id):
        return await cb.answer("❌ Faqat admin", show_alert=True)
    on = _role_enabled(chat_id, role)
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=("🟢 ON" if on else "🔴 OFF"), callback_data=f"adminrole:toggle:{chat_id}:{role}")],
            [InlineKeyboardButton(text="⬅️ Orqaga", callback_data=f"adminset:roles:{chat_id}")],
        ]
    )
    try:
        await cb.message.edit_text(f"{ROLE_EMOJI.get(role, '🎭')} {role}\n\n{ROLE_DESC_UZ.get(role,'—')}", reply_markup=kb)
    except Exception:
        pass
    await cb.answer()


@dp.callback_query(F.data.startswith("adminrole:toggle:"))
async def cb_admin_role_toggle(cb: CallbackQuery) -> None:
    parts = cb.data.split(":")
    if len(parts) != 4:
        return await cb.answer()
    chat_id = int(parts[2])
    role = parts[3]
    if role not in ROLES_LIST:
        return await cb.answer("❌", show_alert=True)
    if not await is_admin(chat_id, cb.from_user.id):
        return await cb.answer("❌ Faqat admin", show_alert=True)

    _toggle_role(chat_id, role)
    on = _role_enabled(chat_id, role)
    await cb.answer("✅")
    try:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text=("🟢 ON" if on else "🔴 OFF"), callback_data=f"adminrole:toggle:{chat_id}:{role}")],
                [InlineKeyboardButton(text="⬅️ Orqaga", callback_data=f"adminset:roles:{chat_id}")],
            ]
        )
        await cb.message.edit_reply_markup(reply_markup=kb)
    except Exception:
        pass


@dp.callback_query(F.data.startswith("rolecfg:"))
async def cb_rolecfg(cb: CallbackQuery) -> None:
    parts = cb.data.split(":")
    if len(parts) < 4:
        return await cb.answer()
    action = parts[1]
    key = parts[2]
    chat_id = int(parts[3])
    if not await is_admin(chat_id, cb.from_user.id):
        return await cb.answer("❌ Faqat admin", show_alert=True)

    if action == "open":
        if key == "max":
            await cb.message.edit_text("Bir o'yinda ko'pi bilan qancha o'yinchi o'ynashi mumkin?", reply_markup=rolecfg_max_players_kb(chat_id))
            return await cb.answer()
        if key == "ratio":
            await cb.message.edit_text(
                "Mafiya sonining nisbatini tanlang.\n\n"
                "\"Ko'proq\" variant bilan har 3-o'yinchi mafia bo'ladi, \"Kamroq\" variant bilan esa har 4-o'yinchi bo'ladi.",
                reply_markup=rolecfg_ratio_kb(chat_id),
            )
            return await cb.answer()
        return await cb.answer()

    if action == "set":
        if len(parts) < 5:
            return await cb.answer()
        val = parts[4]
        g = get_group(chat_id)
        if key == "max":
            g["max_players"] = int(val)
            update_group(chat_id, g)
            await cb.answer("✅")
            await cb.message.edit_reply_markup(reply_markup=rolecfg_max_players_kb(chat_id))
            return
        if key == "ratio":
            if val not in {"1/3", "1/4"}:
                return await cb.answer("❌", show_alert=True)
            g["mafia_ratio"] = val
            update_group(chat_id, g)
            await cb.answer("✅")
            await cb.message.edit_reply_markup(reply_markup=rolecfg_ratio_kb(chat_id))
            return

    if action in {"back", "noop"}:
        await cb.message.edit_text("Boshqa sozlamalar", reply_markup=other_main_kb(chat_id))
        return await cb.answer()

    await cb.answer()


@dp.callback_query(F.data.startswith("silence:toggle:"))
async def cb_silence_toggle(cb: CallbackQuery) -> None:
    parts = cb.data.split(":")
    if len(parts) != 4:
        return await cb.answer()
    chat_id = int(parts[2])
    key = parts[3]
    if not await is_admin(chat_id, cb.from_user.id):
        return await cb.answer("❌ Faqat admin", show_alert=True)
    g = get_group(chat_id)
    g[key] = not bool(g.get(key, False))
    update_group(chat_id, g)
    await cb.answer("✅", show_alert=True)
    try:
        await cb.message.edit_reply_markup(reply_markup=silence_item_kb(chat_id, key))
    except Exception:
        pass
    await apply_silence_rules(chat_id)


@dp.callback_query(F.data.startswith("silence:open:"))
async def cb_silence_open(cb: CallbackQuery) -> None:
    parts = cb.data.split(":")
    if len(parts) != 4:
        return await cb.answer()
    chat_id = int(parts[2])
    section = parts[3]
    if not await is_admin(chat_id, cb.from_user.id):
        return await cb.answer("❌ Faqat admin", show_alert=True)

    if section == "dead":
        await cb.message.edit_text("O'lganlar uchun", reply_markup=silence_item_kb(chat_id, "silence_dead"))
        return await cb.answer()
    if section == "sleep":
        await cb.message.edit_text("Uxlayotganlar uchun", reply_markup=silence_item_kb(chat_id, "silence_night"))
        return await cb.answer()
    if section == "inactive":
        await cb.message.edit_text("O'ynamayotganlar uchun", reply_markup=silence_item_kb(chat_id, "silence_inactive"))
        return await cb.answer()
    await cb.answer()


@dp.message(F.chat.type.in_({"group", "supergroup"}), F.text, ~F.text.startswith("/"))
async def enforce_inactive_silence(msg: Message) -> None:
    # Mute non-players during active game if enabled.
    if not msg.from_user:
        return
    chat_id = msg.chat.id
    game = get_game(chat_id)
    if not game.get("started"):
        return
    g = get_group(chat_id)
    if not bool(g.get("silence_inactive", False)):
        return
    if not await bot_has_required_rights(chat_id):
        return

    uid = str(msg.from_user.id)
    if uid in game.get("players", {}):
        return
    if await is_admin(chat_id, msg.from_user.id):
        return

    await _restrict_user(chat_id, msg.from_user.id, False)


@dp.callback_query(F.data.startswith("setlang:"))
async def cb_setlang(cb: CallbackQuery) -> None:
    code = cb.data.split(":", 1)[1]
    if code not in {"uz", "ru", "az", "tr", "en", "uk", "kk", "id"}:
        return await cb.answer("❌", show_alert=True)
    u = get_user(cb.from_user.id, cb.from_user.full_name)
    u["language"] = code
    update_user(cb.from_user.id, u)
    await cb.answer("✅")
    try:
        await cb.message.edit_text("👋 Mafia botga xush kelibsiz!", reply_markup=main_menu_kb())
    except Exception:
        return


@dp.callback_query(F.data.startswith("menu:"))
async def cb_menu(cb: CallbackQuery) -> None:
    action = cb.data.split(":", 1)[1]

    if action == "start":
        return await cb.message.edit_text("👋 Menu:", reply_markup=main_menu_kb())

    if action == "profile":
        u = get_user(cb.from_user.id, cb.from_user.full_name)
        safe_name = html.escape(str(cb.from_user.full_name or "User"))
        text = (
            f"⭐️ ID: {cb.from_user.id}\n"
            f"👤 <a href=\"tg://user?id={cb.from_user.id}\">{safe_name}</a>\n\n"
            f"💵 Dollar: {fmt_money(u.get('money', 0))}\n"
            f"💎 Olmos: {u.get('diamonds', 0)}\n\n"
            f"🛡 Himoya: {u.get('protect', 0)}\n"
            f"⛑️ Qotildan himoya: {u.get('anti_killer', 0)}\n"
            f"⚖️ Ovoz berishni himoya qilish: {u.get('vote_protect', 0)}\n"
            f"🔫 Miltiq: {u.get('gun', 0)}\n\n"
            f"🎭 Maska: {u.get('mask', 0)}\n"
            f"📄 Soxta hujat: {u.get('fake_docs', 0)}\n"
            f"🃏 Keyingi o'yindagi rolingiz: {u.get('next_role', '-')}\n\n"
            f"🏅 G'alaba: {u.get('wins', 0)}\n"
            f"🎲 Jami o'yinlar: {u.get('games', 0)}"
        )
        await cb.message.edit_text(text, reply_markup=profile_kb(u), parse_mode="HTML")
        return await cb.answer()

    if action == "shop":
        await cb.message.edit_text("🛒 Do'kon:", reply_markup=shop_kb())
        return await cb.answer()

    if action == "roles":
        await cb.message.edit_text(roles_overview_text(), reply_markup=roles_kb())
        return await cb.answer()

    if action == "help":
        await cb.message.edit_text(
            "📚 Yordam:\n\nGuruhda admin /game, keyin /start.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="⬅️ Orqaga", callback_data="menu:start")]]
            ),
        )
        return await cb.answer()

    if action == "lang":
        u = get_user(cb.from_user.id, cb.from_user.full_name)
        cur = u.get("language", "uz")
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text=("✅ " if cur == "uz" else "") + "O'zbek", callback_data="setlang:uz")],
                [InlineKeyboardButton(text=("✅ " if cur == "ru" else "") + "Русский", callback_data="setlang:ru")],
                [InlineKeyboardButton(text="⬅️ Orqaga", callback_data="menu:start")],
            ]
        )
        await cb.message.edit_text("🌐 Tilni tanlang:", reply_markup=kb)
        return await cb.answer()

    if action == "game":
        active = []
        for cid, game in GAMES.items():
            if str(cb.from_user.id) in game.get("players", {}):
                active.append((cid, game))
        if not active:
            await cb.message.edit_text(
                "🎮 Siz hech qanday o'yinda qatnashmaysiz.\n\nGuruhda /game buyrug'ini bosing va ro'yxatdan o'ting!",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[[InlineKeyboardButton(text="⬅️ Orqaga", callback_data="menu:start")]]
                ),
            )
            return await cb.answer()
        text = "🎮 Faol o'yinlar:\n\n" + "\n".join(
            f"- {cid} ({len(g['players'])} o'yinchi)" for cid, g in active
        )
        await cb.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="⬅️ Orqaga", callback_data="menu:start")]]
            ),
        )
        return await cb.answer()

    if action == "top":
        users = load_users()
        arr = sorted(users.values(), key=lambda u: int(u.get("wins", 0)), reverse=True)[:10]
        if not arr:
            text = "🏆 Top: hali yo'q."
        else:
            text = "🏆 Top 10:\n\n" + "\n".join(
                f"{i+1}. {u.get('name','User')} — {u.get('wins',0)}" for i, u in enumerate(arr)
            )
        await cb.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="⬅️ Orqaga", callback_data="menu:start")]]
            ),
        )
        return await cb.answer()

    if action == "back":
        user_games = [cid for cid, g in GAMES.items() if str(cb.from_user.id) in g.get("players", {})]
        if not user_games:
            await cb.message.edit_text(
                "❌ Siz hech qanday o'yinda qatnashmaysiz.",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[[InlineKeyboardButton(text="⬅️ Orqaga", callback_data="menu:start")]]
                ),
            )
            return await cb.answer()
        cid = user_games[0]
        url = f"https://t.me/c/{str(cid)[4:]}" if str(cid).startswith("-100") else f"https://t.me/{cid}"
        await cb.message.edit_text(
            "🔙 Guruhga qaytish:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Guruh", url=url)]]),
        )
        return await cb.answer()

    await cb.answer()


@dp.callback_query(F.data.startswith("toggle:"))
async def cb_toggle(cb: CallbackQuery) -> None:
    field = cb.data.split(":", 1)[1]
    u = get_user(cb.from_user.id, cb.from_user.full_name)
    if field not in {"protect", "anti_killer", "vote_protect", "gun"}:
        return await cb.answer()

    use_map = {
        "protect": "use_protect",
        "anti_killer": "use_anti_killer",
        "vote_protect": "use_vote_protect",
        "gun": "use_gun",
    }
    use_key = use_map[field]

    u[use_key] = not bool(u.get(use_key, True))
    update_user(cb.from_user.id, u)
    await cb.answer("✅")
    try:
        safe_name = html.escape(str(cb.from_user.full_name or "User"))
        text = (
            f"⭐️ ID: {cb.from_user.id}\n"
            f"👤 <a href=\"tg://user?id={cb.from_user.id}\">{safe_name}</a>\n\n"
            f"💵 Dollar: {fmt_money(u.get('money', 0))}\n"
            f"💎 Olmos: {u.get('diamonds', 0)}\n\n"
            f"🛡 Himoya: {u.get('protect', 0)}\n"
            f"⛑️ Qotildan himoya: {u.get('anti_killer', 0)}\n"
            f"⚖️ Ovoz berishni himoya qilish: {u.get('vote_protect', 0)}\n"
            f"🔫 Miltiq: {u.get('gun', 0)}\n\n"
            f"🎭 Maska: {u.get('mask', 0)}\n"
            f"📄 Soxta hujat: {u.get('fake_docs', 0)}\n"
            f"🃏 Keyingi o'yindagi rolingiz: {u.get('next_role', '-')}\n\n"
            f"🏅 G'alaba: {u.get('wins', 0)}\n"
            f"🎲 Jami o'yinlar: {u.get('games', 0)}"
        )
        await cb.message.edit_text(text, reply_markup=profile_kb(u), parse_mode="HTML")
    except Exception:
        return


@dp.callback_query(F.data.startswith("buy:"))
async def cb_buy(cb: CallbackQuery) -> None:
    key = cb.data.split(":", 1)[1]
    if key not in SHOP_ITEMS:
        return await cb.answer("❌", show_alert=True)

    it = SHOP_ITEMS[key]
    u = get_user(cb.from_user.id, cb.from_user.full_name)
    price = int(it["price"])
    cur = it["currency"]

    if cur == "money":
        if int(u.get("money", 0)) < price:
            return await cb.answer("❌ Dollar yetarli emas", show_alert=True)
        u["money"] = int(u.get("money", 0)) - price
    else:
        if int(u.get("diamonds", 0)) < price:
            return await cb.answer("❌ Olmos yetarli emas", show_alert=True)
        u["diamonds"] = int(u.get("diamonds", 0)) - price

    field = it["field"]
    u[field] = int(u.get(field, 0)) + int(it.get("qty", 1))
    u["total_spent"] = int(u.get("total_spent", 0)) + price
    update_user(cb.from_user.id, u)
    await cb.answer("✅ Sotib olindi", show_alert=True)


@dp.callback_query(F.data.startswith("role:"))
async def cb_role(cb: CallbackQuery) -> None:
    role = cb.data.split(":", 1)[1]
    if role not in ROLE_DESC_UZ:
        return await cb.answer()

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Rollar", callback_data="menu:roles")],
            [InlineKeyboardButton(text="🏠 Menu", callback_data="menu:start")],
        ]
    )
    await cb.message.edit_text(f"{ROLE_EMOJI.get(role, '🎭')} {role}\n\n{ROLE_DESC_UZ.get(role,'—')}", reply_markup=kb)
    await cb.answer()


@dp.message(Command("game"))
async def cmd_game(msg: Message) -> None:
    async def _admin_open_registration(chat_id: int, admin_id: int) -> None:
        if not await is_admin(chat_id, admin_id):
            ok = await try_dm(admin_id, "❌ Faqat admin")
            if not ok:
                try:
                    await bot.send_message(chat_id, "⬇️", reply_markup=await bot_link_kb())
                except Exception:
                    pass
            return

        if not await bot_has_required_rights(chat_id):
            try:
                await bot.send_message(chat_id, bot_rights_text())
            except Exception:
                pass
            return

        game = get_game(chat_id)
        if game.get("phase") == PHASE_REG:
            kb = await reg_join_kb(chat_id)
            try:
                await _cleanup_reg_announces(chat_id)
                old_mid = game.get("reg_message_id")
                if old_mid:
                    try:
                        await _safe_delete_message(chat_id, int(old_mid))
                    except Exception:
                        pass
                reg_msg = await bot.send_message(chat_id, _group_status_text(chat_id), reply_markup=kb, parse_mode="HTML")
                game["reg_message_id"] = reg_msg.message_id
                if bool(get_group(chat_id).get("pin_registration", True)):
                    await _safe_pin_message(chat_id, reg_msg.message_id)
            except Exception:
                pass

            t = game.get("reg_task")
            if t:
                try:
                    t.cancel()
                except Exception:
                    pass
            game["reg_task"] = asyncio.create_task(_registration_timeout(chat_id, float(game.get("reg_deadline", 0))))
            await try_dm(admin_id, "🔁 Ro'yxat posti yangilandi")
            return

        g = get_group(chat_id)
        game["started"] = False
        game["phase"] = PHASE_REG
        game["players"].clear()
        game["roles"].clear()
        game["alive"].clear()
        game["teams"] = None
        game["reg_deadline"] = asyncio.get_event_loop().time() + int(g.get("registration_seconds", 60))

        kb = await reg_join_kb(chat_id)
        try:
            old_mid = game.get("reg_message_id")
            if old_mid:
                try:
                    await _safe_delete_message(chat_id, int(old_mid))
                except Exception:
                    pass
            reg_msg = await bot.send_message(chat_id, _group_status_text(chat_id), reply_markup=kb, parse_mode="HTML")
            game["reg_message_id"] = reg_msg.message_id
            if bool(get_group(chat_id).get("pin_registration", True)):
                await _safe_pin_message(chat_id, reg_msg.message_id)
        except Exception:
            pass

        await try_dm(admin_id, f"✅ Guruh {chat_id} da ro'yxat boshlandi!")

        t = game.get("reg_task")
        if t:
            try:
                t.cancel()
            except Exception:
                pass
        game["reg_task"] = asyncio.create_task(_registration_timeout(chat_id, float(game.get("reg_deadline", 0))))


    async def _admin_start_game(chat_id: int, admin_id: int) -> None:
        if not await is_admin(chat_id, admin_id):
            await try_dm(admin_id, "❌ Faqat admin")
            return
        if not await bot_has_required_rights(chat_id):
            try:
                await bot.send_message(chat_id, bot_rights_text())
            except Exception:
                pass
            return
        game = get_game(chat_id)
        if game.get("phase") != PHASE_REG or game.get("started"):
            await try_dm(admin_id, "❌ Avval ro'yxatni oching")
            return
        if len(game.get("players", {})) < 4:
            await try_dm(admin_id, "❌ Kamida 4 o'yinchi kerak")
            return

        players = list(game["players"].keys())
        roles = assign_roles(len(players), chat_id)
        for uid, role in zip(players, roles):
            game["roles"][uid] = role

        game["started"] = True
        game["phase"] = PHASE_NIGHT
        game["round"] = 1
        game["started_ts"] = asyncio.get_event_loop().time()
        game["alive"] = list(game["players"].keys())
        game["participants"] = list(game.get("players", {}).keys())

        for uid, role in game["roles"].items():
            if group_url(chat_id):
                await safe_dm(
                    int(uid),
                    f"🎭 Sizning roliz: {ROLE_EMOJI.get(role,'🎭')} {role}",
                    reply_markup=group_open_kb(chat_id),
                )
            else:
                await safe_dm(int(uid), f"🎭 Sizning roliz: {ROLE_EMOJI.get(role,'🎭')} {role}")

        try:
            mid = game.get("reg_message_id")
            if mid:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=int(mid),
                    text="O'yin boshlandi!",
                    reply_markup=InlineKeyboardMarkup(
                        inline_keyboard=[[InlineKeyboardButton(text="Sizning rolingiz", callback_data="menu:profile")]]
                    ),
                )
        except Exception:
            pass

        alive = _alive_ids(game)
        alive_lines = "\n".join(f"{i+1}. {_player_link(game, uid)}" for i, uid in enumerate(alive))
        try:
            me = await bot.get_me()
            await bot.send_message(
                chat_id,
                (
                    "Tirik o'yinchilar:\n"
                    f"{alive_lines}\n\n"
                    f"Ulardan: {_role_counts_line(game)}\n"
                    f"Jami: {len(alive)}"
                ),
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[[InlineKeyboardButton(text="Bot-ga o'tish", url=f"https://t.me/{me.username}")]]
                ),
                parse_mode="HTML",
            )
        except Exception:
            pass

        t = game.get("loop_task")
        if t:
            try:
                t.cancel()
            except Exception:
                pass
        game["loop_task"] = asyncio.create_task(_game_loop(chat_id))
        await apply_silence_rules(chat_id)


    async def _admin_extend_reg(chat_id: int, admin_id: int) -> None:
        if not await is_admin(chat_id, admin_id):
            await try_dm(admin_id, "❌ Faqat admin")
            return
        game = get_game(chat_id)
        if game.get("phase") != PHASE_REG:
            await try_dm(admin_id, "❌ Faqat ro'yxat vaqtini uzaytirish mumkin")
            return
        g = get_group(chat_id)
        add_sec = int(g.get("registration_seconds", 60))
        now = asyncio.get_event_loop().time()
        old_deadline = float(game.get("reg_deadline", 0.0))
        base = max(old_deadline, now)
        game["reg_deadline"] = base + add_sec
        await try_dm(admin_id, "✅ Ro'yxat vaqti uzaytirildi")
        try:
            left = max(0, int(float(game.get("reg_deadline", 0)) - now))
            mm = left // 60
            ss = left % 60
            left_txt = f"{mm:02d}:{ss:02d}"
            variants = [
                f"⏳ Bonus vaqt! +{add_sec}s\nQolgan vaqt: {left_txt}",
                f"📝 Ro'yxat cho'zildi! +{add_sec}s\nQolgan vaqt: {left_txt}",
                f"⚡️ Tezroq! Ro'yxatga +{add_sec}s qo'shildi\nQolgan vaqt: {left_txt}",
                f"⏳ Oxirgi imkon! +{add_sec}s\nQolgan vaqt: {left_txt}",
            ]
            await _cleanup_reg_announces(chat_id)
            sent = await bot.send_message(chat_id, random.choice(variants), parse_mode="HTML")
            game["reg_announce_ids"] = [int(sent.message_id)]
        except Exception:
            pass
        try:
            await _edit_group_status(
                chat_id,
                reply_markup=await reg_join_kb(chat_id),
            )
        except Exception:
            pass

        t = game.get("reg_task")
        if t:
            try:
                t.cancel()
            except Exception:
                pass
        game["reg_task"] = asyncio.create_task(_registration_timeout(chat_id, float(game.get("reg_deadline", 0))))


    async def _admin_stop_game(chat_id: int, admin_id: int) -> None:
        if not await is_admin(chat_id, admin_id):
            await try_dm(admin_id, "❌ Faqat admin")
            return
        game = get_game(chat_id)
        t = game.get("loop_task")
        if t:
            try:
                t.cancel()
            except Exception:
                pass
        game["loop_task"] = None
        game["started"] = False
        game["phase"] = PHASE_IDLE
        try:
            await _edit_group_status(chat_id, reply_markup=None)
        except Exception:
            pass
        await clear_silence(chat_id)
        await try_dm(admin_id, "✅ O'yin to'xtatildi")

    if msg.chat.type in {"group", "supergroup"}:
        try:
            game = get_game(msg.chat.id)
            now_ts = asyncio.get_event_loop().time()
            last_ts = float(game.get("_last_game_cmd_ts", 0.0) or 0.0)
            if now_ts - last_ts < 1.5:
                return
            game["_last_game_cmd_ts"] = now_ts

            # /game in group => open registration in group (admin only)
            g = get_group(msg.chat.id)
            if not await is_admin(msg.chat.id, msg.from_user.id) and not bool(g.get("other_anyone_can_reg", False)):
                ok = await try_dm(msg.from_user.id, "❌ Faqat admin")
                if not ok:
                    try:
                        await msg.answer("⬇️", reply_markup=await bot_link_kb())
                    except Exception:
                        pass
                return

            if not await bot_has_required_rights(msg.chat.id):
                await msg.answer(bot_rights_text())
                return

            if game.get("phase") == PHASE_REG:
                kb = await reg_join_kb(msg.chat.id)
                try:
                    await _cleanup_reg_announces(msg.chat.id)
                    old_mid = game.get("reg_message_id")
                    if old_mid:
                        try:
                            await _safe_delete_message(msg.chat.id, int(old_mid))
                        except Exception:
                            pass
                    reg_msg = await msg.answer(_group_status_text(msg.chat.id), reply_markup=kb, parse_mode="HTML")
                    game["reg_message_id"] = reg_msg.message_id

                    if bool(get_group(msg.chat.id).get("pin_registration", True)):
                        await _safe_pin_message(msg.chat.id, reg_msg.message_id)
                except Exception:
                    pass

                t = game.get("reg_task")
                if t:
                    try:
                        t.cancel()
                    except Exception:
                        pass
                game["reg_task"] = asyncio.create_task(_registration_timeout(msg.chat.id, float(game.get("reg_deadline", 0))))
                return

            g = get_group(msg.chat.id)
            game["started"] = False
            game["phase"] = PHASE_REG
            game["players"].clear()
            game["roles"].clear()
            game["alive"].clear()
            game["teams"] = None
            game["reg_deadline"] = asyncio.get_event_loop().time() + int(g.get("registration_seconds", 60))

            kb = await reg_join_kb(msg.chat.id)
            try:
                old_mid = game.get("reg_message_id")
                if old_mid:
                    await _safe_delete_message(msg.chat.id, int(old_mid))
            except Exception:
                pass
            reg_msg = await msg.answer(_group_status_text(msg.chat.id), reply_markup=kb, parse_mode="HTML")
            game["reg_message_id"] = reg_msg.message_id

            if bool(get_group(msg.chat.id).get("pin_registration", True)):
                await _safe_pin_message(msg.chat.id, reg_msg.message_id)

            try:
                _runtime_set(
                    msg.chat.id,
                    {
                        "phase": "reg",
                        "reg_message_id": game.get("reg_message_id"),
                        "reg_deadline": float(game.get("reg_deadline", 0.0) or 0.0),
                    },
                )
            except Exception:
                pass

            t = game.get("reg_task")
            if t:
                try:
                    t.cancel()
                except Exception:
                    pass
            game["reg_task"] = asyncio.create_task(_registration_timeout(msg.chat.id, float(game.get("reg_deadline", 0))))

            ok = await try_dm(msg.from_user.id, f"✅ Guruh {msg.chat.id} da ro'yxat boshlandi!")
            if not ok:
                try:
                    await msg.answer("⬇️", reply_markup=await bot_link_kb())
                except Exception:
                    pass
            return
        except Exception as e:
            try:
                print(f"[group /game error] {type(e).__name__}: {e}")
            except Exception:
                pass
            return
        finally:
            pass

    # private /game => show active games
    active = [cid for cid, g in GAMES.items() if str(msg.from_user.id) in g.get("players", {})]
    if not active:
        await msg.answer(
            "🎮 Siz hech qanday o'yinda qatnashmaysiz.\n\nGuruhda /game buyrug'ini bosing va ro'yxatdan o'ting!"
        )
        return
    await msg.answer("🎮 Faol o'yinlar:\n\n" + "\n".join(f"- {cid}" for cid in active))


@dp.message(F.chat.type.in_({"group", "supergroup"}), F.text.regexp(r"^/game(@\w+)?(\s|$)"))
async def alias_game_group(msg: Message) -> None:
    return


@dp.callback_query(F.data.startswith("join:"))
async def cb_join(cb: CallbackQuery) -> None:
    chat_id = int(cb.data.split(":", 1)[1])
    game = get_game(chat_id)
    if game.get("phase") != PHASE_REG:
        return await cb.answer("❌ Ro'yxat yopiq", show_alert=True)

    now = asyncio.get_event_loop().time()
    if now > float(game.get("reg_deadline", 0)):
        return await cb.answer("⏳ Ro'yxat tugadi", show_alert=True)

    # Legacy callback is kept only for old messages; joining is done via deep-link /start join_<chat_id>
    try:
        await cb.answer("Botga o'ting va \"Qo'shilish\" tugmasini bosing", show_alert=True)
    except Exception:
        return await cb.answer()


@dp.message(Command("start"), F.chat.type.in_({"group", "supergroup"}))
async def group_start(msg: Message) -> None:
    try:
        g = get_group(msg.chat.id)
        if not await is_admin(msg.chat.id, msg.from_user.id) and not bool(g.get("other_anyone_can_start", False)):
            ok = await try_dm(msg.from_user.id, "❌ Faqat admin")
            if not ok:
                try:
                    await msg.answer("⬇️", reply_markup=await bot_link_kb())
                except Exception:
                    pass
            return

        if not await bot_has_required_rights(msg.chat.id):
            await msg.answer(bot_rights_text())
            return

        game = get_game(msg.chat.id)
        if game.get("phase") != PHASE_REG or game.get("started"):
            ok = await try_dm(msg.from_user.id, "❌ Avval /game bilan ro'yxat oching")
            if not ok:
                try:
                    await msg.answer("⬇️", reply_markup=await bot_link_kb())
                except Exception:
                    pass
            return
        now = asyncio.get_event_loop().time()
        if now > float(game.get("reg_deadline", 0)):
            try:
                await msg.answer("⏳ Ro'yxatdan o'tish vaqti tugadi")
            except Exception:
                pass
            return
        if len(game.get("players", {})) < 4:
            try:
                mid = game.get("reg_message_id")
                if mid:
                    try:
                        await _safe_delete_message(msg.chat.id, int(mid))
                    except Exception:
                        pass

                try:
                    await _cleanup_reg_announces(msg.chat.id)
                except Exception:
                    pass

                t = game.get("reg_task")
                if t:
                    try:
                        t.cancel()
                    except Exception:
                        pass
                game["reg_task"] = None

                game.update(
                    {
                        "phase": PHASE_IDLE,
                        "started": False,
                        "players": {},
                        "roles": {},
                        "alive": [],
                        "teams": None,
                        "reg_message_id": None,
                        "reg_deadline": 0.0,
                        "blocked": set(),
                        "votes": {},
                        "afsungar_day_revenge": None,
                    }
                )
                _runtime_clear(msg.chat.id)
                await clear_silence(msg.chat.id)

                await msg.answer("O'yinni boshlash uchun o'yinchilar yetarli emas....")
            except Exception:
                pass
            ok = await try_dm(msg.from_user.id, "❌ Kamida 4 o'yinchi kerak")
            if not ok:
                try:
                    await msg.answer("⬇️", reply_markup=await bot_link_kb())
                except Exception:
                    pass
            return

        # Ensure DM is available for all players; drop those who cannot be reached.
        dropped: List[str] = []
        for uid, nm in list(game.get("players", {}).items()):
            ok = False
            try:
                ok = await safe_dm(int(uid), "✅")
            except Exception:
                ok = False
            if not ok:
                game["players"].pop(str(uid), None)
                game.get("roles", {}).pop(str(uid), None)
                if str(uid) in game.get("alive", []):
                    try:
                        game["alive"].remove(str(uid))
                    except Exception:
                        pass
                dropped.append(str(nm or "User"))

        if dropped:
            try:
                await bot.send_message(
                    msg.chat.id,
                    "⚠️ DM yopiq bo'lgani uchun ro'yxatdan chiqarildi:\n" + "\n".join(f"- {html.escape(n)}" for n in dropped),
                    parse_mode="HTML",
                )
            except Exception:
                pass

        if len(game.get("players", {})) < 4:
            ok = await try_dm(msg.from_user.id, "❌ Kamida 4 o'yinchi kerak")
            if not ok:
                try:
                    await msg.answer("⬇️", reply_markup=await bot_link_kb())
                except Exception:
                    pass
            return

        players = list(game["players"].keys())
        roles = assign_roles(len(players), msg.chat.id)
        for uid, role in zip(players, roles):
            game["roles"][uid] = role

        game["started"] = True
        game["phase"] = PHASE_NIGHT
        game["round"] = 1
        game["started_ts"] = asyncio.get_event_loop().time()
        game["alive"] = list(game["players"].keys())
        game["participants"] = list(game.get("players", {}).keys())

        # send roles
        for uid, role in game["roles"].items():
            desc = ROLE_DESC_UZ.get(role, "")
            mates = _teammates_block_html(game, uid)
            role_text = f"🎭 Sizning roliz: {ROLE_EMOJI.get(role,'🎭')} {role}" + (f"\n\n{desc}" if desc else "")
            if mates:
                role_text += f"\n\n{mates}"
            if group_url(msg.chat.id):
                await safe_dm(
                    int(uid),
                    role_text,
                    reply_markup=group_open_kb(msg.chat.id),
                    parse_mode="HTML",
                )
            else:
                await safe_dm(int(uid), role_text, parse_mode="HTML")

        # edit group message
        try:
            await bot.edit_message_text(
                chat_id=msg.chat.id,
                message_id=game.get("reg_message_id"),
                text="O'yin boshlandi!",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[[InlineKeyboardButton(text="Sizning rolingiz", callback_data="menu:profile")]]
                ),
            )
        except Exception:
            pass

        # group alive list + role counts (Baku style)
        alive = _alive_ids(game)
        alive_lines = "\n".join(f"{i+1}. {_player_link(game, uid)}" for i, uid in enumerate(alive))
        try:
            await bot.send_message(
                msg.chat.id,
                (
                    "Tirik o'yinchilar:\n"
                    f"{alive_lines}\n\n"
                    f"Ulardan: {_role_counts_line(game)}\n"
                    f"Jami: {len(alive)}"
                ),
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[[InlineKeyboardButton(text="Bot-ga o'tish", url=f"https://t.me/{(await bot.get_me()).username}")]]
                ),
                parse_mode="HTML",
            )
        except Exception:
            pass

        ok = await try_dm(msg.from_user.id, "✅ O'yin boshlandi")
        if not ok:
            try:
                await msg.answer("⬇️", reply_markup=await bot_link_kb())
            except Exception:
                pass

        t = game.get("loop_task")
        if t:
            try:
                t.cancel()
            except Exception:
                pass
        game["loop_task"] = asyncio.create_task(_game_loop(msg.chat.id))

        await apply_silence_rules(msg.chat.id)
    finally:
        pass


@dp.message(F.chat.type.in_({"group", "supergroup"}), F.text.regexp(r"^/start(@\w+)?(\s|$)"))
async def alias_start_group(msg: Message) -> None:
    return


@dp.callback_query(F.data.startswith("act:"))
async def cb_act(cb: CallbackQuery) -> None:
    parts = cb.data.split(":")
    if len(parts) < 5:
        return await cb.answer()

    chat_id = int(parts[1])
    action = parts[2]
    actor_uid = str(parts[3])
    target_uid = str(parts[4])
    game = get_game(chat_id)

    if str(cb.from_user.id) != actor_uid:
        return await cb.answer("❌", show_alert=True)
    if not game.get("started") or game.get("phase") != PHASE_NIGHT:
        return await cb.answer("❌ Hozir tun emas", show_alert=True)
    if actor_uid not in _alive_ids(game):
        return await cb.answer("❌", show_alert=True)
    if action == "cancel":
        return await cb.answer("✅", show_alert=True)

    if target_uid != "0" and target_uid not in _alive_ids(game):
        return await cb.answer("❌ Target tirik emas", show_alert=True)

    role = _role_of(game, actor_uid)

    try:
        game.setdefault("night_acted", set()).add(actor_uid)
    except Exception:
        pass

    def _announce_once(text: str) -> None:
        try:
            if not bool(get_group(chat_id).get("other_action_announces", True)):
                return
            key = (action, actor_uid)
            announced = game.setdefault("night_announced", set())
            if key in announced:
                return
            announced.add(key)
            asyncio.create_task(bot.send_message(chat_id, text))
        except Exception:
            return

    if action == "mafia_kill" and role in {"Don", "Mafia"}:
        game["night"]["mafia_kill"] = target_uid
        _announce_once("🤵 Mafiya navbatdagi o'ljasini tanladi...")
        await cb.answer("✅ Tanlandi", show_alert=True)
        return

    if action == "qotil_kill" and role == "Qotil":
        game["night"]["qotil_kill"] = target_uid
        _announce_once("🔪 Qotil nishonni tanladi...")
        await cb.answer("✅ Tanlandi", show_alert=True)
        return

    if action == "udda_kill" and role == "убийца":
        game["night"]["udda_kill"] = target_uid
        _announce_once("🔪 убийца nishonni tanladi...")
        await cb.answer("✅ Tanlandi", show_alert=True)
        return

    if action == "doctor_save" and role == "Doktor":
        allow_self = actor_uid not in game["night"].get("doctor_self_used", set())
        if target_uid == actor_uid and not allow_self:
            return await cb.answer("❌ O'zingizni faqat 1 marta", show_alert=True)
        game["night"]["doctor_save"] = target_uid
        _announce_once("👨🏻‍⚕ Doktor bemorni tanladi...")
        await cb.answer("✅ Tanlandi", show_alert=True)
        return

    if action == "kom_check" and role == "Komissar":
        game["night"]["kom_check"] = target_uid
        _announce_once("🕵🏻‍♂ Komissar tekshiruv boshladi...")
        await cb.answer("✅ Tanlandi", show_alert=True)
        return

    if action == "kom_shoot" and role == "Komissar":
        if target_uid == actor_uid:
            return await cb.answer("❌", show_alert=True)
        game["night"]["kom_shoot"] = target_uid
        _announce_once("🕵🏻‍♂ Komissar katani katani pistolettini o'qladi...")
        await cb.answer("✅ Tanlandi", show_alert=True)
        return

    if action == "adv_protect" and role == "Advokat":
        game["night"]["adv_protect"] = target_uid
        _announce_once("👨‍💼 Advokat himoyani tanladi...")
        await cb.answer("✅ Tanlandi", show_alert=True)
        return

    if action == "kezuvchi_block" and role == "Kezuvchi":
        game["night"]["kezuvchi_block"] = target_uid
        _announce_once("💃 Kezuvchi kimnidir uxlatdi...")
        await cb.answer("✅ Tanlandi", show_alert=True)
        return

    if action == "daydi_visit" and role == "Daydi":
        game["night"]["daydi_visit"] = target_uid
        _announce_once("🧙‍♂ Daydi ko'chaga chiqdi...")
        await cb.answer("✅ Tanlandi", show_alert=True)
        return

    if action == "journalist_check" and role == "Jurnalist":
        game["night"]["journalist_check"] = target_uid
        _announce_once("📰 Jurnalist tekshiruv boshladi...")
        await cb.answer("✅ Tanlandi", show_alert=True)
        return

    if action == "gazabkor_mark" and role == "G'azabkor":
        game["night"].setdefault("gazabkor_mark", {})[actor_uid] = target_uid
        _announce_once("🧟 G'azabkor nishonni tanladi...")
        await cb.answer("✅ Tanlandi", show_alert=True)
        return

    if action == "afsun_revenge":
        # Afsungar day revenge (only after lynch)
        if game.get("afsungar_day_revenge") != actor_uid:
            return await cb.answer("❌", show_alert=True)
        if target_uid == actor_uid:
            return await cb.answer("❌", show_alert=True)
        if target_uid in game.get("alive", []):
            game["alive"].remove(target_uid)
        game["afsungar_day_revenge"] = None
        await cb.answer("✅", show_alert=True)
        try:
            await bot.send_message(
                chat_id,
                f"🧞‍♂️ Afsungar bilan birga ketdi: {_player_link(game, target_uid)}",
                parse_mode="HTML",
            )
        except Exception:
            pass

        try:
            await _maybe_promote_don(chat_id, game)
        except Exception:
            pass
        return

    await cb.answer("❌", show_alert=True)


@dp.callback_query(F.data.startswith("vote:"))
async def cb_vote(cb: CallbackQuery) -> None:
    parts = cb.data.split(":")
    if len(parts) < 4:
        return await cb.answer()
    chat_id = int(parts[1])
    voter_uid = str(parts[2])
    target_uid = str(parts[3])
    game = get_game(chat_id)
    if str(cb.from_user.id) != voter_uid:
        return await cb.answer("❌", show_alert=True)
    if not game.get("started") or game.get("phase") != PHASE_LYNCH:
        return await cb.answer("❌", show_alert=True)
    if voter_uid not in _alive_ids(game):
        return await cb.answer("❌", show_alert=True)

    if voter_uid in set(game.get("sleep_day", set()) or set()):
        return await cb.answer("😴", show_alert=True)
    if target_uid != "0" and target_uid not in _alive_ids(game):
        return await cb.answer("❌", show_alert=True)
    if target_uid == voter_uid:
        return await cb.answer("❌", show_alert=True)

    if target_uid == "0":
        game["votes"].pop(voter_uid, None)
        await cb.answer("✅", show_alert=True)
        try:
            await bot.send_message(
                chat_id,
                f"{_player_link(game, voter_uid)} — 🚫 O'tkazib yubordi",
                parse_mode="HTML",
            )
        except Exception:
            pass
        return

    # Aferist can cast vote on behalf of another player once per day
    if _role_of(game, voter_uid) == "Aferist":
        used = set(game.setdefault("aferist_used", set()) or set())
        if voter_uid in used:
            return await cb.answer("❌", show_alert=True)
        candidates = [u for u in _alive_ids(game) if u != voter_uid and u not in game.get("votes", {})]
        if not candidates:
            return await cb.answer("❌", show_alert=True)
        masked = random.choice(candidates)
        game["votes"][masked] = target_uid
        used.add(voter_uid)
        game["aferist_used"] = used
        await cb.answer("✅ Ovoz berildi", show_alert=True)
        try:
            await bot.send_message(
                chat_id,
                f"{_player_link(game, masked)} — {_player_link(game, target_uid)} ga ovoz berdi",
                parse_mode="HTML",
            )
        except Exception:
            pass
        return

    game["votes"][voter_uid] = target_uid
    await cb.answer("✅ Ovoz berildi", show_alert=True)

    try:
        await bot.send_message(
            chat_id,
            f"{_player_link(game, voter_uid)} — {_player_link(game, target_uid)} ga ovoz berdi",
            parse_mode="HTML",
        )
    except Exception:
        pass


@dp.callback_query(F.data.startswith("sehrgar:"))
async def cb_sehrgar(cb: CallbackQuery) -> None:
    parts = (cb.data or "").split(":")
    if len(parts) != 4:
        return await cb.answer()
    try:
        chat_id = int(parts[1])
    except Exception:
        return await cb.answer()
    sg_uid = str(parts[2])
    choice = str(parts[3])
    if choice not in {"forgive", "kill"}:
        return await cb.answer("❌", show_alert=True)
    if str(cb.from_user.id) != sg_uid:
        return await cb.answer("❌", show_alert=True)
    game = get_game(chat_id)
    if not game.get("started"):
        return await cb.answer("❌", show_alert=True)
    game.setdefault("sehrgar_decision", {})[sg_uid] = choice
    await cb.answer("✅", show_alert=True)
    try:
        await cb.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass


@dp.callback_query(F.data.startswith("openvote:"))
async def cb_openvote(cb: CallbackQuery) -> None:
    chat_id = int(cb.data.split(":", 1)[1])
    game = get_game(chat_id)
    if not game.get("started") or game.get("phase") != PHASE_LYNCH:
        return await cb.answer("❌", show_alert=True)

    uid = str(cb.from_user.id)
    if uid not in _alive_ids(game):
        return await cb.answer("❌", show_alert=True)

    if uid in set(game.get("sleep_day", set()) or set()):
        return await cb.answer("😴", show_alert=True)

    ok = await safe_dm(
        int(uid),
        "Aybdorlarni topish va jazolash vaqti keldi!\nKimni osish kerak deb xisoblaysiz?",
        reply_markup=with_group_button(chat_id, _vote_kb(chat_id, uid)),
    )
    if ok:
        return await cb.answer("✅", show_alert=False)
    try:
        await bot.send_message(
            chat_id,
            "⬇️",
            reply_markup=await bot_link_kb(),
        )
    except Exception:
        pass
    return await cb.answer("", show_alert=False)


@dp.callback_query(F.data.startswith("confirm:"))
async def cb_confirm(cb: CallbackQuery) -> None:
    parts = cb.data.split(":")
    if len(parts) != 3:
        return await cb.answer()
    chat_id = int(parts[1])
    vote = parts[2]
    if vote not in {"up", "down"}:
        return await cb.answer()

    game = get_game(chat_id)
    if not game.get("started") or game.get("phase") != PHASE_CONFIRM:
        return await cb.answer("❌", show_alert=True)

    uid = str(cb.from_user.id)
    if uid not in _alive_ids(game):
        return await cb.answer("❌", show_alert=True)

    if uid in set(game.get("sleep_day", set()) or set()):
        return await cb.answer("😴", show_alert=True)

    game.setdefault("confirm", {}).setdefault("votes", {})[uid] = vote
    cvotes: Dict[str, str] = game.get("confirm", {}).get("votes", {})

    up = 0
    down = 0
    for voter, v in cvotes.items():
        weight = 2 if _role_of(game, str(voter)) == "Janob" else 1
        if v == "up":
            up += int(weight)
        elif v == "down":
            down += int(weight)

    await cb.answer("✅")
    try:
        await cb.message.edit_reply_markup(
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(text=f"👍 {up}", callback_data=f"confirm:{chat_id}:up"),
                        InlineKeyboardButton(text=f"👎 {down}", callback_data=f"confirm:{chat_id}:down"),
                    ]
                ]
            )
        )
    except Exception:
        pass


@dp.message(Command("stop"), F.chat.type.in_({"group", "supergroup"}))
async def group_stop(msg: Message) -> None:
    if not await is_admin(msg.chat.id, msg.from_user.id):
        ok = await try_dm(msg.from_user.id, "❌ Faqat admin")
        if not ok:
            try:
                await msg.answer("⬇️", reply_markup=await bot_link_kb())
            except Exception:
                pass
        return

    game = get_game(msg.chat.id)

    rt = game.get("reg_task")
    if rt:
        try:
            rt.cancel()
        except Exception:
            pass
    game["reg_task"] = None

    try:
        mid = game.get("reg_message_id")
        if mid:
            await _safe_delete_message(msg.chat.id, int(mid))
    except Exception:
        pass

    try:
        await _cleanup_reg_announces(msg.chat.id)
    except Exception:
        pass

    t = game.get("loop_task")
    if t:
        try:
            t.cancel()
        except Exception:
            pass
        game["loop_task"] = None
    game.update({
        "phase": PHASE_IDLE,
        "started": False,
        "players": {},
        "roles": {},
        "alive": [],
        "teams": None,
        "reg_message_id": None,
        "reg_deadline": 0.0,
        "blocked": set(),
        "votes": {},
        "afsungar_day_revenge": None,
    })
    await clear_silence(msg.chat.id)

    try:
        await msg.answer("O'yin to'xtatildi")
    except Exception:
        pass

    return


@dp.message(F.chat.type.in_({"group", "supergroup"}), F.text.regexp(r"^/stop(@\w+)?(\s|$)"))
async def alias_stop_group(msg: Message) -> None:
    return await group_stop(msg)


@dp.message(Command("extend"), F.chat.type.in_({"group", "supergroup"}))
async def group_extend(msg: Message) -> None:
    if not await is_admin(msg.chat.id, msg.from_user.id):
        ok = await try_dm(msg.from_user.id, "❌ Faqat admin")
        if not ok:
            try:
                await msg.answer("⬇️", reply_markup=await bot_link_kb())
            except Exception:
                pass
        return

    game = get_game(msg.chat.id)
    if game.get("phase") != PHASE_REG:
        ok = await try_dm(msg.from_user.id, "❌ Faqat ro'yxat vaqtini uzaytirish mumkin")
        if not ok:
            try:
                await msg.answer("⬇️", reply_markup=await bot_link_kb())
            except Exception:
                pass
        return
    g = get_group(msg.chat.id)
    add_sec = int(g.get("registration_seconds", 60))
    try:
        parts = (msg.text or "").split(maxsplit=1)
        arg = parts[1] if len(parts) > 1 else ""
        parsed = _parse_extend_seconds(arg)
        if parsed is not None:
            add_sec = int(parsed)
    except Exception:
        pass
    now = asyncio.get_event_loop().time()
    old_deadline = float(game.get("reg_deadline", 0.0))
    base = max(old_deadline, now)
    game["reg_deadline"] = base + add_sec
    ok = await try_dm(msg.from_user.id, "✅ Ro'yxat vaqti uzaytirildi")
    if not ok:
        try:
            await msg.answer("⬇️", reply_markup=await bot_link_kb())
        except Exception:
            pass

    try:
        left = max(0, int(float(game.get("reg_deadline", 0)) - now))
        mm = left // 60
        ss = left % 60
        left_txt = f"{mm:02d}:{ss:02d}"
        variants = [
            f"⏳ Bonus vaqt! +{add_sec}s\nQolgan vaqt: {left_txt}",
            f"📝 Ro'yxat cho'zildi! +{add_sec}s\nQolgan vaqt: {left_txt}",
            f"⚡️ Tezroq! Ro'yxatga +{add_sec}s qo'shildi\nQolgan vaqt: {left_txt}",
            f"⏳ Oxirgi imkon! +{add_sec}s\nQolgan vaqt: {left_txt}",
        ]
        await _cleanup_reg_announces(msg.chat.id)
        sent = await msg.answer(random.choice(variants))
        game["reg_announce_ids"] = [int(sent.message_id)]
    except Exception:
        pass

    try:
        await _edit_group_status(
            msg.chat.id,
            reply_markup=await reg_join_kb(msg.chat.id),
        )
    except Exception:
        pass

    t = game.get("reg_task")
    if t:
        try:
            t.cancel()
        except Exception:
            pass
    game["reg_task"] = asyncio.create_task(_registration_timeout(msg.chat.id, float(game.get("reg_deadline", 0))))

    return


@dp.message(F.chat.type.in_({"group", "supergroup"}), F.text.regexp(r"^/extend(@\w+)?(\s|$)"))
async def alias_extend_group(msg: Message) -> None:
    return await group_extend(msg)


@dp.message(Command("leave"), F.chat.type.in_({"group", "supergroup"}))
async def group_leave(msg: Message) -> None:
    try:
        game = get_game(msg.chat.id)
        uid = str(msg.from_user.id)
        if uid not in game.get("players", {}):
            return
        game["players"].pop(uid, None)
        game.get("roles", {}).pop(uid, None)
        if uid in game.get("alive", []):
            game["alive"].remove(uid)

        if game.get("phase") == PHASE_REG:
            await _edit_group_status(
                msg.chat.id,
                reply_markup=await reg_join_kb(msg.chat.id),
            )

        if game.get("started"):
            outcome = _win_state(game)
            if outcome:
                t = game.get("loop_task")
                if t:
                    try:
                        t.cancel()
                    except Exception:
                        pass
                    game["loop_task"] = None
                await _finish_game(msg.chat.id, str(outcome))

        ok = await try_dm(msg.from_user.id, "✅ Siz o'yindan chiqdingiz")
        if not ok:
            try:
                await msg.answer("⬇️", reply_markup=await bot_link_kb())
            except Exception:
                pass
    finally:
        pass


@dp.message(F.chat.type.in_({"group", "supergroup"}), F.text.regexp(r"^/leave(@\w+)?(\s|$)"))
async def alias_leave_group(msg: Message) -> None:
    return await group_leave(msg)


@dp.message(Command("settings"), F.chat.type.in_({"group", "supergroup"}))
async def group_settings(msg: Message) -> None:
    if not await is_admin(msg.chat.id, msg.from_user.id):
        ok = await try_dm(msg.from_user.id, "❌ Faqat admin")
        if not ok:
            try:
                await msg.answer("⬇️", reply_markup=await bot_link_kb())
            except Exception:
                pass
        return

    ok = await try_dm(
        msg.from_user.id,
        "Qanday parametrlarni o'zgartirmoqchisiz?",
        reply_markup=admin_settings_main_kb(msg.chat.id),
    )
    if not ok:
        try:
            await msg.answer("⬇️", reply_markup=await bot_link_kb())
        except Exception:
            pass

    return


@dp.message(F.chat.type.in_({"group", "supergroup"}), F.text.regexp(r"^/settings(@\w+)?(\s|$)"))
async def alias_settings_group(msg: Message) -> None:
    return await group_settings(msg)
@dp.callback_query(F.data.startswith("adminset:"))
async def cb_admin_settings(cb: CallbackQuery) -> None:
    parts = cb.data.split(":")
    if len(parts) < 3:
        return await cb.answer()
    section = parts[1]
    chat_id = int(parts[2])
    if not await is_admin(chat_id, cb.from_user.id):
        return await cb.answer("❌ Faqat admin", show_alert=True)

    if section == "times":
        await cb.message.edit_text("Qaysi vaqtni o'zgartirish kerak", reply_markup=time_main_kb(chat_id))
        return await cb.answer()

    if section == "giveaway":
        g = get_group(chat_id)
        await cb.message.edit_text(
            (
                "Giveaway sozlamalari:\n\n"
                f"Minimal o'yin (7 kun): {int(g.get('give_min_games_7d', 0))}\n\n"
                f"🎮 O'yinlar: {int(g.get('give_games', 0))}\n"
                f"💎 Olmos: {int(g.get('give_diamonds', 0))}\n"
                f"🎭 Maska: {int(g.get('give_mask', 0))}\n"
                f"🔫 Miltiq: {int(g.get('give_gun', 0))}\n"
                f"📄 Soxta hujjat: {int(g.get('give_docs', 0))}\n"
                f"🛡 Himoya: {int(g.get('give_protect', 0))}\n\n"
                f"⛑️ Qotildan himoya: {int(g.get('give_anti_killer', 0))}\n"
                f"⚖️ Ovoz himoyasi: {int(g.get('give_vote_protect', 0))}\n\n"
                "Qiymatni oshirish uchun + tugmasini bosing."
            ),
            reply_markup=giveaway_kb(chat_id),
        )
        return await cb.answer()

    if section == "weapons":
        await cb.message.edit_text("Qurollar", reply_markup=weapons_main_kb(chat_id))
        return await cb.answer()

    if section == "other":
        await cb.message.edit_text("Boshqa sozlamalar", reply_markup=other_main_kb(chat_id))
        return await cb.answer()

    if section == "lang":
        await cb.message.edit_text("Guruh tili:", reply_markup=group_lang_kb(chat_id))
        return await cb.answer()

    if section == "panel":
        await cb.message.edit_text(
            "Boshqaruv paneli:",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=group_admin_kb().inline_keyboard
                + [[InlineKeyboardButton(text="⬅️ Orqaga", callback_data=f"adminset:back:{chat_id}")]],
            ),
        )
        return await cb.answer()

    if section == "silence":
        await cb.message.edit_text(
            "Chatda xabar yozish qobiliyatini o'chirib qo'ying",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="O'lganlar", callback_data=f"silence:open:{chat_id}:dead")],
                    [InlineKeyboardButton(text="Uxlayotganlar", callback_data=f"silence:open:{chat_id}:sleep")],
                    [InlineKeyboardButton(text="O'ynamayotganlar", callback_data=f"silence:open:{chat_id}:inactive")],
                    [InlineKeyboardButton(text="⬅️ Orqaga", callback_data=f"adminset:back:{chat_id}")],
                ]
            ),
        )
        return await cb.answer()

    if section == "roles":
        await cb.message.edit_text(roles_overview_text(), reply_markup=admin_roles_kb(chat_id))
        return await cb.answer()

    if section == "back":
        await cb.message.edit_text("Qanday parametrlarni o'zgartirmoqchisiz?", reply_markup=admin_settings_main_kb(chat_id))
        return await cb.answer()

    await cb.answer()


@dp.callback_query(F.data == "news")
async def cb_news(cb: CallbackQuery) -> None:
    await cb.answer()
    try:
        await cb.message.edit_text(
            (
                "📰 Mafia Bot Yangiliklar\n\n"
                "🔮 Tez kunda:\n"
                "• 🏆 Turnirlar\n"
                "• 🎁 Sovg'alar\n"
                "• 🌟 VIP imkoniyatlar\n\n"
                "📞 Bog'lanish: Admin"
            ),
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="📞 Admin", url=f"tg://user?id={int(ADMIN_CONTACT_ID)}")],
                    [InlineKeyboardButton(text="⬅️ Orqaga", callback_data="menu:profile")],
                ]
            ),
        )
    except Exception:
        return


@dp.callback_query(F.data == "premium:groups")
async def cb_premium_groups(cb: CallbackQuery) -> None:
    await cb.answer()
    try:
        await cb.message.edit_text(
            (
                "Premium guruhlar:\n\n"
                "Hozircha premium tizimi yoqilmagan.\n"
                "Bu bo'lim faqat UI uchun qo'yilgan."
            ),
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="⬅️ Orqaga", callback_data="menu:profile")]]
            ),
        )
    except Exception:
        return


@dp.callback_query(F.data.startswith("shop:"))
async def cb_shop_shortcuts(cb: CallbackQuery) -> None:
    section = cb.data.split(":", 1)[1]
    await cb.answer()
    try:
        if section == "money":
            await cb.message.edit_text("Dollar olish 💵", reply_markup=buy_money_to_diamond_kb())
            return
        if section == "diamond":
            await cb.message.edit_text("Xarid qiling 💎", reply_markup=buy_diamond_prices_kb())
            return
        await cb.message.edit_text("🛒 Do'kon:", reply_markup=shop_kb())
    except Exception:
        return


@dp.callback_query(F.data.startswith("time:open:"))
async def cb_time_open(cb: CallbackQuery) -> None:
    parts = cb.data.split(":")
    if len(parts) != 4:
        return await cb.answer()
    chat_id = int(parts[2])
    key = parts[3]
    if not await is_admin(chat_id, cb.from_user.id):
        return await cb.answer("❌ Faqat admin", show_alert=True)
    title = TIME_LABELS.get(key, key)
    await cb.message.edit_text(f"{title} vaqtini tanlang (sek.)", reply_markup=time_preset_kb(chat_id, key))
    return await cb.answer()


@dp.callback_query(F.data.startswith("time:set:"))
async def cb_time_set(cb: CallbackQuery) -> None:
    parts = cb.data.split(":")
    if len(parts) != 5:
        return await cb.answer()
    chat_id = int(parts[2])
    key = parts[3]
    val = int(parts[4])
    if not await is_admin(chat_id, cb.from_user.id):
        return await cb.answer("❌ Faqat admin", show_alert=True)
    g = get_group(chat_id)
    g[key] = val
    update_group(chat_id, g)
    title = TIME_LABELS.get(key, key)
    await cb.message.edit_text(f"{title} vaqtini tanlang (sek.)", reply_markup=time_preset_kb(chat_id, key))
    return await cb.answer("✅")


@dp.callback_query(F.data.startswith("givecfg:open:"))
async def cb_givecfg_open(cb: CallbackQuery) -> None:
    parts = cb.data.split(":")
    if len(parts) != 4:
        return await cb.answer()
    chat_id = int(parts[2])
    key = parts[3]
    if not await is_admin(chat_id, cb.from_user.id):
        return await cb.answer("❌ Faqat admin", show_alert=True)
    if key == "min7":
        try:
            await cb.message.edit_text(
                "Kakoe minimal'noe kolichestvo igr za poslednie 7 dney\n"
                "trebuyetsya ot igroka dlya uchastiya v rozygryshe?",
                reply_markup=giveaway_min_games_kb(chat_id),
            )
        except Exception:
            pass
        return await cb.answer()

    titles = {
        "give_games": "🎲 Kolichestvo igr",
        "give_diamonds": "💎 Olmos",
        "give_mask": "🎭 Maska",
        "give_gun": "🔫 Miltiq",
        "give_docs": "📁 Hujjatlar",
        "give_protect": "🛡 Himoya",
        "give_anti_killer": "⛑️ Qotildan himoya",
        "give_vote_protect": "⚖️ Ovoz himoyasi",
    }

    presets: Dict[str, List[int]] = {
        "give_games": [0, 1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50],
        "give_diamonds": [0, 1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50],
        "give_mask": [0, 1, 5, 10, 15, 20, 30, 40, 50],
        "give_gun": [0, 1, 5, 10, 15, 20, 30, 40, 50],
        "give_docs": [0, 1, 5, 10, 15, 20, 30, 40, 50, 70, 100, 150, 200, 500, 1000],
        "give_protect": [0, 1, 5, 10, 15, 20, 30, 40, 50],
        "give_anti_killer": [0, 1, 5, 10, 15, 20, 30, 40, 50],
        "give_vote_protect": [0, 1, 5, 10, 15, 20, 30, 40, 50],
    }

    if key not in titles or key not in presets:
        return await cb.answer("❌", show_alert=True)
    try:
        await cb.message.edit_text(titles.get(key, "Giveaway"), reply_markup=giveaway_item_kb(chat_id, key, presets[key]))
    except Exception:
        pass
    return await cb.answer()


@dp.callback_query(F.data.startswith("givecfg:set:"))
async def cb_givecfg_set(cb: CallbackQuery) -> None:
    parts = cb.data.split(":")
    if len(parts) != 5:
        return await cb.answer()
    chat_id = int(parts[2])
    key = parts[3]
    val = parts[4]
    if not await is_admin(chat_id, cb.from_user.id):
        return await cb.answer("❌ Faqat admin", show_alert=True)

    try:
        ival = int(val)
    except Exception:
        return await cb.answer("❌", show_alert=True)

    if key == "min7":
        if ival not in {0, 1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50}:
            return await cb.answer("❌", show_alert=True)
        g = get_group(chat_id)
        g["give_min_games_7d"] = ival
        update_group(chat_id, g)
        await cb.answer("✅")
        try:
            await cb.message.edit_reply_markup(reply_markup=giveaway_min_games_kb(chat_id))
        except Exception:
            pass
        return

    allowed = {
        "give_games": {0, 1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50},
        "give_diamonds": {0, 1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50},
        "give_mask": {0, 1, 5, 10, 15, 20, 30, 40, 50},
        "give_gun": {0, 1, 5, 10, 15, 20, 30, 40, 50},
        "give_docs": {0, 1, 5, 10, 15, 20, 30, 40, 50, 70, 100, 150, 200, 500, 1000},
        "give_protect": {0, 1, 5, 10, 15, 20, 30, 40, 50},
        "give_anti_killer": {0, 1, 5, 10, 15, 20, 30, 40, 50},
        "give_vote_protect": {0, 1, 5, 10, 15, 20, 30, 40, 50},
    }
    if key not in allowed or ival not in allowed[key]:
        return await cb.answer("❌", show_alert=True)
    g = get_group(chat_id)
    g[key] = ival
    update_group(chat_id, g)
    await cb.answer("✅")
    try:
        titles = {
            "give_games": "🎲 Kolichestvo igr",
            "give_diamonds": "💎 Olmos",
            "give_mask": "🎭 Maska",
            "give_gun": "🔫 Miltiq",
            "give_docs": "📁 Hujjatlar",
            "give_protect": "🛡 Himoya",
            "give_anti_killer": "⛑️ Qotildan himoya",
            "give_vote_protect": "⚖️ Ovoz himoyasi",
        }
        await cb.message.edit_reply_markup(
            reply_markup=giveaway_item_kb(chat_id, key, sorted(list(allowed[key])))
        )
    except Exception:
        pass


@dp.callback_query(F.data.startswith("weap:open:"))
async def cb_weap_open(cb: CallbackQuery) -> None:
    parts = cb.data.split(":")
    if len(parts) != 4:
        return await cb.answer()
    chat_id = int(parts[2])
    key = parts[3]
    if not await is_admin(chat_id, cb.from_user.id):
        return await cb.answer("❌ Faqat admin", show_alert=True)
    g = get_group(chat_id)
    cur = bool(g.get(key, True))
    title_map = {
        "allow_fake_docs": "Soxta hujjatlarga ruxsat berilsinmi?",
        "allow_protect": "Himoyaga ruxsat berilsinmi?",
        "allow_mask": "Maskaga ruxsat berilsinmi?",
        "allow_gun": "Miltiqqa ruxsat berilsinmi?",
        "allow_anti_killer": "Qotildan himoyaga ruxsat berilsinmi?",
        "allow_vote_protect": "Ovoz berishni himoya qilishga ruxsat berilsinmi?",
    }
    await cb.message.edit_text(
        title_map.get(key, "Ruxsat berilsinmi?"),
        reply_markup=yesno_kb(
            back_cb=f"adminset:weapons:{chat_id}",
            yes_cb=f"weap:set:{chat_id}:{key}:1",
            no_cb=f"weap:set:{chat_id}:{key}:0",
            cur=cur,
        ),
    )
    return await cb.answer()


@dp.callback_query(F.data.startswith("weap:set:"))
async def cb_weap_set(cb: CallbackQuery) -> None:
    parts = cb.data.split(":")
    if len(parts) != 5:
        return await cb.answer()
    chat_id = int(parts[2])
    key = parts[3]
    val = parts[4] == "1"
    if not await is_admin(chat_id, cb.from_user.id):
        return await cb.answer("❌ Faqat admin", show_alert=True)
    g = get_group(chat_id)
    g[key] = val
    update_group(chat_id, g)
    await cb.answer("✅")
    try:
        await cb.message.edit_reply_markup(
            reply_markup=yesno_kb(
                back_cb=f"adminset:weapons:{chat_id}",
                yes_cb=f"weap:set:{chat_id}:{key}:1",
                no_cb=f"weap:set:{chat_id}:{key}:0",
                cur=val,
            )
        )
    except Exception:
        pass


@dp.callback_query(F.data.startswith("other:"))
async def cb_other(cb: CallbackQuery) -> None:
    parts = cb.data.split(":")
    if len(parts) < 4:
        return await cb.answer()
    action = parts[1]
    chat_id = int(parts[2])
    key = parts[3]
    if not await is_admin(chat_id, cb.from_user.id):
        return await cb.answer("❌ Faqat admin", show_alert=True)

    if action == "open":
        title_map = {
            "other_group_roles": "Rollarni guruhlash?",
            "other_skip_day_vote": "Kunduzgi ovoz berishda navbatni o'tkazib yuborish?",
            "other_skip_night_vote": "Tungi navbatni o'tkazib yuborish?",
            "other_action_announces": "Tun harakatlari xabarlari ko'rsatilsinmi?",
            "other_media_messages": "Media xabarlar yoqilsinmi?",
            "other_show_roles_in_reg": "Ro'yxatdan o'tishda rollar ko'rsatilsinmi?",
            "other_advokat_mafia": "Advokat mafiyaga kiradimi?",
            "other_don_vote": "Don ovoz berishda ishtirok etadimi?",
            "pin_registration": "Ro'yxat postini auto pin qilinsinmi?",
            "other_anonymous_vote": "Anonim ovoz berish yoqilsinmi?",
            "other_leave_cmd": "Guruhda /leave buyruği yoqilsinmi?",
            "other_show_emoji": "Emoji ko'rsatilsinmi?",
            "other_anyone_can_reg": "Barcha foydalanuvchilarga ro'yxatdan o'tishni boshlashga ruxsat berilsinmi?",
            "other_anyone_can_start": "Barcha foydalanuvchilarga o'yinni boshlashga ruxsat berilsinmi?",
        }
        if key not in title_map:
            return await cb.answer("❌", show_alert=True)
        g = get_group(chat_id)
        cur_default_true = key in {"other_action_announces", "other_media_messages", "pin_registration", "other_leave_cmd", "other_show_emoji"}
        cur = bool(g.get(key, True if cur_default_true else False))
        try:
            await cb.message.edit_text(
                title_map[key],
                reply_markup=yesno_kb(
                    back_cb=f"adminset:other:{chat_id}",
                    yes_cb=f"other:set:{chat_id}:{key}:1",
                    no_cb=f"other:set:{chat_id}:{key}:0",
                    cur=cur,
                ),
            )
        except Exception:
            pass
        return await cb.answer()

    if action == "set":
        if len(parts) != 5:
            return await cb.answer()
        val = parts[4] == "1"
        g = get_group(chat_id)
        g[key] = val
        update_group(chat_id, g)
        await cb.answer("✅")
        try:
            await cb.message.edit_reply_markup(
                reply_markup=yesno_kb(
                    back_cb=f"adminset:other:{chat_id}",
                    yes_cb=f"other:set:{chat_id}:{key}:1",
                    no_cb=f"other:set:{chat_id}:{key}:0",
                    cur=val,
                )
            )
        except Exception:
            pass
        return
    if action == "toggle":
        g = get_group(chat_id)
        g[key] = not bool(g.get(key, False))
        update_group(chat_id, g)
        await cb.answer("✅")
        try:
            await cb.message.edit_reply_markup(reply_markup=other_main_kb(chat_id))
        except Exception:
            pass
        return
    if action == "do":
        try:
            if key == "reg":
                await _admin_open_registration(chat_id, cb.from_user.id)
                return await cb.answer("✅", show_alert=False)
            if key == "start":
                await _admin_start_game(chat_id, cb.from_user.id)
                return await cb.answer("✅", show_alert=False)
            if key == "extend":
                await _admin_extend_reg(chat_id, cb.from_user.id)
                return await cb.answer("✅", show_alert=False)
            if key == "pause":
                await _admin_stop_game(chat_id, cb.from_user.id)
                return await cb.answer("✅", show_alert=False)
        except Exception:
            return await cb.answer("❌", show_alert=True)
        return await cb.answer("❌", show_alert=True)


@dp.callback_query(F.data.startswith("givecfg:add:"))
async def cb_givecfg_add(cb: CallbackQuery) -> None:
    parts = cb.data.split(":")
    if len(parts) != 4:
        return await cb.answer()
    chat_id = int(parts[2])
    key = parts[3]
    if not await is_admin(chat_id, cb.from_user.id):
        return await cb.answer("❌ Faqat admin", show_alert=True)
    g = get_group(chat_id)
    g[key] = int(g.get(key, 0)) + 1
    update_group(chat_id, g)
    try:
        await cb.message.edit_reply_markup(reply_markup=giveaway_kb(chat_id))
    except Exception:
        pass
    return await cb.answer("✅")


@dp.callback_query(F.data.startswith("buycoins:"))
async def cb_buycoins(cb: CallbackQuery) -> None:
    parts = cb.data.split(":")
    if len(parts) != 3:
        return await cb.answer()
    diamonds = int(parts[1])
    money = int(parts[2])
    u = get_user(cb.from_user.id, cb.from_user.full_name)
    if int(u.get("diamonds", 0)) < diamonds:
        return await cb.answer("❌ Olmos yetarli emas", show_alert=True)
    u["diamonds"] = int(u.get("diamonds", 0)) - diamonds
    u["money"] = int(u.get("money", 0)) + money
    update_user(cb.from_user.id, u)
    return await cb.answer("✅", show_alert=True)


@dp.callback_query(F.data.startswith("buydiamond:"))
async def cb_buydiamond(cb: CallbackQuery) -> None:
    parts = cb.data.split(":")
    if len(parts) != 2:
        return await cb.answer()
    try:
        diamonds = int(parts[1])
    except Exception:
        return await cb.answer("❌", show_alert=True)

    if diamonds not in DIAMOND_PACKS_UZS:
        return await cb.answer("❌", show_alert=True)
    try:
        await cb.message.edit_text(
            "Qancha olmos sotib olmoqchisiz?\n\n"
            "To'lovni amalga oshiring. Chek/skrinshotni admin'ga yuboring. Admin sizga olmosni /give orqali beradi.",
            reply_markup=buy_diamond_methods_kb(diamonds),
        )
    except Exception:
        pass


async def _send_diamond_invoice(cb: CallbackQuery, diamonds: int) -> None:
    if diamonds not in DIAMOND_PACKS_UZS:
        return await cb.answer("❌", show_alert=True)
    if not PAYMENT_PROVIDER_TOKEN:
        await cb.answer("⏳ To'lov tizimi ulanmagan", show_alert=True)
        try:
            await cb.message.edit_text(
                "💎 Olmos sotib olish:\n\nHozircha to'lov tizimi ulanmagan.",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[[InlineKeyboardButton(text="⬅️ Orqaga", callback_data="menu:profile")]]
                ),
            )
        except Exception:
            pass
        return

    amount_uzs = int(DIAMOND_PACKS_UZS[diamonds])
    amount_minor = amount_uzs * 100
    payload = f"buy_diamond:{cb.from_user.id}:{diamonds}"
    prices = [LabeledPrice(label=f"💎 {diamonds} olmos", amount=amount_minor)]
    await cb.answer()
    try:
        await bot.send_invoice(
            chat_id=cb.from_user.id,
            title=f"💎 {diamonds} olmos",
            description=f"{diamonds} ta olmos sotib olish (UZS)",
            payload=payload,
            provider_token=PAYMENT_PROVIDER_TOKEN,
            currency="UZS",
            prices=prices,
        )
    except Exception:
        try:
            me = await bot.get_me()
            await cb.message.edit_text(
                "❌ Invoice yuborilmadi.\n\nIltimos botga private'da /start bosing va qayta urinib ko'ring.",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="Bot-ga o'tish", url=f"https://t.me/{me.username}")],
                        [InlineKeyboardButton(text="⬅️ Orqaga", callback_data="menu:profile")],
                    ]
                ),
            )
        except Exception:
            pass


@dp.callback_query(F.data.startswith("buydiamond_card:"))
async def cb_buydiamond_card(cb: CallbackQuery) -> None:
    parts = cb.data.split(":")
    if len(parts) != 2:
        return await cb.answer()
    try:
        diamonds = int(parts[1])
    except Exception:
        return await cb.answer("❌", show_alert=True)
    if diamonds not in DIAMOND_PACKS_UZS:
        return await cb.answer("❌", show_alert=True)
    try:
        await cb.message.edit_text(
            "💳 Karta orqali olish (manual):\n\n"
            "1) To'lovni amalga oshiring\n"
            "2) Chek/skrinshotni admin'ga yuboring\n"
            "3) Admin sizga olmosni /give orqali beradi\n\n"
            f"Tanlangan paket: 💎 {diamonds}",
            reply_markup=admin_manual_pay_kb(back_cb="shop:diamond"),
        )
    except Exception:
        pass
    return await cb.answer()


@dp.pre_checkout_query()
async def pre_checkout(pre: PreCheckoutQuery) -> None:
    payload = str(pre.invoice_payload or "")
    parts = payload.split(":")
    if len(parts) != 3 or parts[0] != "buy_diamond":
        try:
            await bot.answer_pre_checkout_query(pre.id, ok=False, error_message="❌ Xato payload")
        except Exception:
            pass
        return
    try:
        uid = int(parts[1])
        diamonds = int(parts[2])
    except Exception:
        try:
            await bot.answer_pre_checkout_query(pre.id, ok=False, error_message="❌ Xato")
        except Exception:
            pass
        return
    if diamonds not in DIAMOND_PACKS_UZS:
        try:
            await bot.answer_pre_checkout_query(pre.id, ok=False, error_message="❌ Paket topilmadi")
        except Exception:
            pass
        return
    expected_minor = int(DIAMOND_PACKS_UZS[diamonds]) * 100
    if str(pre.currency or "") != "UZS" or int(pre.total_amount or 0) != expected_minor:
        try:
            await bot.answer_pre_checkout_query(pre.id, ok=False, error_message="❌ Summa xato")
        except Exception:
            pass
        return
    try:
        await bot.answer_pre_checkout_query(pre.id, ok=True)
    except Exception:
        return


@dp.message(F.successful_payment)
async def on_successful_payment(msg: Message) -> None:
    sp = msg.successful_payment
    payload = str(sp.invoice_payload or "")
    parts = payload.split(":")
    if len(parts) != 3 or parts[0] != "buy_diamond":
        return
    try:
        uid = int(parts[1])
        diamonds = int(parts[2])
    except Exception:
        return
    if msg.from_user and int(msg.from_user.id) != uid:
        return
    if diamonds not in DIAMOND_PACKS_UZS:
        return

    expected_minor = int(DIAMOND_PACKS_UZS[diamonds]) * 100
    if int(sp.total_amount or 0) != expected_minor or str(sp.currency or "") != "UZS":
        return

    charge_id = str(getattr(sp, "telegram_payment_charge_id", "") or "")
    if not charge_id:
        return

    u = get_user(uid, msg.from_user.full_name if msg.from_user else "User")
    paid_ids = list(u.get("paid_ids", []) or [])
    if charge_id in paid_ids:
        return
    paid_ids.append(charge_id)
    u["paid_ids"] = paid_ids[-50:]
    u["diamonds"] = int(u.get("diamonds", 0)) + diamonds
    update_user(uid, u)
    try:
        await msg.answer(f"✅ To'lov qabul qilindi. +{diamonds}💎")
    except Exception:
        pass


@dp.message(Command("give1"), F.chat.type.in_({"group", "supergroup"}))
async def cmd_give1(msg: Message) -> None:
    if not await is_admin(msg.chat.id, msg.from_user.id):
        return
    text = "A baham ko'rish 💎 1 olmoslari!\n💎 1 ta olish uchun bosing!"
    sent = await msg.answer(
        text,
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="Bosing", callback_data=f"give:{msg.chat.id}:1")]]
        ),
    )
    key = (msg.chat.id, sent.message_id)
    GIVEAWAYS[key]["claimed"] = set()
    GIVEAWAYS[key]["amount"] = 1


@dp.message(Command("give"), F.chat.type.in_({"group", "supergroup"}))
async def cmd_give(msg: Message) -> None:
    if not await is_admin(msg.chat.id, msg.from_user.id):
        return

    parts = (msg.text or "").split()
    if len(parts) < 2:
        try:
            await msg.reply("❌ Foydalanish: /give <olmos> (reply bilan)")
        except Exception:
            pass
        return

    if not msg.reply_to_message or not msg.reply_to_message.from_user:
        try:
            await msg.reply("❌ Kimga berishni bilish uchun odamning xabariga reply qiling.")
        except Exception:
            pass
        return

    try:
        amount = int(parts[1])
    except Exception:
        amount = 0
    if amount <= 0 or amount > 1000000:
        try:
            await msg.reply("❌ Olmos soni xato")
        except Exception:
            pass
        return

    target = msg.reply_to_message.from_user
    u = get_user(target.id, target.full_name)
    u["diamonds"] = int(u.get("diamonds", 0)) + amount
    update_user(target.id, u)
    try:
        await msg.reply(f"✅ {target.full_name} ga +{amount}💎 berildi")
    except Exception:
        pass

    return


@dp.callback_query(F.data.startswith("give:"))
async def cb_give(cb: CallbackQuery) -> None:
    parts = cb.data.split(":")
    if len(parts) != 3:
        return await cb.answer()
    chat_id = int(parts[1])
    amount = int(parts[2])
    key = (chat_id, cb.message.message_id)
    state = GIVEAWAYS.get(key)
    if not state:
        return await cb.answer("❌", show_alert=True)
    try:
        claimed_raw = state.get("claimed", set())
        claimed = {str(x) for x in (claimed_raw or set())}
    except Exception:
        claimed = set()
    try:
        expected_amount = int(state.get("amount", amount))
    except Exception:
        expected_amount = amount
    if int(amount) != int(expected_amount):
        return await cb.answer("❌", show_alert=True)
    uid = str(cb.from_user.id)
    if uid in claimed:
        return await cb.answer("✅", show_alert=True)
    claimed.add(uid)
    state["claimed"] = claimed
    u = get_user(cb.from_user.id, cb.from_user.full_name)
    u["diamonds"] = int(u.get("diamonds", 0)) + amount
    update_user(cb.from_user.id, u)
    return await cb.answer("✅ +1💎", show_alert=True)


@dp.callback_query(F.data.startswith("group:"))
async def cb_group(cb: CallbackQuery) -> None:
    if cb.message.chat.type not in {"group", "supergroup"}:
        return await cb.answer()
    chat_id = cb.message.chat.id
    g = get_group(chat_id)
    action = cb.data.split(":", 1)[1]
    if action == "game":
        if not await is_admin(chat_id, cb.from_user.id) and not bool(g.get("other_anyone_can_reg", False)):
            return await cb.answer("❌ Faqat admin", show_alert=True)
    elif action == "start":
        if not await is_admin(chat_id, cb.from_user.id) and not bool(g.get("other_anyone_can_start", False)):
            return await cb.answer("❌ Faqat admin", show_alert=True)
    else:
        if not await is_admin(chat_id, cb.from_user.id):
            return await cb.answer("❌ Faqat admin", show_alert=True)

    await cb.answer()

    if action == "game":
        await cmd_game(cb.message)
    elif action == "start":
        await group_start(cb.message)
    elif action == "stop":
        await group_stop(cb.message)
    elif action == "extend":
        await group_extend(cb.message)
    elif action == "settings":
        await group_settings(cb.message)
    else:
        return

    if bool(get_group(chat_id).get("delete_commands", True)):
        await _safe_delete_message(chat_id, cb.message.message_id)
    return


@dp.chat_member(F.chat.type.in_({"group", "supergroup"}))
async def on_chat_member_update(ev: ChatMemberUpdated) -> None:
    try:
        chat_id = ev.chat.id
        chat_title = ev.chat.title or f"Group {chat_id}"
        
        # Always update group info when we get any chat member update
        get_group(chat_id, chat_title)
        
        user = ev.new_chat_member.user
        if not user:
            return
        new_status = getattr(ev.new_chat_member, "status", None)
        
        # Track user leaving group (but don't delete from users list)
        if new_status == "kicked":
            game = get_game(chat_id)
            uid = str(user.id)
            if uid in game.get("players", {}):
                game["players"].pop(uid, None)
                game.get("roles", {}).pop(uid, None)
                if uid in game.get("alive", []):
                    game["alive"].remove(uid)

                if game.get("phase") == PHASE_REG:
                    await _edit_group_status(
                        chat_id,
                        reply_markup=await reg_join_kb(chat_id),
                    )

                if game.get("started"):
                    outcome = _win_state(game)
                    if outcome:
                        t = game.get("loop_task")
                        if t:
                            try:
                                t.cancel()
                            except Exception:
                                pass
                        game["loop_task"] = None
                        await _finish_game(chat_id, str(outcome))
    except Exception:
        return


@dp.callback_query(F.data.startswith("set:"))
async def cb_set(cb: CallbackQuery) -> None:
    parts = cb.data.split(":")
    if len(parts) < 3:
        return await cb.answer()

    action = parts[1]
    chat_id = int(parts[2])

    if not await is_admin(chat_id, cb.from_user.id):
        return await cb.answer("❌ Faqat admin", show_alert=True)

    if action == "main":
        await cb.message.edit_text(f"⚙️ Guruh {chat_id} sozlamalari:", reply_markup=settings_kb(chat_id))
        return await cb.answer()

    if action == "default":
        update_group(chat_id, DEFAULT_GROUP.copy())
        await cb.answer("✅ Default", show_alert=True)
        await cb.message.edit_text(f"⚙️ Guruh {chat_id} sozlamalari:", reply_markup=settings_kb(chat_id))
        return

    if action == "toggle":
        key = parts[3]
        g = get_group(chat_id)
        g[key] = not bool(g.get(key, False))
        update_group(chat_id, g)
        await cb.answer("✅", show_alert=True)
        await cb.message.edit_text(f"⚙️ Guruh {chat_id} sozlamalari:", reply_markup=settings_kb(chat_id))
        return

    if action == "show":
        key = parts[3]
        await cb.message.edit_text(
            f"⏱ {key} sozlash:",
            reply_markup=time_adjust_kb(chat_id, key),
        )
        return await cb.answer()

    if action == "adj":
        key = parts[3]
        delta = int(parts[4])
        g = get_group(chat_id)
        g[key] = max(5, int(g.get(key, 0)) + delta)
        update_group(chat_id, g)
        await cb.answer("✅", show_alert=True)
        await cb.message.edit_text(
            f"⏱ {key} sozlash:",
            reply_markup=time_adjust_kb(chat_id, key),
        )
        return

    await cb.answer()


async def main() -> None:
    print(f"Starting bot with {TELEGRAM_LIB}")
    
    if TELEGRAM_LIB == "python-telegram-bot":
        # python-telegram-bot initialization
        print("Using python-telegram-bot library")
        
        # Basic handlers
        async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
            await update.message.reply_text("🎮 Mafia Bot is running!")
        
        async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
            await update.message.reply_text("📚 Use /start to begin!")
        
        # Add handlers
        application.add_handler(CommandHandler("start", start_command))
        application.add_handler(CommandHandler("help", help_command))
        
        # Start bot
        print("Starting bot polling...")
        await application.initialize()
        await application.start()
        await application.run_polling()
        
    else:
        # aiogram initialization (original code)
        # Railway compatibility: use PORT from environment
        import os
        port = int(os.environ.get('PORT', '8443'))
        
        # Disable setup_commands_menu for Railway to avoid timeout
        # await setup_commands_menu()
        
        global BOT_USERNAME
        try:
            BOT_USERNAME = (await bot.get_me()).username
        except Exception:
            BOT_USERNAME = None
        try:
            await _runtime_recover_on_startup()
        except Exception:
            pass
        try:
            dp.message.middleware(_DeleteGroupSlashCommandsMiddleware())
        except Exception:
            pass
        try:
            await bot.delete_webhook(drop_pending_updates=True)
        except Exception:
            pass
        
        # Railway webhook mode
        import os
        port = int(os.environ.get('PORT', '8443'))
        
        # Check if running on Railway
        railway_domain = os.environ.get('RAILWAY_PUBLIC_DOMAIN')
        if railway_domain:
            webhook_url = f"https://{railway_domain}/webhook"
            try:
                await bot.set_webhook(url=webhook_url)
                print("Webhook set:", webhook_url)
            except Exception as e:
                print("Webhook failed:", e)
                print("Falling back to polling mode...")
                await dp.start_polling(bot)
                return
        else:
            print("Not on Railway, using polling mode...")
            await dp.start_polling(bot)
            return
        
        # Start webhook server
        from aiogram.webhook.aiohttp_server import setup_application
        app = setup_application(dp, bot)
        
        import aiohttp
        async def handle_webhook(request):
            if request.method == 'POST':
                update_data = await request.json()
                await dp.feed_webhook_update(bot, update_data)
                return aiohttp.web.Response(status=200)
            return aiohttp.web.Response(status=200)
        
        web_app = aiohttp.web.Application()
        web_app.router.add_post('/webhook', handle_webhook)
        
        runner = aiohttp.web.AppRunner(web_app)
        await runner.setup()
        site = aiohttp.web.TCPSite(runner, port=port)
        
        print(f"Starting webhook server on port {port}")
        await site.start()
        print("Bot is running on Railway!")


if __name__ == "__main__":
    asyncio.run(main())
