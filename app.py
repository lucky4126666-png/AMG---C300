import os
import re
import ssl
import time
import asyncio
import contextlib
import logging
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F, types
from aiogram.enums import ParseMode
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.exceptions import TelegramBadRequest
from aiogram.client.default import DefaultBotProperties

from sqlalchemy import Column, Integer, String, Text, select, delete
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base

# ======================
# BASIC
# ======================
logging.basicConfig(level=logging.INFO)
load_dotenv()


def normalize_database_url(url: str | None) -> str:
    if not url:
        return "sqlite+aiosqlite:///bot.db"
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql+asyncpg://", 1)
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return url


BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = normalize_database_url(os.getenv("DATABASE_URL"))
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
ADMIN_IDS = {
    int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip().isdigit()
}

if not BOT_TOKEN:
    raise RuntimeError("Thiếu BOT_TOKEN trong .env")

# ======================
# DB ENGINE
# ======================
engine_kwargs = {"echo": False, "future": True}

if DATABASE_URL.startswith("sqlite"):
    engine_kwargs["connect_args"] = {"check_same_thread": False}
else:
    ssl_context = ssl.create_default_context()
    engine_kwargs["pool_size"] = 10
    engine_kwargs["max_overflow"] = 20
    engine_kwargs["pool_timeout"] = 30
    engine_kwargs["pool_recycle"] = 1800
    engine_kwargs["connect_args"] = {"ssl": ssl_context}

engine = create_async_engine(DATABASE_URL, **engine_kwargs)
SessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

# ======================
# BOT
# ======================
bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher()

# ======================
# MEMORY / CACHE
# ======================
admin_cache = set()

keyword_cache = []       # list[dict]
welcome_cache = {}       # chat_id -> dict
auto_cache = []          # list[dict]

private_menu_msg = {}    # uid -> message_id
selected_group = {}      # uid -> chat_id
selected_lang = {}       # uid -> "zh"/"vi"

user_state = {}          # uid -> str
temp = {}                # uid -> dict

last_trigger = {}        # (chat_id, key) -> ts
worker_task: asyncio.Task | None = None

SPAM_WINDOW = 5
SPAM_CLEAN_TTL = 300

STRANGER_START_TEXT = "欢迎使用机器人，请点击下方按钮："
INIT_GROUP_TEXT = "组防骗助手为您服务,我正在进行相关初始化配置请稍后"

# ======================
# MODELS
# ======================
class AdminUser(Base):
    __tablename__ = "admin_users"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, unique=True, index=True)
    note = Column(String, default="")
    created_at = Column(Integer, default=0)


class BotGroup(Base):
    __tablename__ = "bot_groups"
    id = Column(Integer, primary_key=True)
    chat_id = Column(String, unique=True, index=True)
    title = Column(String, default="")
    type = Column(String, default="group")
    is_admin = Column(Integer, default=0)
    updated_at = Column(Integer, default=0)


class Keyword(Base):
    __tablename__ = "keywords"
    id = Column(Integer, primary_key=True)
    key = Column(String, unique=True, index=True)
    mode = Column(String, default="exact")   # exact / contains
    active = Column(Integer, default=1)
    text = Column(Text, default="")
    image = Column(Text, default="")
    button = Column(Text, default="")


class WelcomeSetting(Base):
    __tablename__ = "welcome_settings"
    id = Column(Integer, primary_key=True)
    chat_id = Column(String, unique=True, index=True)
    active = Column(Integer, default=0)
    text = Column(Text, default="")
    image = Column(Text, default="")
    button = Column(Text, default="")
    delete_after = Column(Integer, default=0)
    pin = Column(Integer, default=0)


class AutoPost(Base):
    __tablename__ = "auto_post"
    id = Column(Integer, primary_key=True)
    chat_id = Column(String, index=True)
    active = Column(Integer, default=0)
    text = Column(Text, default="")
    image = Column(Text, default="")
    button = Column(Text, default="")
    interval = Column(Integer, default=10)
    pin = Column(Integer, default=0)
    start_at = Column(Text, default="")
    end_at = Column(Text, default="")
    last_sent_ts = Column(Integer, default=0)

# ======================
# AUTH
# ======================
def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID


def is_allowed_user(user_id: int) -> bool:
    return is_owner(user_id) or user_id in ADMIN_IDS or user_id in admin_cache


def can_change_language(user_id: int) -> bool:
    return is_allowed_user(user_id)


async def load_admin_cache():
    global admin_cache
    try:
        async with SessionLocal() as db:
            rows = (await db.execute(select(AdminUser))).scalars().all()
        admin_cache = {r.user_id for r in rows}
        logging.info("Loaded admin cache: %s", admin_cache)
    except Exception as e:
        admin_cache = set()
        logging.exception("[ADMIN CACHE ERROR] %s", e)


async def add_admin_db(user_id: int, note: str = ""):
    async with SessionLocal() as db:
        row = (await db.execute(
            select(AdminUser).where(AdminUser.user_id == user_id)
        )).scalars().first()

        now_ts = int(time.time())
        if not row:
            row = AdminUser(user_id=user_id, note=note, created_at=now_ts)
            db.add(row)
        else:
            row.note = note
        await db.commit()

    await load_admin_cache()


async def remove_admin_db(user_id: int):
    async with SessionLocal() as db:
        await db.execute(delete(AdminUser).where(AdminUser.user_id == user_id))
        await db.commit()

    await load_admin_cache()


def reset(uid: int):
    user_state.pop(uid, None)
    temp.pop(uid, None)


async def ack(c: types.CallbackQuery, text: str | None = None):
    with contextlib.suppress(Exception):
        await c.answer(text=text)


async def allowed_or_ignore(c: types.CallbackQuery):
    if not c.from_user or not is_allowed_user(c.from_user.id):
        await ack(c)
        return False
    return True

# ======================
# HELPERS
# ======================
def message_text(m: types.Message) -> str:
    return (m.text or m.caption or "").strip()


def is_valid_button_url(url: str) -> bool:
    if not url:
        return False
    url = url.strip()
    return bool(re.match(r"^(https?://|tg://|www\.)", url, re.IGNORECASE))


def parse_buttons(text):
    if not text:
        return None

    rows = []
    for line in text.split("\n"):
        row = []
        for part in line.split("&&"):
            part = part.strip()
            if not part:
                continue

            if " - " in part:
                t, u = part.split(" - ", 1)
            elif "-" in part:
                t, u = part.split("-", 1)
            else:
                continue

            t = t.strip()
            u = u.strip()

            if not t or not u:
                continue
            if not is_valid_button_url(u):
                logging.warning("[BUTTON INVALID URL] %s", u)
                continue

            row.append({"text": t, "url": u})

        if row:
            rows.append(row)

    return rows or None


def build_buttons(data):
    if not data:
        return None
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=b["text"], url=b["url"]) for b in row]
            for row in data
        ]
    )


def extract_image_from_message(m: types.Message):
    if m.photo:
        return m.photo[-1].file_id
    if m.text:
        return m.text.strip()
    if m.caption:
        return m.caption.strip()
    return None


def parse_dt(s: str):
    if not s:
        return None
    try:
        return datetime.strptime(s.strip(), "%Y-%m-%d %H:%M")
    except Exception:
        return None


async def safe_edit(message: types.Message, text_: str, reply_markup=None):
    try:
        return await message.edit_text(text_, reply_markup=reply_markup)
    except TelegramBadRequest:
        return await message.answer(text_, reply_markup=reply_markup)
    except Exception:
        return await message.answer(text_, reply_markup=reply_markup)


async def send_preview(chat_id, text=None, image=None, button=None):
    kb = build_buttons(parse_buttons(button))
    try:
        if image:
            return await bot.send_photo(
                chat_id=chat_id,
                photo=image,
                caption=text or None,
                reply_markup=kb
            )
        return await bot.send_message(
            chat_id=chat_id,
            text=text or " ",
            reply_markup=kb,
            disable_web_page_preview=True
        )
    except Exception as e:
        logging.exception("[SEND_PREVIEW ERROR] chat_id=%s error=%s", chat_id, e)
        raise


def cleanup_last_trigger():
    now = time.time()
    expired = [k for k, ts in last_trigger.items() if now - ts > SPAM_CLEAN_TTL]
    for k in expired:
        last_trigger.pop(k, None)


async def send_or_edit_private_home(uid: int, chat_id: int):
    old_msg_id = private_menu_msg.get(uid)
    if old_msg_id:
        with contextlib.suppress(Exception):
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=old_msg_id,
                text="🏠 首页",
                reply_markup=start_menu_kb(uid)
            )
            return

    msg = await bot.send_message(chat_id, "🏠 首页", reply_markup=start_menu_kb(uid))
    private_menu_msg[uid] = msg.message_id

# ======================
# MENUS
# ======================
def stranger_start_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="➕ 添加机器人进群",
            url="https://t.me/nnnnzubot?startgroup=foo"
        )],
        [InlineKeyboardButton(
            text="🌐 官方服务",
            url="https://t.me/xbkf/"
        )]
    ])


def init_group_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="公群导航", url="https://t.me/gqdh"),
            InlineKeyboardButton(text="供需频道", url="https://t.me/gqdh"),
        ]
    ])


def start_menu_kb(uid: Optional[int] = None):
    kb = [
        [InlineKeyboardButton(text="👑 管理员设置", callback_data="admin_menu")],
        [InlineKeyboardButton(text="👥 群组管理", callback_data="group_menu")],
    ]
    if uid is not None and can_change_language(uid):
        kb.append([InlineKeyboardButton(text="🌐 语言", callback_data="lang_menu")])
    return InlineKeyboardMarkup(inline_keyboard=kb)


def admin_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📌 关键词", callback_data="kw_menu")],
        [InlineKeyboardButton(text="👋 群组欢迎", callback_data="wl_menu")],
        [InlineKeyboardButton(text="📅 定时发送", callback_data="auto_menu")],
        [InlineKeyboardButton(text="👮 管理员", callback_data="admin_manage_menu")],
        [InlineKeyboardButton(text="🌐 语言", callback_data="lang_menu")],
        [InlineKeyboardButton(text="⬅️ 返回", callback_data="back_start")],
    ])


def admin_manage_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 管理员列表", callback_data="admin_list")],
        [InlineKeyboardButton(text="➕ 添加管理员", callback_data="admin_add")],
        [InlineKeyboardButton(text="➖ 删除管理员", callback_data="admin_del")],
        [InlineKeyboardButton(text="⬅️ 返回", callback_data="admin_menu")],
    ])


def group_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 群组列表", callback_data="group_list")],
        [InlineKeyboardButton(text="➕ 选择群组", callback_data="group_pick")],
        [InlineKeyboardButton(text="⬅️ 返回", callback_data="back_start")],
    ])


def lang_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🇻🇳 越南语", callback_data="lang_vi")],
        [InlineKeyboardButton(text="🇨🇳 中文", callback_data="lang_zh")],
        [InlineKeyboardButton(text="⬅️ 返回", callback_data="back_start")],
    ])


def group_select_kb(groups):
    kb = []
    for g in groups:
        title = g.title or g.chat_id
        admin_mark = " 👑" if g.is_admin else ""
        kb.append([
            InlineKeyboardButton(
                text=f"{title}{admin_mark}",
                callback_data=f"pick_group_{g.chat_id}"
            )
        ])
    kb.append([InlineKeyboardButton(text="⬅️ 返回", callback_data="back_start")])
    return InlineKeyboardMarkup(inline_keyboard=kb)


def kw_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ 添加", callback_data="kw_add")],
        [InlineKeyboardButton(text="📋 列表", callback_data="kw_list")],
        [InlineKeyboardButton(text="🔙 首页", callback_data="back_start")],
    ])


def wl_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ 新建", callback_data="wl_add")],
        [InlineKeyboardButton(text="📋 列表", callback_data="wl_list")],
        [InlineKeyboardButton(text="🔙 首页", callback_data="back_start")],
    ])


def auto_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ 新建", callback_data="auto_add")],
        [InlineKeyboardButton(text="📋 列表", callback_data="auto_list")],
        [InlineKeyboardButton(text="🔙 首页", callback_data="back_start")],
    ])

# ======================
# DB CACHE LOAD
# ======================
async def ensure_schema():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def load_cache():
    global keyword_cache, welcome_cache, auto_cache

    async with SessionLocal() as db:
        keywords = (await db.execute(
            select(Keyword).where(Keyword.active == 1)
        )).scalars().all()

        welcomes = (await db.execute(
            select(WelcomeSetting).where(WelcomeSetting.active == 1)
        )).scalars().all()

        autos = (await db.execute(
            select(AutoPost).where(AutoPost.active == 1)
        )).scalars().all()

    keyword_cache = [
        {
            "id": k.id,
            "key": k.key,
            "mode": k.mode,
            "text": k.text or "",
            "image": k.image or "",
            "button": k.button or "",
        }
        for k in keywords
    ]

    welcome_cache = {
        w.chat_id: {
            "id": w.id,
            "chat_id": w.chat_id,
            "text": w.text or "",
            "image": w.image or "",
            "button": w.button or "",
            "delete_after": int(w.delete_after or 0),
            "pin": int(w.pin or 0),
        }
        for w in welcomes
    }

    auto_cache = [
        {
            "id": p.id,
            "chat_id": p.chat_id,
            "text": p.text or "",
            "image": p.image or "",
            "button": p.button or "",
            "interval": int(p.interval or 10),
            "pin": int(p.pin or 0),
            "start_at": p.start_at or "",
            "end_at": p.end_at or "",
            "last_sent_ts": int(p.last_sent_ts or 0),
        }
        for p in autos
    ]

# ======================
# DB HELPERS
# ======================
async def get_admin_groups():
    async with SessionLocal() as db:
        groups = (await db.execute(
            select(BotGroup).where(BotGroup.is_admin == 1).order_by(BotGroup.id.desc())
        )).scalars().all()
    return groups


async def get_all_groups():
    async with SessionLocal() as db:
        groups = (await db.execute(
            select(BotGroup).order_by(BotGroup.id.desc())
        )).scalars().all()
    return groups


async def get_all_admin_users():
    async with SessionLocal() as db:
        rows = (await db.execute(
            select(AdminUser).order_by(AdminUser.id.desc())
        )).scalars().all()
    return rows


async def show_admin_list(message):
    rows = await get_all_admin_users()

    lines = ["<b>管理员列表</b>\n"]
    lines.append(f"Owner: <code>{OWNER_ID}</code>")
    if ADMIN_IDS:
        lines.append("ENV ADMIN_IDS: " + ", ".join(f"<code>{x}</code>" for x in sorted(ADMIN_IDS)))
    else:
        lines.append("ENV ADMIN_IDS: <i>空</i>")

    if rows:
        lines.append("\nDB Admins:")
        for r in rows:
            note = f" — {r.note}" if r.note else ""
            lines.append(f"• <code>{r.user_id}</code>{note}")
    else:
        lines.append("\nDB Admins: <i>空</i>")

    await safe_edit(message, "\n".join(lines), reply_markup=admin_manage_menu_kb())


async def show_kw_list(message):
    async with SessionLocal() as db:
        rows = (await db.execute(
            select(Keyword).order_by(Keyword.id.desc())
        )).scalars().all()

    kb = []
    for k in rows:
        kb.append([
            InlineKeyboardButton(
                text=f"{k.key} ({'✅' if k.active else '❌'})",
                callback_data=f"kw_view_{k.id}"
            ),
            InlineKeyboardButton(text="🗑", callback_data=f"kw_del_{k.id}")
        ])
    kb.append([InlineKeyboardButton(text="🔙", callback_data="kw_menu")])

    await safe_edit(
        message,
        "关键词列表" if rows else "暂无关键词。",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
    )


async def show_kw_view(message, kid):
    async with SessionLocal() as db:
        k = await db.get(Keyword, kid)

    if not k:
        return await message.answer("关键词不存在。")

    await safe_edit(
        message,
        f"关键词详情\n\n"
        f"关键词：{k.key}\n"
        f"模式：{'精确匹配' if k.mode == 'exact' else '包含匹配'}\n"
        f"启用：{'✅' if k.active else '❌'}\n"
        f"按钮：{'✅' if k.button else '❌'}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text=f"模式：{'✅ 精确匹配' if k.mode == 'exact' else '包含匹配'}",
                callback_data=f"kw_mode_{k.id}"
            )],
            [InlineKeyboardButton(
                text=f"状态：{'✅ 开启' if k.active else '❌ 关闭'}",
                callback_data=f"kw_toggle_{k.id}"
            )],
            [InlineKeyboardButton(text="📝 修改关键词", callback_data=f"kw_key_{k.id}")],
            [InlineKeyboardButton(text="📝 修改文本", callback_data=f"kw_text_{k.id}")],
            [InlineKeyboardButton(text="🖼 修改媒体", callback_data=f"kw_img_{k.id}")],
            [InlineKeyboardButton(text="🔤 修改按钮", callback_data=f"kw_btn_{k.id}")],
            [InlineKeyboardButton(text="👀 预览", callback_data=f"kw_pre_{k.id}")],
            [InlineKeyboardButton(text="⬅️ 返回", callback_data="kw_list")],
        ])
    )


async def show_wl_list(message):
    async with SessionLocal() as db:
        rows = (await db.execute(
            select(WelcomeSetting).order_by(WelcomeSetting.id.desc())
        )).scalars().all()

    kb = []
    for w in rows:
        kb.append([
            InlineKeyboardButton(
                text=f"{w.chat_id} ({'✅' if w.active else '❌'})",
                callback_data=f"wl_view_{w.id}"
            ),
            InlineKeyboardButton(text="🗑", callback_data=f"wl_del_{w.id}")
        ])

    kb.append([InlineKeyboardButton(text="🔙", callback_data="wl_menu")])

    await safe_edit(
        message,
        "群组欢迎设置" if rows else "暂无群组欢迎配置。",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
    )


async def show_wl_view(message, wid):
    async with SessionLocal() as db:
        w = await db.get(WelcomeSetting, wid)

    if not w:
        return await message.answer("欢迎配置不存在。")

    await safe_edit(
        message,
        f"群组欢迎详情\n\n"
        f"状态：{'开启' if w.active else '关闭'}\n"
        f"删除消息：{w.delete_after if w.delete_after else '不删除'} 分钟\n"
        f"图片：{'✅' if w.image else '❌'}\n"
        f"按钮：{'✅' if w.button else '❌'}\n"
        f"文本：{'✅' if w.text else '❌'}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text=f"状态：{'✅ 开启' if w.active else '❌ 关闭'}",
                callback_data=f"wl_toggle_{w.id}"
            )],
            [InlineKeyboardButton(
                text=f"删除消息：{w.delete_after if w.delete_after else 0} 分钟",
                callback_data=f"wl_delmin_{w.id}"
            )],
            [InlineKeyboardButton(text="📌 置顶", callback_data=f"wl_pin_{w.id}")],
            [InlineKeyboardButton(text="📝 修改文本", callback_data=f"wl_text_{w.id}")],
            [InlineKeyboardButton(text="📷 修改媒体", callback_data=f"wl_img_{w.id}")],
            [InlineKeyboardButton(text="🔤 修改按钮", callback_data=f"wl_btn_{w.id}")],
            [InlineKeyboardButton(text="👀 预览", callback_data=f"wl_pre_{w.id}")],
            [InlineKeyboardButton(text="⬅️ 返回", callback_data="wl_list")],
        ])
    )


async def show_auto_list(message):
    async with SessionLocal() as db:
        rows = (await db.execute(
            select(AutoPost).order_by(AutoPost.id.desc())
        )).scalars().all()

    kb = []
    for p in rows:
        kb.append([
            InlineKeyboardButton(
                text=f"{p.id} ({'✅' if p.active else '❌'})",
                callback_data=f"auto_view_{p.id}"
            ),
            InlineKeyboardButton(text="🗑", callback_data=f"auto_del_{p.id}")
        ])

    kb.append([InlineKeyboardButton(text="🔙", callback_data="auto_menu")])

    await safe_edit(
        message,
        "定时发送列表" if rows else "暂无定时发送。",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
    )


async def show_auto_view(message, pid):
    async with SessionLocal() as db:
        p = await db.get(AutoPost, pid)

    if not p:
        return await message.answer("定时发送不存在。")

    last_time = "-"
    if p.last_sent_ts:
        last_time = datetime.fromtimestamp(p.last_sent_ts).strftime("%Y-%m-%d %H:%M:%S")

    await safe_edit(
        message,
        f"定时发送详情\n\n"
        f"状态：{'✅ 开启' if p.active else '❌ 关闭'}\n"
        f"间隔：{p.interval} 分钟\n"
        f"开始：{p.start_at or '-'}\n"
        f"结束：{p.end_at or '-'}\n"
        f"最近发送：{last_time}\n\n"
        f"图片：{'✅' if p.image else '❌'}\n"
        f"按钮：{'✅' if p.button else '❌'}\n"
        f"文本：{'✅' if p.text else '❌'}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text=f"状态：{'✅ 开启' if p.active else '❌ 关闭'}",
                callback_data=f"auto_toggle_{p.id}"
            )],
            [InlineKeyboardButton(
                text=f"📌 置顶：{'✅ 是' if p.pin else '❌ 否'}",
                callback_data=f"auto_pin_{p.id}"
            )],
            [InlineKeyboardButton(text="📝 修改文本", callback_data=f"auto_text_{p.id}")],
            [InlineKeyboardButton(text="📷 修改媒体", callback_data=f"auto_img_{p.id}")],
            [InlineKeyboardButton(text="🔤 修改按钮", callback_data=f"auto_btn_{p.id}")],
            [InlineKeyboardButton(text="👀 预览", callback_data=f"auto_pre_{p.id}")],
            [InlineKeyboardButton(text="⏩ 间隔时间", callback_data=f"auto_int_{p.id}")],
            [InlineKeyboardButton(text="📅 开始时间", callback_data=f"auto_start_{p.id}")],
            [InlineKeyboardButton(text="📅 结束时间", callback_data=f"auto_end_{p.id}")],
            [InlineKeyboardButton(text="⬅️ 返回", callback_data="auto_menu")],
        ])
    )

# ======================
# TRACK GROUP MEMBERSHIP
# ======================
@dp.my_chat_member()
async def track_bot_membership(event: types.ChatMemberUpdated):
    chat = event.chat
    new_status = event.new_chat_member.status

    if chat.type not in ("group", "supergroup"):
        return

    chat_id = str(chat.id)
    now_ts = int(time.time())

    if new_status in ("member", "administrator"):
        async with SessionLocal() as db:
            row = (await db.execute(
                select(BotGroup).where(BotGroup.chat_id == chat_id)
            )).scalars().first()

            if not row:
                row = BotGroup(
                    chat_id=chat_id,
                    title=chat.title or "",
                    type=chat.type,
                    is_admin=1 if new_status == "administrator" else 0,
                    updated_at=now_ts
                )
                db.add(row)
            else:
                row.title = chat.title or row.title
                row.type = chat.type
                row.is_admin = 1 if new_status == "administrator" else 0
                row.updated_at = now_ts

            await db.commit()
            logging.info("Saved group %s (%s)", chat_id, chat.title)

    if new_status in ("left", "kicked"):
        async with SessionLocal() as db:
            await db.execute(delete(BotGroup).where(BotGroup.chat_id == chat_id))
            await db.commit()
            logging.info("Removed group %s", chat_id)

# ======================
# ADMIN COMMANDS
# ======================
@dp.message(F.text.startswith("/admins"))
async def cmd_admins(m: types.Message):
    if not m.from_user or not is_owner(m.from_user.id):
        return

    rows = await get_all_admin_users()
    text = ["<b>ADMIN STATUS</b>"]
    text.append(f"Owner: <code>{OWNER_ID}</code>")
    text.append("ENV ADMIN_IDS: " + (", ".join(f"<code>{x}</code>" for x in sorted(ADMIN_IDS)) if ADMIN_IDS else "<i>空</i>"))

    if rows:
        text.append("\nDB Admins:")
        for r in rows:
            note = f" — {r.note}" if r.note else ""
            text.append(f"• <code>{r.user_id}</code>{note}")
    else:
        text.append("\nDB Admins: <i>空</i>")

    await m.answer("\n".join(text))


@dp.message(F.text.startswith("/adminadd"))
async def cmd_adminadd(m: types.Message):
    if not m.from_user or not is_owner(m.from_user.id):
        return

    parts = (m.text or "").split(maxsplit=2)
    if len(parts) < 2:
        return await m.answer("Cú pháp: <code>/adminadd user_id [ghi_chú]</code>")

    try:
        uid = int(parts[1])
    except ValueError:
        return await m.answer("user_id không hợp lệ.")

    note = parts[2] if len(parts) >= 3 else ""
    await add_admin_db(uid, note)
    await m.answer(f"✅ Đã thêm admin: <code>{uid}</code>")


@dp.message(F.text.startswith("/admindel"))
async def cmd_admindel(m: types.Message):
    if not m.from_user or not is_owner(m.from_user.id):
        return

    parts = (m.text or "").split(maxsplit=1)
    if len(parts) < 2:
        return await m.answer("Cú pháp: <code>/admindel user_id</code>")

    try:
        uid = int(parts[1])
    except ValueError:
        return await m.answer("user_id không hợp lệ.")

    if uid == OWNER_ID:
        return await m.answer("Không thể xóa Owner.")

    await remove_admin_db(uid)
    await m.answer(f"✅ Đã xóa admin: <code>{uid}</code>")

# ======================
# START / HOME
# ======================
@dp.message(F.text == "/start")
async def start(m: types.Message):
    if not m.from_user:
        return

    uid = m.from_user.id

    if not is_allowed_user(uid):
        if m.chat.type == "private":
            await m.answer(STRANGER_START_TEXT, reply_markup=stranger_start_kb())
        return

    reset(uid)

    if m.chat.type == "private":
        await send_or_edit_private_home(uid, m.chat.id)
        return

    groups = await get_admin_groups()
    if groups:
        await m.answer("👥 请选择要管理的群组：", reply_markup=group_select_kb(groups))
        return

    await m.answer("🏠 首页", reply_markup=start_menu_kb(uid))


@dp.message(F.text == "/cancel")
async def cancel(m: types.Message):
    if not m.from_user:
        return
    reset(m.from_user.id)
    await m.answer("已取消操作。", reply_markup=start_menu_kb(m.from_user.id))


@dp.callback_query(F.data == "back_start")
async def back_start(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    await safe_edit(c.message, "🏠 首页", reply_markup=start_menu_kb(c.from_user.id))

# ======================
# MAIN MENUS
# ======================
@dp.callback_query(F.data == "admin_menu")
async def admin_menu_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    await safe_edit(c.message, "👑 管理员设置", reply_markup=admin_menu_kb())


@dp.callback_query(F.data == "admin_manage_menu")
async def admin_manage_menu(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    await safe_edit(c.message, "👮 管理员管理", reply_markup=admin_manage_menu_kb())


@dp.callback_query(F.data == "admin_list")
async def admin_list_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    await show_admin_list(c.message)


@dp.callback_query(F.data == "admin_add")
async def admin_add_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    if not c.from_user or not is_owner(c.from_user.id):
        return
    await ack(c)
    uid = c.from_user.id
    user_state[uid] = "admin_add_wait"
    temp[uid] = {}
    await c.message.answer("Gửi user_id của admin mới, hoặc dùng lệnh:\n<code>/adminadd user_id [ghi chú]</code>")


@dp.callback_query(F.data == "admin_del")
async def admin_del_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    if not c.from_user or not is_owner(c.from_user.id):
        return
    await ack(c)
    uid = c.from_user.id
    user_state[uid] = "admin_del_wait"
    temp[uid] = {}
    await c.message.answer("Gửi user_id cần xóa, hoặc dùng lệnh:\n<code>/admindel user_id</code>")


@dp.callback_query(F.data == "group_menu")
async def group_menu_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    await safe_edit(c.message, "👥 群组管理", reply_markup=group_menu_kb())


@dp.callback_query(F.data == "lang_menu")
async def lang_menu_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    await safe_edit(c.message, "🌐 语言设置", reply_markup=lang_menu_kb())


@dp.callback_query(F.data == "group_list")
async def group_list_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return

    await ack(c)
    groups = await get_all_groups()

    kb = []
    for g in groups:
        kb.append([
            InlineKeyboardButton(
                text=f"{g.title or g.chat_id} ({'管理员' if g.is_admin else '成员'})",
                callback_data=f"pick_group_{g.chat_id}"
            )
        ])
    kb.append([InlineKeyboardButton(text="⬅️ 返回", callback_data="group_menu")])

    await safe_edit(
        c.message,
        "📋 群组列表" if groups else "暂无已保存的群组。",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
    )


@dp.callback_query(F.data == "group_pick")
async def group_pick_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return

    await ack(c)
    groups = await get_all_groups()
    if not groups:
        return await c.message.answer("暂无群组。")
    await safe_edit(c.message, "➕ 选择群组：", reply_markup=group_select_kb(groups))


@dp.callback_query(F.data.startswith("pick_group_"))
async def pick_group_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return

    await ack(c)
    chat_id = c.data.replace("pick_group_", "")
    uid = c.from_user.id
    selected_group[uid] = chat_id

    async with SessionLocal() as db:
        g = (await db.execute(
            select(BotGroup).where(BotGroup.chat_id == chat_id)
        )).scalars().first()

    title = g.title if g and g.title else chat_id

    await safe_edit(
        c.message,
        f"✅ 已选择群组：\n{title}\n\n🏠 首页",
        reply_markup=start_menu_kb(uid)
    )


@dp.callback_query(F.data == "lang_vi")
async def lang_vi_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    selected_lang[c.from_user.id] = "vi"
    await safe_edit(c.message, "✅ 已选择：越南语", reply_markup=start_menu_kb(c.from_user.id))


@dp.callback_query(F.data == "lang_zh")
async def lang_zh_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    selected_lang[c.from_user.id] = "zh"
    await safe_edit(c.message, "✅ 已选择：中文", reply_markup=start_menu_kb(c.from_user.id))

# ======================
# KEYWORD MENU
# ======================
@dp.callback_query(F.data == "kw_menu")
async def kw_menu_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    await safe_edit(c.message, "📌 关键词", reply_markup=kw_menu_kb())


@dp.callback_query(F.data == "kw_add")
async def kw_add_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    uid = c.from_user.id
    user_state[uid] = "kw_add_key"
    temp[uid] = {}
    await c.message.answer(
        "请输入关键词。\n"
        "如果要一次创建多个关键词，请每行一个关键词。"
    )


@dp.callback_query(F.data == "kw_list")
async def kw_list_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    await show_kw_list(c.message)


@dp.callback_query(F.data.startswith("kw_view_"))
async def kw_view_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    await show_kw_view(c.message, int(c.data.split("_")[-1]))


@dp.callback_query(F.data.startswith("kw_toggle_"))
async def kw_toggle_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    kid = int(c.data.split("_")[-1])
    async with SessionLocal() as db:
        k = await db.get(Keyword, kid)
        if not k:
            return await c.message.answer("未找到关键词。")
        k.active = 0 if k.active else 1
        await db.commit()
    await load_cache()
    await show_kw_view(c.message, kid)


@dp.callback_query(F.data.startswith("kw_mode_"))
async def kw_mode_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    kid = int(c.data.split("_")[-1])
    async with SessionLocal() as db:
        k = await db.get(Keyword, kid)
        if not k:
            return await c.message.answer("未找到关键词。")
        k.mode = "contains" if k.mode == "exact" else "exact"
        await db.commit()
    await load_cache()
    await show_kw_view(c.message, kid)


@dp.callback_query(F.data.startswith("kw_key_"))
async def kw_key_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    kid = int(c.data.split("_")[-1])
    uid = c.from_user.id
    user_state[uid] = "kw_edit_key"
    temp[uid] = {"id": kid}
    await c.message.answer("请输入新的关键词：")


@dp.callback_query(F.data.startswith("kw_text_"))
async def kw_text_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    kid = int(c.data.split("_")[-1])
    uid = c.from_user.id
    user_state[uid] = "kw_edit_text"
    temp[uid] = {"id": kid}
    await c.message.answer("请输入回复文案：")


@dp.callback_query(F.data.startswith("kw_img_"))
async def kw_img_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    kid = int(c.data.split("_")[-1])
    uid = c.from_user.id
    user_state[uid] = "kw_edit_image"
    temp[uid] = {"id": kid}
    await c.message.answer("请发送图片，或输入图片 URL / file_id：")


@dp.callback_query(F.data.startswith("kw_btn_"))
async def kw_btn_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    kid = int(c.data.split("_")[-1])
    uid = c.from_user.id
    user_state[uid] = "kw_edit_button"
    temp[uid] = {"id": kid}
    await c.message.answer(
        "请输入按钮格式：\n"
        "Google - https://google.com && YouTube - https://youtube.com\n"
        "每行是一行按钮。"
    )


@dp.callback_query(F.data.startswith("kw_pre_"))
async def kw_pre_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    kid = int(c.data.split("_")[-1])
    async with SessionLocal() as db:
        k = await db.get(Keyword, kid)
    if not k:
        return await c.message.answer("关键词不存在。")
    await send_preview(chat_id=c.from_user.id, text=k.text, image=k.image, button=k.button)


@dp.callback_query(F.data.startswith("kw_del_"))
async def kw_del_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c, "已删除")
    kid = int(c.data.split("_")[-1])
    async with SessionLocal() as db:
        await db.execute(delete(Keyword).where(Keyword.id == kid))
        await db.commit()
    await load_cache()
    await show_kw_list(c.message)

# ======================
# WELCOME MENU
# ======================
@dp.callback_query(F.data == "wl_menu")
async def wl_menu_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    await safe_edit(c.message, "👋 群组欢迎", reply_markup=wl_menu_kb())


@dp.callback_query(F.data == "wl_add")
async def wl_add_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    uid = c.from_user.id
    user_state[uid] = "wl_add_chat"
    temp[uid] = {}
    await c.message.answer("请输入群组 chat_id：")


@dp.callback_query(F.data == "wl_list")
async def wl_list_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    await show_wl_list(c.message)


@dp.callback_query(F.data.startswith("wl_view_"))
async def wl_view_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    await show_wl_view(c.message, int(c.data.split("_")[-1]))


@dp.callback_query(F.data.startswith("wl_toggle_"))
async def wl_toggle_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    wid = int(c.data.split("_")[-1])
    async with SessionLocal() as db:
        w = await db.get(WelcomeSetting, wid)
        if not w:
            return await c.message.answer("未找到。")
        w.active = 0 if w.active else 1
        await db.commit()
    await load_cache()
    await show_wl_view(c.message, wid)


@dp.callback_query(F.data.startswith("wl_text_"))
async def wl_text_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    wid = int(c.data.split("_")[-1])
    uid = c.from_user.id
    user_state[uid] = "wl_edit_text"
    temp[uid] = {"id": wid}
    await c.message.answer("请输入欢迎文案：")


@dp.callback_query(F.data.startswith("wl_img_"))
async def wl_img_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    wid = int(c.data.split("_")[-1])
    uid = c.from_user.id
    user_state[uid] = "wl_edit_image"
    temp[uid] = {"id": wid}
    await c.message.answer("请发送图片，或输入图片 URL / file_id：")


@dp.callback_query(F.data.startswith("wl_btn_"))
async def wl_btn_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    wid = int(c.data.split("_")[-1])
    uid = c.from_user.id
    user_state[uid] = "wl_edit_button"
    temp[uid] = {"id": wid}
    await c.message.answer(
        "请输入按钮格式：\n"
        "Google - https://google.com && YouTube - https://youtube.com\n"
        "每行是一行按钮。"
    )


@dp.callback_query(F.data.startswith("wl_delmin_"))
async def wl_delmin_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    wid = int(c.data.split("_")[-1])
    uid = c.from_user.id
    user_state[uid] = "wl_edit_delete_after"
    temp[uid] = {"id": wid}
    await c.message.answer("请输入删除时间（分钟），0 表示不删除：")


@dp.callback_query(F.data.startswith("wl_pre_"))
async def wl_pre_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    wid = int(c.data.split("_")[-1])
    async with SessionLocal() as db:
        w = await db.get(WelcomeSetting, wid)
    if not w:
        return await c.message.answer("未找到。")
    await send_preview(chat_id=c.from_user.id, text=w.text, image=w.image, button=w.button)


@dp.callback_query(F.data.startswith("wl_del_"))
async def wl_del_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c, "已删除")
    wid = int(c.data.split("_")[-1])
    async with SessionLocal() as db:
        await db.execute(delete(WelcomeSetting).where(WelcomeSetting.id == wid))
        await db.commit()
    await load_cache()
    await show_wl_list(c.message)


@dp.callback_query(F.data.startswith("wl_pin_"))
async def wl_pin_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    wid = int(c.data.split("_")[-1])
    async with SessionLocal() as db:
        w = await db.get(WelcomeSetting, wid)
        if not w:
            return await c.message.answer("未找到。")
        w.pin = 0 if w.pin else 1
        await db.commit()
    await load_cache()
    await show_wl_view(c.message, wid)

# ======================
# AUTO MENU
# ======================
@dp.callback_query(F.data == "auto_menu")
async def auto_menu_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    await safe_edit(c.message, "📅 定时发送", reply_markup=auto_menu_kb())


@dp.callback_query(F.data == "auto_add")
async def auto_add_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    uid = c.from_user.id
    user_state[uid] = "auto_add_chat"
    temp[uid] = {}
    await c.message.answer("请输入群组 chat_id：")


@dp.callback_query(F.data == "auto_list")
async def auto_list_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    await show_auto_list(c.message)


@dp.callback_query(F.data.startswith("auto_view_"))
async def auto_view_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    await show_auto_view(c.message, int(c.data.split("_")[-1]))


@dp.callback_query(F.data.startswith("auto_toggle_"))
async def auto_toggle_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    pid = int(c.data.split("_")[-1])
    async with SessionLocal() as db:
        p = await db.get(AutoPost, pid)
        if not p:
            return await c.message.answer("未找到定时发送。")
        p.active = 0 if p.active else 1
        await db.commit()
    await load_cache()
    await show_auto_view(c.message, pid)


@dp.callback_query(F.data.startswith("auto_pin_"))
async def auto_pin_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    pid = int(c.data.split("_")[-1])
    async with SessionLocal() as db:
        p = await db.get(AutoPost, pid)
        if not p:
            return await c.message.answer("未找到定时发送。")
        p.pin = 0 if p.pin else 1
        await db.commit()
    await load_cache()
    await show_auto_view(c.message, pid)


@dp.callback_query(F.data.startswith("auto_text_"))
async def auto_text_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    pid = int(c.data.split("_")[-1])
    uid = c.from_user.id
    user_state[uid] = "auto_edit_text"
    temp[uid] = {"id": pid}
    await c.message.answer("请输入文案内容：")


@dp.callback_query(F.data.startswith("auto_img_"))
async def auto_img_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    pid = int(c.data.split("_")[-1])
    uid = c.from_user.id
    user_state[uid] = "auto_edit_image"
    temp[uid] = {"id": pid}
    await c.message.answer("请发送图片，或输入图片 URL / file_id：")


@dp.callback_query(F.data.startswith("auto_btn_"))
async def auto_btn_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    pid = int(c.data.split("_")[-1])
    uid = c.from_user.id
    user_state[uid] = "auto_edit_button"
    temp[uid] = {"id": pid}
    await c.message.answer(
        "请输入按钮格式：\n"
        "Google - https://google.com && YouTube - https://youtube.com\n"
        "每行是一行按钮。"
    )


@dp.callback_query(F.data.startswith("auto_chat_"))
async def auto_chat_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    pid = int(c.data.split("_")[-1])
    uid = c.from_user.id
    user_state[uid] = "auto_edit_chat"
    temp[uid] = {"id": pid}
    await c.message.answer("请输入新的 chat_id：")


@dp.callback_query(F.data.startswith("auto_int_"))
async def auto_int_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    pid = int(c.data.split("_")[-1])
    uid = c.from_user.id
    user_state[uid] = "auto_edit_interval"
    temp[uid] = {"id": pid}
    await c.message.answer("请输入发送间隔（分钟）：")


@dp.callback_query(F.data.startswith("auto_start_"))
async def auto_start_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    pid = int(c.data.split("_")[-1])
    uid = c.from_user.id
    user_state[uid] = "auto_edit_start"
    temp[uid] = {"id": pid}
    await c.message.answer("请输入开始时间：YYYY-MM-DD HH:MM")


@dp.callback_query(F.data.startswith("auto_end_"))
async def auto_end_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    pid = int(c.data.split("_")[-1])
    uid = c.from_user.id
    user_state[uid] = "auto_edit_end"
    temp[uid] = {"id": pid}
    await c.message.answer("请输入结束时间：YYYY-MM-DD HH:MM")


@dp.callback_query(F.data.startswith("auto_pre_"))
async def auto_pre_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c)
    pid = int(c.data.split("_")[-1])
    async with SessionLocal() as db:
        p = await db.get(AutoPost, pid)
    if not p:
        return await c.message.answer("未找到。")
    await send_preview(chat_id=c.from_user.id, text=p.text, image=p.image, button=p.button)


@dp.callback_query(F.data.startswith("auto_del_"))
async def auto_del_cb(c: types.CallbackQuery):
    if not await allowed_or_ignore(c):
        return
    await ack(c, "已删除")
    pid = int(c.data.split("_")[-1])
    async with SessionLocal() as db:
        await db.execute(delete(AutoPost).where(AutoPost.id == pid))
        await db.commit()
    await load_cache()
    await show_auto_list(c.message)

# ======================
# STATE HANDLER
# ======================
@dp.message()
async def all_messages(m: types.Message):
    if not m.from_user:
        return

    uid = m.from_user.id
    state = user_state.get(uid)

    if m.text and m.text.startswith("/"):
        return

    if m.chat.type == "private" and not is_allowed_user(uid):
        return

    logging.info("[MESSAGE] chat=%s user=%s text=%r state=%s", m.chat.id, uid, m.text, state)

    # ======================
    # ADMIN FLOW
    # ======================
    if state == "admin_add_wait":
        if not is_owner(uid):
            reset(uid)
            return
        raw = message_text(m)
        try:
            target = int(raw)
        except ValueError:
            return await m.answer("user_id không hợp lệ.")
        await add_admin_db(target, "")
        reset(uid)
        return await m.answer(f"✅ Đã thêm admin: <code>{target}</code>", reply_markup=start_menu_kb(uid))

    if state == "admin_del_wait":
        if not is_owner(uid):
            reset(uid)
            return
        raw = message_text(m)
        try:
            target = int(raw)
        except ValueError:
            return await m.answer("user_id không hợp lệ.")
        if target == OWNER_ID:
            return await m.answer("Không thể xóa Owner.")
        await remove_admin_db(target)
        reset(uid)
        return await m.answer(f"✅ Đã xóa admin: <code>{target}</code>", reply_markup=start_menu_kb(uid))

    # ======================
    # KEYWORD FLOW
    # ======================
    if state == "kw_add_key":
        raw = message_text(m)
        if not raw:
            return await m.answer("关键词不能为空。")

        keys = [line.strip() for line in raw.splitlines() if line.strip()]
        if not keys:
            return await m.answer("关键词不能为空。")

        added = 0
        existed = 0

        async with SessionLocal() as db:
            for key in keys:
                exists = (await db.execute(
                    select(Keyword).where(Keyword.key == key)
                )).scalars().first()
                if exists:
                    existed += 1
                    continue
                db.add(Keyword(key=key, mode="exact", active=1, text="", image="", button=""))
                added += 1
            await db.commit()

        reset(uid)
        await load_cache()
        return await m.answer(
            f"已添加 {added} 个关键词。\n已跳过 {existed} 个重复关键词。",
            reply_markup=start_menu_kb(uid)
        )

    if state == "kw_edit_key":
        kid = temp[uid]["id"]
        key = message_text(m)
        if not key:
            return await m.answer("关键词不能为空。")

        async with SessionLocal() as db:
            exists = (await db.execute(
                select(Keyword).where(Keyword.key == key, Keyword.id != kid)
            )).scalars().first()
            if exists:
                return await m.answer("关键词已存在，请输入其他关键词。")

            k = await db.get(Keyword, kid)
            if k:
                k.key = key
                await db.commit()

        reset(uid)
        await load_cache()
        return await m.answer("已更新关键词。", reply_markup=start_menu_kb(uid))

    if state == "kw_edit_text":
        kid = temp[uid]["id"]
        async with SessionLocal() as db:
            k = await db.get(Keyword, kid)
            if k:
                k.text = message_text(m)
                await db.commit()
        reset(uid)
        await load_cache()
        return await m.answer("已更新文案。", reply_markup=start_menu_kb(uid))

    if state == "kw_edit_image":
        kid = temp[uid]["id"]
        image = extract_image_from_message(m)
        if not image:
            return await m.answer("请发送图片或输入图片 URL / file_id。")
        async with SessionLocal() as db:
            k = await db.get(Keyword, kid)
            if k:
                k.image = image
                await db.commit()
        reset(uid)
        await load_cache()
        return await m.answer("已更新图片。", reply_markup=start_menu_kb(uid))

    if state == "kw_edit_button":
        kid = temp[uid]["id"]
        async with SessionLocal() as db:
            k = await db.get(Keyword, kid)
            if k:
                k.button = message_text(m)
                await db.commit()
        reset(uid)
        await load_cache()
        return await m.answer("已更新按钮。", reply_markup=start_menu_kb(uid))

    # ======================
    # WELCOME FLOW
    # ======================
    if state == "wl_add_chat":
        chat_id = message_text(m)
        if not chat_id:
            return await m.answer("chat_id 不能为空。")
        async with SessionLocal() as db:
            exists = (await db.execute(
                select(WelcomeSetting).where(WelcomeSetting.chat_id == chat_id)
            )).scalars().first()
            if exists:
                return await m.answer("该 chat_id 已存在。")
            db.add(WelcomeSetting(chat_id=chat_id))
            await db.commit()
        reset(uid)
        await load_cache()
        return await m.answer("已创建欢迎配置。", reply_markup=start_menu_kb(uid))

    if state == "wl_edit_text":
        wid = temp[uid]["id"]
        async with SessionLocal() as db:
            w = await db.get(WelcomeSetting, wid)
            if not w:
                return await m.answer("欢迎配置不存在。")
            w.text = message_text(m)
            await db.commit()
        reset(uid)
        await load_cache()
        await m.answer("✅ Welcome text 已更新。")
        return await show_wl_view(m, wid)

    if state == "wl_edit_image":
        wid = temp[uid]["id"]
        image = extract_image_from_message(m)
        if not image:
            return await m.answer("请发送图片或输入图片 URL / file_id。")
        async with SessionLocal() as db:
            w = await db.get(WelcomeSetting, wid)
            if w:
                w.image = image
                await db.commit()
        reset(uid)
        await load_cache()
        await m.answer("✅ 图片已更新。")
        return await show_wl_view(m, wid)

    if state == "wl_edit_button":
        wid = temp[uid]["id"]
        async with SessionLocal() as db:
            w = await db.get(WelcomeSetting, wid)
            if w:
                w.button = message_text(m)
                await db.commit()
        reset(uid)
        await load_cache()
        await m.answer("✅ 按钮已更新。")
        return await show_wl_view(m, wid)

    if state == "wl_edit_delete_after":
        wid = temp[uid]["id"]
        try:
            minutes = int(message_text(m))
            if minutes < 0:
                raise ValueError
        except ValueError:
            return await m.answer("请输入大于等于 0 的整数。")
        async with SessionLocal() as db:
            w = await db.get(WelcomeSetting, wid)
            if w:
                w.delete_after = minutes
                await db.commit()
        reset(uid)
        await load_cache()
        await m.answer("✅ 删除时间已更新。")
        return await show_wl_view(m, wid)

    # ======================
    # AUTO FLOW
    # ======================
    if state == "auto_add_chat":
        chat_id = message_text(m)
        if not chat_id:
            return await m.answer("chat_id 不能为空。")
        async with SessionLocal() as db:
            db.add(AutoPost(chat_id=chat_id))
            await db.commit()
        reset(uid)
        await load_cache()
        return await m.answer("已创建定时发送。", reply_markup=start_menu_kb(uid))

    if state == "auto_edit_text":
        pid = temp[uid]["id"]
        async with SessionLocal() as db:
            p = await db.get(AutoPost, pid)
            if p:
                p.text = message_text(m)
                await db.commit()
        reset(uid)
        await load_cache()
        await m.answer("✅ 文案已更新。")
        return await show_auto_view(m, pid)

    if state == "auto_edit_image":
        pid = temp[uid]["id"]
        image = extract_image_from_message(m)
        if not image:
            return await m.answer("请发送图片或输入图片 URL / file_id。")
        async with SessionLocal() as db:
            p = await db.get(AutoPost, pid)
            if p:
                p.image = image
                await db.commit()
        reset(uid)
        await load_cache()
        await m.answer("✅ 图片已更新。")
        return await show_auto_view(m, pid)

    if state == "auto_edit_button":
        pid = temp[uid]["id"]
        async with SessionLocal() as db:
            p = await db.get(AutoPost, pid)
            if p:
                p.button = message_text(m)
                await db.commit()
        reset(uid)
        await load_cache()
        await m.answer("✅ 按钮已更新。")
        return await show_auto_view(m, pid)

    if state == "auto_edit_chat":
        pid = temp[uid]["id"]
        chat_id = message_text(m)
        if not chat_id:
            return await m.answer("chat_id 不能为空。")
        async with SessionLocal() as db:
            p = await db.get(AutoPost, pid)
            if p:
                p.chat_id = chat_id
                await db.commit()
        reset(uid)
        await load_cache()
        await m.answer("✅ 群组已更新。")
        return await show_auto_view(m, pid)

    if state == "auto_edit_interval":
        pid = temp[uid]["id"]
        try:
            interval = int(message_text(m))
            if interval <= 0:
                raise ValueError
        except ValueError:
            return await m.answer("间隔必须是正整数。")
        async with SessionLocal() as db:
            p = await db.get(AutoPost, pid)
            if p:
                p.interval = interval
                await db.commit()
        reset(uid)
        await load_cache()
        await m.answer("✅ 间隔已更新。")
        return await show_auto_view(m, pid)

    if state == "auto_edit_start":
        pid = temp[uid]["id"]
        if not parse_dt(message_text(m)):
            return await m.answer("格式错误，请使用：YYYY-MM-DD HH:MM")
        async with SessionLocal() as db:
            p = await db.get(AutoPost, pid)
            if p:
                p.start_at = message_text(m)
                await db.commit()
        reset(uid)
        await load_cache()
        await m.answer("✅ 开始时间已更新。")
        return await show_auto_view(m, pid)

    if state == "auto_edit_end":
        pid = temp[uid]["id"]
        if not parse_dt(message_text(m)):
            return await m.answer("格式错误，请使用：YYYY-MM-DD HH:MM")
        async with SessionLocal() as db:
            p = await db.get(AutoPost, pid)
            if p:
                p.end_at = message_text(m)
                await db.commit()
        reset(uid)
        await load_cache()
        await m.answer("✅ 结束时间已更新。")
        return await show_auto_view(m, pid)

    # ======================
    # KEYWORD AUTO REPLY
    # ======================
    if m.chat.type not in ("group", "supergroup"):
        return

    text_ = message_text(m)
    if not text_ or text_.startswith("/"):
        return

    cleanup_last_trigger()

    lower_text = text_.lower()
    matched = None

    for k in keyword_cache:
        key = (k["key"] or "").strip().lower()
        if not key:
            continue

        if k["mode"] == "exact" and lower_text == key:
            matched = k
            break

        if k["mode"] == "contains" and key in lower_text:
            matched = k
            break

    if matched:
        spam_key = (m.chat.id, matched["key"])
        now = time.time()
        if spam_key in last_trigger and now - last_trigger[spam_key] < SPAM_WINDOW:
            return

        last_trigger[spam_key] = now
        await send_preview(
            chat_id=m.chat.id,
            text=matched["text"],
            image=matched["image"],
            button=matched["button"]
        )

# ======================
# WELCOME NEW MEMBER
# ======================
@dp.message(F.new_chat_members)
async def welcome_new_member(m: types.Message):
    if not m.chat:
        return

    chat_id = str(m.chat.id)
    w = welcome_cache.get(chat_id)
    if not w:
        return

    try:
        member = m.new_chat_members[0] if m.new_chat_members else None
        display_name = member.full_name if member else "朋友"
        welcome_text = w["text"] or "欢迎 [name] 加入群组！"

        welcome_text = (
            welcome_text
            .replace("[name]", display_name)
            .replace("[group]", m.chat.title or "群组")
        )

        msg = await send_preview(
            chat_id=m.chat.id,
            text=welcome_text,
            image=w["image"],
            button=w["button"]
        )

        if w["pin"]:
            with contextlib.suppress(Exception):
                await bot.pin_chat_message(
                    chat_id=m.chat.id,
                    message_id=msg.message_id,
                    disable_notification=True
                )

        if w["delete_after"] and w["delete_after"] > 0:
            async def later_delete():
                await asyncio.sleep(w["delete_after"] * 60)
                with contextlib.suppress(Exception):
                    await bot.delete_message(chat_id=m.chat.id, message_id=msg.message_id)

            asyncio.create_task(later_delete())

    except Exception as e:
        logging.exception("welcome lỗi: %s", e)

# ======================
# AUTO WORKER
# ======================
async def auto_worker():
    while True:
        try:
            now = int(time.time())

            for p in auto_cache:
                if not p["chat_id"]:
                    continue

                interval_sec = max(int(p["interval"] or 10), 1) * 60
                if now - int(p["last_sent_ts"] or 0) < interval_sec:
                    continue

                start_dt = parse_dt(p["start_at"]) if p["start_at"] else None
                end_dt = parse_dt(p["end_at"]) if p["end_at"] else None

                if start_dt and datetime.now() < start_dt:
                    continue
                if end_dt and datetime.now() > end_dt:
                    continue

                try:
                    msg = await send_preview(
                        chat_id=p["chat_id"],
                        text=p["text"],
                        image=p["image"],
                        button=p["button"]
                    )

                    async with SessionLocal() as db:
                        row = await db.get(AutoPost, p["id"])
                        if row:
                            row.last_sent_ts = now
                            await db.commit()

                    p["last_sent_ts"] = now

                    if p["pin"]:
                        with contextlib.suppress(Exception):
                            await bot.pin_chat_message(
                                chat_id=p["chat_id"],
                                message_id=msg.message_id
                            )

                except Exception as e:
                    logging.warning("auto post %s lỗi: %s", p["id"], e)

            cleanup_last_trigger()
        except Exception as e:
            logging.exception("auto_worker lỗi: %s", e)

        await asyncio.sleep(30)

# ======================
# STARTUP / SHUTDOWN
# ======================
async def on_startup():
    global worker_task
    await ensure_schema()
    await load_admin_cache()
    await load_cache()
    worker_task = asyncio.create_task(auto_worker())
    logging.info("READY")


async def on_shutdown():
    global worker_task
    if worker_task:
        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task

    with contextlib.suppress(Exception):
        await bot.session.close()


dp.startup.register(on_startup)
dp.shutdown.register(on_shutdown)

# ======================
# MAIN
# ======================
async def main():
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    asyncio.run(main())
