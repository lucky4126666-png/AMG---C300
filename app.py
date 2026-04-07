import os
import time
import asyncio
import logging
import contextlib

from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse

from aiogram import Bot, Dispatcher, types, F
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, Integer, String, Text, select, delete

# ======================
# BASIC
# ======================
logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN")
BASE_URL = os.getenv("BASE_URL")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
WEB_ADMIN_KEY = os.getenv("WEB_ADMIN_KEY", "")

WELCOME_BTN1_TEXT = os.getenv("WELCOME_BTN1_TEXT", "新币供应")
WELCOME_BTN1_URL = os.getenv("WELCOME_BTN1_URL", "https://t.me/YOUR_SUPPLY_LINK")
WELCOME_BTN2_TEXT = os.getenv("WELCOME_BTN2_TEXT", "新币公群")
WELCOME_BTN2_URL = os.getenv("WELCOME_BTN2_URL", "https://t.me/YOUR_GROUP_LINK")

INIT_BTN1_TEXT = os.getenv("INIT_BTN1_TEXT", "公群导航")
INIT_BTN1_URL = os.getenv("INIT_BTN1_URL", "https://t.me/YOUR_INIT_NAV_LINK")
INIT_BTN2_TEXT = os.getenv("INIT_BTN2_TEXT", "供应频道")
INIT_BTN2_URL = os.getenv("INIT_BTN2_URL", "https://t.me/YOUR_INIT_CHANNEL_LINK")

if not BOT_TOKEN:
    raise RuntimeError("Missing BOT_TOKEN")
if not BASE_URL:
    raise RuntimeError("Missing BASE_URL")

# ======================
# DB
# ======================
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL")

# Railway postgres -> asyncpg
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)
elif DATABASE_URL.startswith("postgresql://") and "+asyncpg" not in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

engine = create_async_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

# ======================
# MODELS
# ======================
class AdminUser(Base):
    __tablename__ = "admin_users"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, unique=True, index=True)
    note = Column(String, default="")
    created_at = Column(Integer, default=0)


class Keyword(Base):
    __tablename__ = "keywords"
    id = Column(Integer, primary_key=True)
    key = Column(String, unique=True)
    text = Column(Text, default="")
    image = Column(String, default="")
    button = Column(Text, default="")
    active = Column(Integer, default=1)


class WelcomeSetting(Base):
    __tablename__ = "welcome"
    id = Column(Integer, primary_key=True)
    chat_id = Column(String)
    text = Column(Text, default="")
    active = Column(Integer, default=1)


class AutoPost(Base):
    __tablename__ = "auto_post"
    id = Column(Integer, primary_key=True)
    chat_id = Column(String)
    text = Column(Text, default="")
    interval = Column(Integer, default=10)

    # dùng cột last_sent (theo log hint của bạn)
    last_sent = Column(Integer, default=0)

    # dùng cột active (theo log hint của bạn)
    active = Column(Integer, default=1)


# ======================
# BOT
# ======================
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ======================
# CACHE / PERMISSION
# ======================
admin_cache = set()
BOT_ID = None
risk_notified_chats = set()
init_sent_chats = set()

# ======================
# BACKGROUND WORKER (graceful stop)
# ======================
auto_worker_task: asyncio.Task | None = None
stop_event = asyncio.Event()

# ======================
# helpers
# ======================
def is_allowed(user_id: int):
    return user_id == OWNER_ID or user_id in admin_cache


async def load_admin():
    global admin_cache
    async with SessionLocal() as db:
        rows = (await db.execute(select(AdminUser))).scalars().all()
    admin_cache = {r.user_id for r in rows}


async def send_text(chat_id, text, reply_markup=None):
    return await bot.send_message(chat_id, text, reply_markup=reply_markup)


def parse_inline_buttons(button_str: str):
    if not button_str:
        return None

    lines = [ln.strip() for ln in button_str.splitlines() if ln.strip()]
    if not lines:
        return None

    keyboard_rows = []
    for ln in lines:
        parts = [p.strip() for p in ln.split("&&") if p.strip()]
        row_buttons = []
        for p in parts:
            if "-" not in p:
                continue
            text_part, url_part = p.split("-", 1)
            t = text_part.strip()
            u = url_part.strip()
            if not t or not u:
                continue
            row_buttons.append(InlineKeyboardButton(text=t, url=u))
        if row_buttons:
            keyboard_rows.append(row_buttons)

    if not keyboard_rows:
        return None
    return InlineKeyboardMarkup(inline_keyboard=keyboard_rows)


async def send_keyword_reply(chat_id: str, k: Keyword):
    reply_markup = parse_inline_buttons(k.button)
    img = (k.image or "").strip()
    caption = (k.text or "").strip()

    if img:
        return await bot.send_photo(
            chat_id,
            photo=img,
            caption=caption or None,
            reply_markup=reply_markup
        )
    return await bot.send_message(chat_id, caption or "", reply_markup=reply_markup)


# ======================
# KEYWORD AUTO REPLY
# ======================
@dp.message()
async def auto_reply(m: types.Message):
    if not m.text:
        return

    text = m.text or ""
    chat_id = m.chat.id

    async with SessionLocal() as db:
        kws = (await db.execute(select(Keyword).where(Keyword.active == 1))).scalars().all()

    for k in kws:
        if (k.key or "").lower() in text.lower():
            try:
                await send_keyword_reply(str(chat_id), k)
            except Exception:
                logging.exception("keyword send error")
            break


# ======================
# AUTO POST WORKER
# ======================
async def auto_worker():
    try:
        while not stop_event.is_set():
            now = int(time.time())

            async with SessionLocal() as db:
                posts = (await db.execute(select(AutoPost).where(AutoPost.active == 1))).scalars().all()

            for p in posts:
                last = p.last_sent or 0
                if now - last >= (p.interval * 60):
                    try:
                        await send_text(p.chat_id, p.text or "")

                        async with SessionLocal() as db2:
                            row = await db2.get(AutoPost, p.id)
                            if row:
                                row.last_sent = now
                                await db2.commit()
                    except Exception:
                        logging.exception("auto post error")

            # ngủ 10s nhưng vẫn thoát nhanh khi stop_event set
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=10)
            except asyncio.TimeoutError:
                pass

    except asyncio.CancelledError:
        # cho phép cancel ngay khi shutdown
        pass
    except Exception:
        logging.exception("auto_worker crashed")


# ======================
# WELCOME + BOT INIT
# ======================
def build_welcome_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=WELCOME_BTN1_TEXT, url=WELCOME_BTN1_URL),
            InlineKeyboardButton(text=WELCOME_BTN2_TEXT, url=WELCOME_BTN2_URL),
        ]
    ])


def build_init_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=INIT_BTN1_TEXT, url=INIT_BTN1_URL),
            InlineKeyboardButton(text=INIT_BTN2_TEXT, url=INIT_BTN2_URL),
        ]
    ])


async def group_has_bot_admin(chat_id: str) -> bool:
    required_ids = set(admin_cache)
    required_ids.add(OWNER_ID)

    try:
        admins = await bot.get_chat_administrators(chat_id)
    except Exception as e:
        logging.warning(f"Cannot get admins for chat {chat_id}: {e}")
        return True

    admin_ids = {a.user.id for a in admins if a.user}
    return len(admin_ids.intersection(required_ids)) > 0


@dp.chat_member()
async def on_user_join(event: types.ChatMemberUpdated):
    if event.new_chat_member.status != "member":
        return

    u = event.new_chat_member.user
    if not u:
        return

    # skip bot join
    if u.is_bot:
        return
    if BOT_ID is not None and u.id == BOT_ID:
        return

    chat_id = str(event.chat.id)

    async with SessionLocal() as db:
        w = (await db.execute(
            select(WelcomeSetting).where(
                WelcomeSetting.chat_id == chat_id,
                WelcomeSetting.active == 1
            )
        )).scalars().first()

    if not w or not (w.text or "").strip():
        return

    group_title = event.chat.title or ""
    name = (u.full_name or u.username or "VIP").strip()

    welcome_text = (w.text or "")
    welcome_text = welcome_text.replace("{name}", name).replace("{group}", group_title)

    try:
        await bot.send_message(chat_id, welcome_text, reply_markup=build_welcome_keyboard())
    except Exception:
        logging.exception("send welcome error")


@dp.my_chat_member()
async def on_bot_join(event: types.ChatMemberUpdated):
    if event.new_chat_member.status not in ("member", "administrator", "creator"):
        return

    chat_id = str(event.chat.id)

    if chat_id not in init_sent_chats:
        init_text = "组防骗助手为您服务,我正在进行相关初始化配置请稍后"
        try:
            await bot.send_message(chat_id, init_text, reply_markup=build_init_keyboard())
        except Exception:
            logging.exception("init send error")
        init_sent_chats.add(chat_id)

    if chat_id not in risk_notified_chats:
        ok = await group_has_bot_admin(chat_id)
        if not ok:
            risk_text = "风险提示，本群没有检测到新币管理员。有交易风险，请联系新币工作人员确认 @xbkf"
            try:
                await bot.send_message(chat_id, risk_text)
            except Exception:
                logging.exception("risk send error")
            risk_notified_chats.add(chat_id)


# ======================
# START CMD
# ======================
@dp.message(F.text == "/start")
async def start(m: types.Message):
    if not m.from_user:
        return
    if not is_allowed(m.from_user.id):
        return await m.answer("❌ Không có quyền")
    return await m.answer("🏠 Bot ready")


# ======================
# WEB ADMIN PANEL
# ======================
def check_key(key):
    return key == WEB_ADMIN_KEY


def render_page(admins, key):
    rows = ""
    for a in admins:
        rows += f"""
        <tr>
            <td>{a.user_id}</td>
            <td>{a.note}</td>
            <td>
                <form method="post" action="/admin/delete">
                    <input type="hidden" name="key" value="{key}">
                    <input type="hidden" name="user_id" value="{a.user_id}">
                    <button>Delete</button>
                </form>
            </td>
        </tr>
        """

    return f"""
    <html>
    <body>
    <h2>Admin Panel</h2>

    <form method="post" action="/admin/add">
        <input type="hidden" name="key" value="{key}">
        <input name="user_id" placeholder="User ID">
        <input name="note" placeholder="Note">
        <button>Add</button>
    </form>

    <table border="1">
        <tr><th>ID</th><th>Note</th><th>Action</th></tr>
        {rows}
    </table>

    </body>
    </html>
    """


app = FastAPI()


@app.get("/admin", response_class=HTMLResponse)
async def admin_page(key: str = ""):
    if not check_key(key):
        return HTMLResponse("403", status_code=403)

    async with SessionLocal() as db:
        admins = (await db.execute(select(AdminUser))).scalars().all()

    return render_page(admins, key)


@app.post("/admin/add", response_class=HTMLResponse)
async def add_admin(key: str = Form(""), user_id: int = Form(...), note: str = Form("")):
    if not check_key(key):
        return HTMLResponse("403", status_code=403)

    async with SessionLocal() as db:
        db.add(AdminUser(user_id=user_id, note=note, created_at=int(time.time())))
        await db.commit()

    await load_admin()
    return HTMLResponse("OK")


@app.post("/admin/delete", response_class=HTMLResponse)
async def del_admin(key: str = Form(""), user_id: int = Form(...)):
    if not check_key(key):
        return HTMLResponse("403", status_code=403)

    async with SessionLocal() as db:
        await db.execute(delete(AdminUser).where(AdminUser.user_id == user_id))
        await db.commit()

    await load_admin()
    return HTMLResponse("Deleted")


@app.get("/")
async def health():
    return {"ok": True}


# ======================
# WEBHOOK
# ======================
@app.post("/webhook")
async def webhook(req: Request):
    try:
        data = await req.json()
        update = types.Update.model_validate(data)
        await dp.feed_update(bot, update)
    except Exception:
        logging.exception("Webhook processing error")
    # luôn trả ok để Telegram không báo wrong response
    return {"ok": True}


# ======================
# STARTUP / SHUTDOWN
# ======================
@app.on_event("startup")
async def startup():
    global BOT_ID, auto_worker_task, stop_event

    BOT_ID = (await bot.get_me()).id

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    await load_admin()

    # reset stop_event
    stop_event.clear()

    if auto_worker_task is None or auto_worker_task.done():
        auto_worker_task = asyncio.create_task(auto_worker())

    await bot.set_webhook(f"{BASE_URL}/webhook")
    logging.info(f"Webhook set: {BASE_URL}/webhook")


@app.on_event("shutdown")
async def shutdown():
    global auto_worker_task

    # stop worker gracefully
    try:
        stop_event.set()
    except Exception:
        pass

    if auto_worker_task:
        try:
            await asyncio.wait_for(auto_worker_task, timeout=5)
        except asyncio.TimeoutError:
            auto_worker_task.cancel()
            with contextlib.suppress(Exception):
                await auto_worker_task
        except asyncio.CancelledError:
            pass
        except Exception:
            logging.exception("auto_worker shutdown error")

    # delete webhook best-effort
    with contextlib.suppress(Exception):
        await bot.delete_webhook()

    # close aiogram aiohttp session + connector
    with contextlib.suppress(Exception):
        await bot.session.close()

    # extra: close internal connector if exists
    with contextlib.suppress(Exception):
        connector = getattr(bot.session, "_connector", None)
        if connector is not None:
            connector.close()

    # dispose sqlalchemy engine (không bắt buộc nhưng sạch sẽ)
    with contextlib.suppress(Exception):
        await engine.dispose()
