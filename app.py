import os
import time
import asyncio
import json
from datetime import datetime

from fastapi import FastAPI, Request, Form, Header
from fastapi.responses import HTMLResponse

from aiogram import Bot, Dispatcher, types, F

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, Integer, String, Text, select, delete, Boolean, DateTime, desc

from openai import AsyncOpenAI

# ======================
# ENV
# ======================
BOT_TOKEN = os.getenv("BOT_TOKEN")
BASE_URL = os.getenv("BASE_URL")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
WEB_ADMIN_KEY = os.getenv("WEB_ADMIN_KEY", "")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not BOT_TOKEN:
    raise RuntimeError("Missing BOT_TOKEN")
if not BASE_URL:
    raise RuntimeError("Missing BASE_URL")
if not OPENAI_API_KEY:
    raise RuntimeError("Missing OPENAI_API_KEY")

# ======================
# DB (sqlite theo code bạn)
# ======================
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./bot.db")
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
    interval = Column(Integer, default=10)  # minutes
    last_sent_ts = Column(Integer, default=0)
    active = Column(Integer, default=1)


# ✅ USER MANAGEMENT
class TelegramUser(Base):
    __tablename__ = "telegram_users"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, unique=True, index=True)
    username = Column(String, nullable=True)
    first_name = Column(String, nullable=True)
    last_seen = Column(DateTime, default=datetime.utcnow)
    is_banned = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)


# ✅ CHAT HISTORY / AI MEMORY
class ChatMessage(Base):
    __tablename__ = "chat_messages"
    id = Column(Integer, primary_key=True)
    chat_id = Column(String, index=True)
    user_id = Column(Integer, index=True)
    role = Column(String)  # "user" | "assistant"
    content = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)


# ======================
# BOT
# ======================
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ======================
# CACHE
# ======================
admin_cache = set()
keyword_cache = []  # Keyword(active=1)

ai_client = AsyncOpenAI(api_key=OPENAI_API_KEY)


def is_allowed(user_id: int) -> bool:
    return user_id == OWNER_ID or user_id in admin_cache


# ======================
# DB HELPERS
# ======================
async def load_admin_and_cache():
    await load_admin()
    await load_keywords_cache()


async def load_admin():
    global admin_cache
    async with SessionLocal() as db:
        rows = (await db.execute(select(AdminUser))).scalars().all()
    admin_cache = {r.user_id for r in rows}


async def load_keywords_cache():
    global keyword_cache
    async with SessionLocal() as db:
        rows = (await db.execute(select(Keyword).where(Keyword.active == 1))).scalars().all()
    keyword_cache = rows


async def upsert_user(tg_user: types.User | None):
    if not tg_user:
        return
    async with SessionLocal() as db:
        row = (await db.execute(select(TelegramUser).where(TelegramUser.user_id == tg_user.id))).scalars().first()
        if not row:
            db.add(
                TelegramUser(
                    user_id=tg_user.id,
                    username=tg_user.username,
                    first_name=tg_user.first_name,
                    last_seen=datetime.utcnow(),
                    is_banned=False,
                )
            )
        else:
            row.username = tg_user.username
            row.first_name = tg_user.first_name
            row.last_seen = datetime.utcnow()
        await db.commit()


async def is_user_banned(user_id: int) -> bool:
    async with SessionLocal() as db:
        row = (await db.execute(select(TelegramUser).where(TelegramUser.user_id == user_id))).scalars().first()
        return bool(row and row.is_banned)


async def add_chat_message(chat_id: str, user_id: int, role: str, content: str):
    async with SessionLocal() as db:
        db.add(ChatMessage(chat_id=chat_id, user_id=user_id, role=role, content=content))
        await db.commit()


async def get_recent_chat_context(chat_id: str, limit: int = 12):
    async with SessionLocal() as db:
        rows = (await db.execute(
            select(ChatMessage)
            .where(ChatMessage.chat_id == chat_id)
            .order_by(desc(ChatMessage.created_at))
            .limit(limit)
        )).scalars().all()

    rows.reverse()
    return [{"role": r.role, "content": r.content} for r in rows]


# ======================
# HELPER
# ======================
async def send_text(chat_id, text):
    return await bot.send_message(chat_id, text)


def check_key(key: str):
    return key == WEB_ADMIN_KEY


def require_web_admin(x_admin_key: str | None):
    if not x_admin_key or not check_key(x_admin_key):
        raise PermissionError("Unauthorized")


# ======================
# KEYWORD MATCH (NO AI)
# ======================
def match_keyword(text: str):
    t = (text or "").lower()
    for k in keyword_cache:
        if (k.key or "").lower() in t:
            return k
    return None


# ======================
# AI TOOLS (restricted to admin/owner)
# ======================
async def tool_add_keyword(key: str, text: str):
    async with SessionLocal() as db:
        existing = (await db.execute(select(Keyword).where(Keyword.key == key))).scalars().first()
        if existing:
            existing.text = text
            existing.active = 1
        else:
            db.add(Keyword(key=key, text=text, active=1))
        await db.commit()

    await load_keywords_cache()
    return {"ok": True, "action": "add_keyword", "key": key}


async def tool_delete_keyword(keyword_id: int):
    async with SessionLocal() as db:
        row = await db.get(Keyword, keyword_id)
        if not row:
            return {"ok": False, "error": "Keyword not found"}
        await db.delete(row)
        await db.commit()

    await load_keywords_cache()
    return {"ok": True, "action": "delete_keyword", "id": keyword_id}


async def tool_set_welcome(chat_id: str, text: str):
    async with SessionLocal() as db:
        existing = (await db.execute(select(WelcomeSetting).where(WelcomeSetting.chat_id == chat_id))).scalars().first()
        if existing:
            existing.text = text
            existing.active = 1
        else:
            db.add(WelcomeSetting(chat_id=chat_id, text=text, active=1))
        await db.commit()
    return {"ok": True, "action": "set_welcome", "chat_id": chat_id}


async def tool_post_now(chat_id: str, text: str):
    await send_text(chat_id, text)
    return {"ok": True, "action": "post_now", "chat_id": chat_id}


async def tool_set_autopost(chat_id: str, text: str, interval_minutes: int):
    interval_minutes = max(1, int(interval_minutes))

    async with SessionLocal() as db:
        existing = (await db.execute(select(AutoPost).where(AutoPost.chat_id == chat_id))).scalars().first()
        now = int(time.time())
        if existing:
            existing.text = text
            existing.interval = interval_minutes
            existing.active = 1
            # reset để tránh spam ngay (bạn có thể đổi hành vi)
            existing.last_sent_ts = now
        else:
            db.add(AutoPost(chat_id=chat_id, text=text, interval=interval_minutes, last_sent_ts=now, active=1))
        await db.commit()

    return {"ok": True, "action": "set_autopost", "chat_id": chat_id, "interval_minutes": interval_minutes}


async def tool_ban_user(user_id: int, ban: bool):
    async with SessionLocal() as db:
        row = (await db.execute(select(TelegramUser).where(TelegramUser.user_id == user_id))).scalars().first()
        if not row:
            return {"ok": False, "error": "User not found"}
        row.is_banned = bool(ban)
        await db.commit()
    return {"ok": True, "action": "ban_user", "user_id": user_id, "ban": bool(ban)}


# ======================
# AI CALL
# ======================
async def ask_ai_with_memory(user_text: str, chat_id: str, user_id: int) -> str:
    context = await get_recent_chat_context(chat_id, limit=12)

    authorized = is_allowed(user_id)

    system_prompt = (
        "Bạn là trợ lý quản trị cho bot Telegram.\n"
        "- Trả lời ngắn gọn, đúng trọng tâm.\n"
        "- Nếu người dùng là admin/owner và yêu cầu quản trị (keyword/welcome/auto-post/đăng bài/ban user) "
        "hãy gọi tool tương ứng.\n"
        "- Nếu không phải admin/owner thì KHÔNG gọi tool quản trị, chỉ hướng dẫn.\n"
    )

    messages = [{"role": "system", "content": system_prompt}] + context + [{"role": "user", "content": user_text}]

    tools = None
    if authorized:
        tools = [
            {
                "type": "function",
                "function": {
                    "name": "add_keyword",
                    "description": "Tạo/cập nhật keyword để bot phản hồi.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "key": {"type": "string"},
                            "text": {"type": "string"},
                        },
                        "required": ["key", "text"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "delete_keyword",
                    "description": "Xóa keyword theo id.",
                    "parameters": {
                        "type": "object",
                        "properties": {"keyword_id": {"type": "integer"}},
                        "required": ["keyword_id"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "set_welcome",
                    "description": "Thiết lập tin nhắn chào mừng cho group.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "chat_id": {"type": "string"},
                            "text": {"type": "string"},
                        },
                        "required": ["chat_id", "text"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "post_now",
                    "description": "Đăng tin nhắn ngay vào group.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "chat_id": {"type": "string"},
                            "text": {"type": "string"},
                        },
                        "required": ["chat_id", "text"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "set_autopost",
                    "description": "Thiết lập auto-post định kỳ.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "chat_id": {"type": "string"},
                            "text": {"type": "string"},
                            "interval_minutes": {"type": "integer"},
                        },
                        "required": ["chat_id", "text", "interval_minutes"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "ban_user",
                    "description": "Ban/unban user.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "user_id": {"type": "integer"},
                            "ban": {"type": "boolean"},
                        },
                        "required": ["user_id", "ban"],
                    },
                },
            },
        ]

    resp = await ai_client.chat.completions.create(
        model="gpt-4o-mini",
        messages=messages,
        tools=tools,
        tool_choice="auto" if authorized else "none",
    )

    msg = resp.choices[0].message
    tool_calls = getattr(msg, "tool_calls", None)

    if not tool_calls:
        return msg.content or "OK"

    # execute tool calls
    results = []
    for tc in tool_calls:
        fn = tc.function.name
        args = json.loads(tc.function.arguments or "{}")

        # fallback chat_id nếu AI quên
        if "chat_id" not in args and fn in ("set_welcome", "post_now", "set_autopost"):
            args["chat_id"] = chat_id

        if fn == "add_keyword":
            results.append(await tool_add_keyword(**args))
        elif fn == "delete_keyword":
            results.append(await tool_delete_keyword(**args))
        elif fn == "set_welcome":
            results.append(await tool_set_welcome(**args))
        elif fn == "post_now":
            results.append(await tool_post_now(**args))
        elif fn == "set_autopost":
            results.append(await tool_set_autopost(**args))
        elif fn == "ban_user":
            results.append(await tool_ban_user(**args))
        else:
            results.append({"ok": False, "error": f"Unknown tool: {fn}"})

    # trả kết quả cho người dùng admin
    return "✅ Đã thực thi:\n" + "\n".join([str(r) for r in results])


# ======================
# HANDLERS
# ======================
@dp.message(F.text == "/start")
async def start(m: types.Message):
    if not m.from_user:
        return
    await upsert_user(m.from_user)

    if not is_allowed(m.from_user.id):
        return await m.answer("❌ Không có quyền")

    return await m.answer("🏠 Bot ready")


# ✅ chỉ xử lý text != /start để tránh trùng handler
@dp.message(F.text != "/start")
async def on_text(m: types.Message):
    if not m.text:
        return

    if m.from_user:
        await upsert_user(m.from_user)

    user_id = m.from_user.id if m.from_user else 0
    chat_id = str(m.chat.id)

    if await is_user_banned(user_id):
        return  # hoặc trả "bị ban"

    # 1) keyword match
    kw = match_keyword(m.text)
    await add_chat_message(chat_id, user_id, "user", m.text)

    if kw:
        await add_chat_message(chat_id, user_id, "assistant", kw.text)
        await send_text(m.chat.id, kw.text)
        return

    # 2) không match => gọi AI + memory
    reply = await ask_ai_with_memory(user_text=m.text, chat_id=chat_id, user_id=user_id)

    await add_chat_message(chat_id, user_id, "assistant", reply)
    await send_text(m.chat.id, reply)


@dp.chat_member()
async def welcome(event: types.ChatMemberUpdated):
    # ai đó join vào group
    if event.new_chat_member.status != "member":
        return

    chat_id = str(event.chat.id)

    async with SessionLocal() as db:
        w = (await db.execute(
            select(WelcomeSetting).where(WelcomeSetting.chat_id == chat_id, WelcomeSetting.active == 1)
        )).scalars().first()

    if w and w.text:
        await send_text(chat_id, w.text)


# ======================
# AUTO POST WORKER (anti-spam theo interval)
# ======================
async def auto_worker():
    while True:
        now = int(time.time())

        async with SessionLocal() as db:
            posts = (await db.execute(
                select(AutoPost).where(AutoPost.active == 1)
            )).scalars().all()

        for p in posts:
            if now - (p.last_sent_ts or 0) >= (p.interval * 60):
                try:
                    await send_text(p.chat_id, p.text)

                    async with SessionLocal() as db:
                        row = await db.get(AutoPost, p.id)
                        if row:
                            row.last_sent_ts = now
                            await db.commit()
                except Exception as e:
                    print("auto error:", e)

        await asyncio.sleep(10)


# ======================
# WEB ADMIN PANEL (sẵn có)
# ======================
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
async def add_admin(
    key: str = Form(""),
    user_id: int = Form(...),
    note: str = Form("")
):
    if not check_key(key):
        return HTMLResponse("403", status_code=403)

    async with SessionLocal() as db:
        db.add(AdminUser(user_id=user_id, note=note, created_at=int(time.time())))
        await db.commit()

    await load_admin_and_cache()
    return HTMLResponse("OK")


@app.post("/admin/delete", response_class=HTMLResponse)
async def del_admin(key: str = Form(""), user_id: int = Form(...)):
    if not check_key(key):
        return HTMLResponse("403", status_code=403)

    async with SessionLocal() as db:
        await db.execute(delete(AdminUser).where(AdminUser.user_id == user_id))
        await db.commit()

    await load_admin_and_cache()
    return HTMLResponse("Deleted")


# ======================
# NEW: USER MANAGEMENT API (cho dashboard React)
# ======================
@app.get("/api/users")
async def api_users(x_admin_key: str = Header(None)):
    try:
        require_web_admin(x_admin_key)
    except Exception:
        return {"error": "Unauthorized"}

    async with SessionLocal() as db:
        rows = (await db.execute(select(TelegramUser))).scalars().all()

    return [{
        "user_id": u.user_id,
        "username": u.username,
        "first_name": u.first_name,
        "is_banned": u.is_banned,
        "last_seen": u.last_seen.isoformat() if u.last_seen else None,
        "created_at": u.created_at.isoformat() if u.created_at else None,
    } for u in rows]


@app.post("/api/users/{user_id}/ban")
async def api_ban_user(user_id: int, x_admin_key: str = Header(None)):
    try:
        require_web_admin(x_admin_key)
    except Exception:
        return {"error": "Unauthorized"}

    async with SessionLocal() as db:
        row = (await db.execute(select(TelegramUser).where(TelegramUser.user_id == user_id))).scalars().first()
        if not row:
            return {"ok": False, "error": "User not found"}
        row.is_banned = True
        await db.commit()
    return {"ok": True}


@app.post("/api/users/{user_id}/unban")
async def api_unban_user(user_id: int, x_admin_key: str = Header(None)):
    try:
        require_web_admin(x_admin_key)
    except Exception:
        return {"error": "Unauthorized"}

    async with SessionLocal() as db:
        row = (await db.execute(select(TelegramUser).where(TelegramUser.user_id == user_id))).scalars().first()
        if not row:
            return {"ok": False, "error": "User not found"}
        row.is_banned = False
        await db.commit()
    return {"ok": True}


# ======================
# WEBHOOK
# ======================
@app.post("/webhook")
async def webhook(req: Request):
    data = await req.json()
    update = types.Update.model_validate(data)
    await dp.feed_update(bot, update)
    return {"ok": True}


# ======================
# STARTUP / SHUTDOWN
# ======================
@app.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    await load_admin_and_cache()
    asyncio.create_task(auto_worker())

    # nếu bạn muốn đúng như bạn đang dùng:
    await bot.set_webhook(f"{BASE_URL}/webhook")


@app.on_event("shutdown")
async def shutdown():
    await bot.delete_webhook()
