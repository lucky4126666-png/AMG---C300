import os
import time
import asyncio
import contextlib
from datetime import datetime

from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse

from aiogram import Bot, Dispatcher, types, F
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, Integer, String, Text, select, delete

# ======================
# ENV
# ======================
BOT_TOKEN = os.getenv("BOT_TOKEN")
BASE_URL = os.getenv("BASE_URL")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
WEB_ADMIN_KEY = os.getenv("WEB_ADMIN_KEY", "")

# ======================
# DB
# ======================
DATABASE_URL = "sqlite+aiosqlite:///./bot.db"

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
    last_sent_ts = Column(Integer, default=0)
    active = Column(Integer, default=1)

# ======================
# BOT
# ======================
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ======================
# CACHE
# ======================
admin_cache = set()

def is_allowed(user_id: int):
    return user_id == OWNER_ID or user_id in admin_cache

async def load_admin():
    global admin_cache
    async with SessionLocal() as db:
        rows = (await db.execute(select(AdminUser))).scalars().all()
    admin_cache = {r.user_id for r in rows}

# ======================
# HELPER
# ======================
async def send_text(chat_id, text):
    return await bot.send_message(chat_id, text)

# ======================
# START
# ======================
@dp.message(F.text == "/start")
async def start(m: types.Message):
    if not m.from_user:
        return

    if not is_allowed(m.from_user.id):
        return await m.answer("❌ Không có quyền")

    await m.answer("🏠 Bot ready")

# ======================
# KEYWORD AUTO REPLY
# ======================
@dp.message()
async def auto_reply(m: types.Message):
    if not m.text:
        return

    async with SessionLocal() as db:
        kws = (await db.execute(
            select(Keyword).where(Keyword.active == 1)
        )).scalars().all()

    for k in kws:
        if k.key.lower() in m.text.lower():
            await send_text(m.chat.id, k.text)
            break

# ======================
# AUTO POST WORKER
# ======================
async def auto_worker():
    while True:
        now = int(time.time())

        async with SessionLocal() as db:
            posts = (await db.execute(
                select(AutoPost).where(AutoPost.active == 1)
            )).scalars().all()

        for p in posts:
            if now - p.last_sent_ts >= p.interval * 60:
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
        db.add(AdminUser(
            user_id=user_id,
            note=note,
            created_at=int(time.time())
        ))
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
# STARTUP
# ======================
@app.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    await load_admin()

    asyncio.create_task(auto_worker())

    await bot.set_webhook(f"{BASE_URL}/webhook")

# ======================
# SHUTDOWN
# ======================
@app.on_event("shutdown")
async def shutdown():
    await bot.delete_webhook()
