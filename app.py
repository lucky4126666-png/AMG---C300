# =========================================
# 🚀 FULL SAAS TELEGRAM BOT (PRO MAX)
# FEATURES:
# - Auto Post
# - Keyword Reply
# - Welcome Message
# - Admin Menu
# - Multi Language (VI + CN)
# - Railway Safe (anti crash)
# =========================================

import os
import time
import asyncio
import logging

from fastapi import FastAPI, Request
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import CommandStart

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, Integer, String, Text, select
from sqlalchemy import text as sql_text

# ================= CONFIG =================
logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN")
BASE_URL = os.getenv("BASE_URL")
DATABASE_URL = os.getenv("DATABASE_URL")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))

if not BOT_TOKEN or not BASE_URL or not DATABASE_URL:
    raise RuntimeError("Missing ENV")

BASE_URL = BASE_URL.rstrip("/")

# ================= DB =================
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)
elif DATABASE_URL.startswith("postgresql://") and "+asyncpg" not in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

# ================= MODELS =================
class AutoPost(Base):
    __tablename__ = "auto_post"
    id = Column(Integer, primary_key=True)
    chat_id = Column(String)
    text = Column(Text)
    interval = Column(Integer, default=60)
    last_sent = Column(Integer, default=0)
    active = Column(Integer, default=1)

class Keyword(Base):
    __tablename__ = "keyword"
    id = Column(Integer, primary_key=True)
    key = Column(String)
    text = Column(Text)

class Welcome(Base):
    __tablename__ = "welcome"
    id = Column(Integer, primary_key=True)
    chat_id = Column(String)
    text = Column(Text)
    enabled = Column(Integer, default=1)

# ================= BOT =================
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
app = FastAPI()

user_state = {}
stop_event = asyncio.Event()
webhook_tasks = set()
MAX_TASKS = 100

# ================= UTILS =================
def set_state(uid, k, v):
    user_state.setdefault(uid, {})[k] = v

def get_state(uid, k):
    return user_state.get(uid, {}).get(k)

def reset(uid):
    user_state.pop(uid, None)

# ================= MENU =================
def admin_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚙️ Auto", callback_data="auto_menu")],
        [InlineKeyboardButton(text="🔑 Keyword", callback_data="kw_menu")],
    ])

# ================= START =================
@dp.message(CommandStart())
async def start(m: types.Message):
    await m.answer("👑 ADMIN PANEL", reply_markup=admin_menu())

# ================= KEYWORD =================
@dp.message()
async def keyword_handler(m: types.Message):
    async with SessionLocal() as db:
        rows = (await db.execute(select(Keyword))).scalars().all()

    for k in rows:
        if k.key.lower() in m.text.lower():
            return await m.answer(k.text)

# ================= AUTO POST =================
async def auto_worker():
    try:
        while not stop_event.is_set():
            now = int(time.time())

            async with SessionLocal() as db:
                rows = (await db.execute(select(AutoPost))).scalars().all()

            for p in rows:
                if not p.active:
                    continue

                if now - (p.last_sent or 0) >= p.interval * 60:
                    try:
                        await bot.send_message(p.chat_id, p.text)

                        async with SessionLocal() as db:
                            row = await db.get(AutoPost, p.id)
                            row.last_sent = now
                            await db.commit()
                    except Exception as e:
                        logging.error(e)

            await asyncio.sleep(10)

    except Exception as e:
        logging.error(f"WORKER CRASH: {e}")

# ================= WELCOME =================
@dp.chat_member()
async def welcome(event: types.ChatMemberUpdated):
    if event.new_chat_member.status != "member":
        return

    user = event.new_chat_member.user
    chat_id = str(event.chat.id)

    async with SessionLocal() as db:
        row = (await db.execute(select(Welcome).where(Welcome.chat_id == chat_id))).scalars().first()

    if row and row.enabled:
        text = (row.text or "Welcome {name}").replace("{name}", user.full_name)
        await bot.send_message(chat_id, text)

# ================= WEBHOOK =================
async def process_update(update):
    try:
        await asyncio.wait_for(dp.feed_update(bot, update), timeout=10)
    except Exception:
        logging.exception("Update error")

@app.post("/webhook")
async def webhook(req: Request):
    data = await req.json()
    update = types.Update.model_validate(data)

    if len(webhook_tasks) > MAX_TASKS:
        return {"ok": True}

    task = asyncio.create_task(process_update(update))
    webhook_tasks.add(task)
    task.add_done_callback(lambda t: webhook_tasks.discard(t))

    return {"ok": True}

@app.get("/")
async def root():
    return {"ok": True}

# ================= STARTUP =================
async def wait_for_db():
    for _ in range(10):
        try:
            async with engine.begin() as conn:
                await conn.execute(sql_text("SELECT 1"))
            return
        except:
            await asyncio.sleep(2)

@app.on_event("startup")
async def startup():
    await wait_for_db()

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    webhook_url = f"{BASE_URL}/webhook"

    await bot.delete_webhook(drop_pending_updates=True)
    await bot.set_webhook(webhook_url)

    asyncio.create_task(auto_worker())

# ================= SHUTDOWN =================
@app.on_event("shutdown")
async def shutdown():
    stop_event.set()

    for t in list(webhook_tasks):
        t.cancel()

    await bot.delete_webhook()
    await bot.session.close()

# ================= RUN =================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("
