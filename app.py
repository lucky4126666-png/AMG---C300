import os
import time
import asyncio
import logging

from fastapi import FastAPI, Request
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, types
from aiogram.enums import ParseMode
from aiogram.types import Update
from aiogram.client.default import DefaultBotProperties

from sqlalchemy import Column, Integer, String, Text, select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base

# ======================
# BASIC
# ======================
logging.basicConfig(level=logging.INFO)
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
BASE_URL = os.getenv("BASE_URL")  # ⚠️ đổi sang BASE_URL

SECRET = os.getenv("WEBHOOK_SECRET", "secret123")

if not BOT_TOKEN:
    raise RuntimeError("Missing BOT_TOKEN")

if not BASE_URL:
    raise RuntimeError("Missing BASE_URL")

WEBHOOK_PATH = f"/webhook/{SECRET}"
WEBHOOK_URL = f"{BASE_URL}{WEBHOOK_PATH}"

# ======================
# DB
# ======================
if DATABASE_URL.startswith("postgres"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://")

engine = create_async_engine(DATABASE_URL, echo=False)
SessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

# ======================
# MODELS
# ======================
class Keyword(Base):
    __tablename__ = "keywords"
    id = Column(Integer, primary_key=True)
    key = Column(String, unique=True)
    text = Column(Text)
    mode = Column(String, default="contains")

class Welcome(Base):
    __tablename__ = "welcome"
    id = Column(Integer, primary_key=True)
    chat_id = Column(String, unique=True)
    text = Column(Text)

class AutoPost(Base):
    __tablename__ = "auto_post"
    id = Column(Integer, primary_key=True)
    chat_id = Column(String)
    text = Column(Text)
    interval = Column(Integer, default=10)
    last_sent = Column(Integer, default=0)

# ======================
# BOT + APP
# ======================
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
app = FastAPI()

# ======================
# CACHE
# ======================
keyword_cache = []
welcome_cache = {}
auto_cache = []

# ======================
# LOAD CACHE
# ======================
async def load_cache():
    global keyword_cache, welcome_cache, auto_cache

    async with SessionLocal() as db:
        kw = (await db.execute(select(Keyword))).scalars().all()
        wl = (await db.execute(select(Welcome))).scalars().all()
        ap = (await db.execute(select(AutoPost))).scalars().all()

    keyword_cache = kw
    welcome_cache = {w.chat_id: w for w in wl}
    auto_cache = ap

# ======================
# HANDLER
# ======================
@dp.message()
async def handle_message(m: types.Message):
    if not m.text:
        return

    text = m.text.lower()

    for k in keyword_cache:
        if (k.mode == "exact" and text == k.key) or (k.mode == "contains" and k.key in text):
            await m.answer(k.text)
            return

# ======================
# WELCOME FIX
# ======================
@dp.chat_member()
async def welcome(event: types.ChatMemberUpdated):
    if event.new_chat_member.status == "member":
        chat_id = str(event.chat.id)
        w = welcome_cache.get(chat_id)
        if w:
            await bot.send_message(chat_id, w.text or "Welcome!")

# ======================
# WEBHOOK
# ======================
@app.post(WEBHOOK_PATH)
async def webhook(req: Request):
    data = await req.json()
    update = Update.model_validate(data)
    await dp.feed_update(bot, update)
    return {"ok": True}

@app.get("/")
async def home():
    return {"status": "running"}

# ======================
# AUTO WORKER
# ======================
async def auto_worker():
    while True:
        now = int(time.time())
        for p in auto_cache:
            if now - p.last_sent > p.interval * 60:
                try:
                    await bot.send_message(p.chat_id, p.text)

                    async with SessionLocal() as db:
                        row = await db.get(AutoPost, p.id)
                        if row:
                            row.last_sent = now
                            await db.commit()

                    p.last_sent = now
                except Exception as e:
                    logging.error(e)

        await asyncio.sleep(30)

# ======================
# STARTUP
# ======================
@app.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    await load_cache()

    await bot.delete_webhook(drop_pending_updates=True)  # ⚠️ FIX
    await bot.set_webhook(WEBHOOK_URL)

    asyncio.create_task(auto_worker())

    logging.info(f"Webhook: {WEBHOOK_URL}")

@app.on_event("shutdown")
async def shutdown():
    await bot.delete_webhook()
    await bot.session.close()
