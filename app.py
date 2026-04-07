import os
import time
import asyncio
import logging
from datetime import datetime

from fastapi import FastAPI, Request, HTTPException
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, types, F
from aiogram.enums import ParseMode
from aiogram.types import Update
from aiogram.client.default import DefaultBotProperties

from sqlalchemy import Column, Integer, String, Text, select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base

# ======================
# CONFIG
# ======================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
DOMAIN = os.getenv("RAILWAY_PUBLIC_DOMAIN")
SECRET = os.getenv("WEBHOOK_SECRET", "secret123")

if not BOT_TOKEN or not DATABASE_URL or not DOMAIN:
    raise RuntimeError("Missing ENV variables")

WEBHOOK_PATH = f"/webhook/{SECRET}"
WEBHOOK_URL = f"https://{DOMAIN}{WEBHOOK_PATH}"

# ======================
# DATABASE
# ======================
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://")

engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    pool_pre_ping=True
)

SessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False
)

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
# BOT
# ======================
bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)

dp = Dispatcher()

# ======================
# FASTAPI
# ======================
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

    logging.info("Cache loaded")

# ======================
# HANDLERS
# ======================
@dp.message(F.text)
async def handle_message(m: types.Message):
    text = m.text.lower()

    for k in keyword_cache:
        if (k.mode == "exact" and text == k.key) or (k.mode == "contains" and k.key in text):
            await m.answer(k.text)
            return


@dp.chat_member()
async def welcome(event: types.ChatMemberUpdated):
    if event.new_chat_member.status == "member":
        chat_id = str(event.chat.id)
        w = welcome_cache.get(chat_id)

        if w:
            await bot.send_message(chat_id, w.text or "Welcome 👋")

# ======================
# WEBHOOK
# ======================
@app.post(WEBHOOK_PATH)
async def webhook(req: Request):
    try:
        data = await req.json()
        update = Update.model_validate(data)
        await dp.feed_update(bot, update)
        return {"ok": True}
    except Exception as e:
        logging.error(e)
        raise HTTPException(status_code=500)

# ======================
# ADMIN API
# ======================
@app.get("/")
async def home():
    return {"status": "running"}


@app.post("/add_keyword")
async def add_keyword(req: Request):
    data = await req.json()

    async with SessionLocal() as db:
        db.add(Keyword(key=data["key"], text=data["text"]))
        await db.commit()

    await load_cache()
    return {"ok": True}

# ======================
# AUTO POST WORKER
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
                    logging.error(f"AutoPost error: {e}")

        await asyncio.sleep(30)

# ======================
# STARTUP / SHUTDOWN
# ======================
@app.on_event("startup")
async def startup():
    logging.info("Starting app...")

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    await load_cache()

    await bot.set_webhook(WEBHOOK_URL)
    logging.info(f"Webhook set: {WEBHOOK_URL}")

    asyncio.create_task(auto_worker())


@app.on_event("shutdown")
async def shutdown():
    logging.info("Shutting down...")
    await bot.delete_webhook()
    await bot.session.close()
