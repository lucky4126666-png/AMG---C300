# ======================
# FULL SAAS TELEGRAM BOT (AI + KEYWORD + WELCOME + AUTO POST)
# ======================

import os
import time
import asyncio
import logging
from urllib.parse import urlparse

from fastapi import FastAPI, Request
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import CommandStart

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, Integer, String, Text, select

# ======================
# CONFIG
# ======================
logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN")
BASE_URL = os.getenv("BASE_URL")
DATABASE_URL = os.getenv("DATABASE_URL")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

if not BOT_TOKEN or not BASE_URL or not DATABASE_URL:
    raise RuntimeError("Missing ENV")

BASE_URL = BASE_URL.rstrip("/")

# ======================
# DATABASE
# ======================
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)
elif DATABASE_URL.startswith("postgresql://") and "+asyncpg" not in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

# ======================
# MODELS
# ======================
class Keyword(Base):
    __tablename__ = "keywords"
    id = Column(Integer, primary_key=True)
    key = Column(String)
    text = Column(Text)
    image = Column(String)
    button = Column(Text)
    active = Column(Integer, default=1)

class Welcome(Base):
    __tablename__ = "welcome"
    id = Column(Integer, primary_key=True)
    chat_id = Column(String)
    text = Column(Text)
    active = Column(Integer, default=1)

class AutoPost(Base):
    __tablename__ = "auto_post"
    id = Column(Integer, primary_key=True)
    chat_id = Column(String)
    text = Column(Text)
    interval = Column(Integer)
    last_sent = Column(Integer, default=0)
    active = Column(Integer, default=1)

# ======================
# BOT
# ======================
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
app = FastAPI()

keywords_cache = []
stop_event = asyncio.Event()

# ======================
# UTIL
# ======================
def valid_url(u):
    try:
        return urlparse(u).scheme in ("http", "https", "tg")
    except:
        return False


def parse_buttons(btn):
    if not btn:
        return None
    rows = []
    for line in btn.splitlines():
        row = []
        for part in line.split("&&"):
            if "-" in part:
                t, u = part.split("-", 1)
                if valid_url(u.strip()):
                    row.append(InlineKeyboardButton(text=t.strip(), url=u.strip()))
        if row:
            rows.append(row)
    return InlineKeyboardMarkup(inline_keyboard=rows) if rows else None

# ======================
# LOAD CACHE
# ======================
async def load_keywords():
    global keywords_cache
    async with SessionLocal() as db:
        keywords_cache = (await db.execute(select(Keyword))).scalars().all()

# ======================
# KEYWORD
# ======================
@dp.message(F.text)
async def keyword_handler(m: types.Message):
    text = m.text.lower()

    for k in keywords_cache:
        if k.key and k.key.lower() in text:
            kb = parse_buttons(k.button)
            if k.image:
                await m.answer_photo(k.image, caption=k.text or "", reply_markup=kb)
            else:
                await m.answer(k.text or "", reply_markup=kb)
            return

# ======================
# AI
# ======================
@dp.message(F.text.startswith("ai "))
async def ai_handler(m: types.Message):
    if not OPENAI_API_KEY:
        return await m.answer("AI chưa cấu hình")

    from openai import AsyncOpenAI
    client = AsyncOpenAI(api_key=OPENAI_API_KEY)

    res = await client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": m.text[3:]}]
    )

    await m.answer(res.choices[0].message.content)

# ======================
# AUTO POST
# ======================
async def auto_worker():
    while not stop_event.is_set():
        now = int(time.time())

        async with SessionLocal() as db:
            posts = (await db.execute(select(AutoPost))).scalars().all()

        for p in posts:
            if not p.active:
                continue
            if now - (p.last_sent or 0) >= p.interval * 60:
                await bot.send_message(p.chat_id, p.text)

                async with SessionLocal() as db:
                    row = await db.get(AutoPost, p.id)
                    row.last_sent = now
                    await db.commit()

        await asyncio.sleep(10)

# ======================
# START
# ======================
@dp.message(CommandStart())
async def start(m: types.Message):
    await m.answer("Bot SaaS Ready 🚀")

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

    await load_keywords()
    asyncio.create_task(auto_worker())
    await bot.set_webhook(f"{BASE_URL}/webhook")

# ======================
# SHUTDOWN
# ======================
@app.on_event("shutdown")
async def shutdown():
    stop_event.set()
    await bot.delete_webhook()
    await bot.session.close()
    await engine.dispose()

# ======================
# RUN
# ======================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
