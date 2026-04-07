# =========================================
# 🚀 TELEGRAM BOT PRO (FULL CLEAN BUILD)
# Stable - Scalable - SaaS Ready
# =========================================

import os
import time
import asyncio
import logging

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, Integer, String, Text, select
from sqlalchemy import text as sql_text

# ================= CONFIG =================
logging.basicConfig(level=logging.INFO)
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)

BOT_TOKEN = os.getenv("BOT_TOKEN")
BASE_URL = os.getenv("BASE_URL")
DATABASE_URL = os.getenv("DATABASE_URL")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))

if not BOT_TOKEN or not BASE_URL or not DATABASE_URL:
    raise RuntimeError("Missing ENV")

BASE_URL = BASE_URL.rstrip("/")

# ================= DATABASE =================
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)

engine = create_async_engine(
    DATABASE_URL,
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,
    pool_recycle=1800,
    pool_pre_ping=True,
)

SessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

@asynccontextmanager
async def get_db():
    async with SessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()

# ================= MODELS =================
class Keyword(Base):
    __tablename__ = "keywords"
    id = Column(Integer, primary_key=True)
    key = Column(String)
    text = Column(Text)

class AutoPost(Base):
    __tablename__ = "auto_posts"
    id = Column(Integer, primary_key=True)
    chat_id = Column(String)
    text = Column(Text)
    interval = Column(Integer, default=60)
    last_sent = Column(Integer, default=0)
    active = Column(Integer, default=1)

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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

stop_event = asyncio.Event()

# ================= MENU =================
def main_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚙️ Auto Post", callback_data="menu_auto")],
        [InlineKeyboardButton(text="🔑 Keyword", callback_data="menu_kw")],
        [InlineKeyboardButton(text="👋 Welcome", callback_data="menu_wl")],
    ])

# ================= START =================
@dp.message(CommandStart())
async def start(m: types.Message):
    await m.answer("🚀 BOT READY", reply_markup=main_menu())

# ================= CALLBACK =================
@dp.callback_query(F.data.startswith("menu_"))
async def menu_handler(c: types.CallbackQuery):
    if c.data == "menu_auto":
        await c.message.answer("⚙️ Auto Post menu")
    elif c.data == "menu_kw":
        await c.message.answer("🔑 Keyword menu")
    elif c.data == "menu_wl":
        await c.message.answer("👋 Welcome menu")
    await c.answer()

# ================= KEYWORD =================
@dp.message()
async def keyword_handler(m: types.Message):
    if not m.text or m.text.startswith("/"):
        return

    async with get_db() as db:
        row = (await db.execute(
            select(Keyword).where(Keyword.key.ilike(f"%{m.text}%"))
        )).scalars().first()

    if row:
        await m.answer(row.text)
    else:
        await m.answer("🤖 Bot đang chạy...")

# ================= AUTO WORKER =================
async def auto_worker():
    while not stop_event.is_set():
        now = int(time.time())

        async with get_db() as db:
            rows = (await db.execute(select(AutoPost))).scalars().all()

        for p in rows:
            if p.active and now - (p.last_sent or 0) >= p.interval * 60:
                try:
                    await bot.send_message(p.chat_id, p.text)

                    async with get_db() as db:
                        row = await db.get(AutoPost, p.id)
                        row.last_sent = now
                        await db.commit()

                except Exception as e:
                    logging.error(e)

        await asyncio.sleep(15)

# ================= WELCOME =================
@dp.chat_member()
async def welcome(event: types.ChatMemberUpdated):
    if event.new_chat_member.status != "member":
        return

    chat_id = str(event.chat.id)
    user = event.new_chat_member.user

    async with get_db() as db:
        row = (await db.execute(select(Welcome).where(Welcome.chat_id == chat_id))).scalars().first()

    if row and row.enabled:
        await bot.send_message(chat_id, row.text.replace("{name}", user.full_name))

# ================= WEBHOOK =================
@app.post("/webhook")
async def webhook(req: Request):
    update = types.Update.model_validate(await req.json())
    await dp.feed_update(bot, update)
    return {"ok": True}

@app.get("/")
async def root():
    return {"ok": True}

# ================= STARTUP =================
async def wait_db():
    for _ in range(10):
        try:
            async with engine.begin() as conn:
                await conn.execute(sql_text("SELECT 1"))
            return
        except:
            await asyncio.sleep(2)

@app.on_event("startup")
async def startup():
    await wait_db()

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    await bot.delete_webhook(drop_pending_updates=True)
    await bot.set_webhook(f"{BASE_URL}/webhook")

    asyncio.create_task(auto_worker())

# ================= SHUTDOWN =================
@app.on_event("shutdown")
async def shutdown():
    stop_event.set()
    await bot.session.close()

# ================= RUN =================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
