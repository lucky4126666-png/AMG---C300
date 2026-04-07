# =========================================
# 🚀 TELEGRAM BOT FULL PRO (FIX NO REPLY + MENU PRO)
# =========================================

import os
import time
import asyncio
import logging

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from aiogram import Bot, Dispatcher, types
from aiogram.filters import CommandStart
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, Integer, String, Text, select

# ================= CONFIG =================
logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN")
BASE_URL = os.getenv("BASE_URL")
DATABASE_URL = os.getenv("DATABASE_URL")

if not BOT_TOKEN or not BASE_URL or not DATABASE_URL:
    raise RuntimeError("Missing ENV")

BASE_URL = BASE_URL.rstrip("/")

# ================= DB =================
engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)
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
    __tablename__ = "keyword"
    id = Column(Integer, primary_key=True)
    key = Column(String)
    text = Column(Text)

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

# ================= MENU =================
def admin_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚙️ Auto Post", callback_data="auto")],
        [InlineKeyboardButton(text="🔑 Keyword", callback_data="kw")],
    ])

# ================= START =================
@dp.message(CommandStart())
async def start(m: types.Message):
    await m.answer("👑 MENU", reply_markup=admin_menu())

# ================= KEYWORD =================
@dp.message()
async def keyword(m: types.Message):
    if not m.text or m.text.startswith("/"):
        return

    async with get_db() as db:
        row = (await db.execute(
            select(Keyword).where(Keyword.key.ilike(f"%{m.text}%"))
        )).scalars().first()

    if row:
        await m.answer(row.text)
    else:
        await m.answer("🤖 Bot đang hoạt động")

# ================= CALLBACK =================
@dp.callback_query()
async def callback(c: types.CallbackQuery):
    if c.data == "auto":
        await c.message.answer("⚙️ Auto menu")
    elif c.data == "kw":
        await c.message.answer("🔑 Keyword menu")
    await c.answer()

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
@app.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    await bot.delete_webhook(drop_pending_updates=True)
    await bot.set_webhook(f"{BASE_URL}/webhook")

# ================= RUN =================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
