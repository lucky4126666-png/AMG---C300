# =========================================
# 🚀 TELEGRAM GROUP MANAGER - FINAL CLEAN
# =========================================

import os, time, asyncio, logging, random
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, WebSocket
from aiogram import Bot, Dispatcher, types, F

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, Integer, String, Text, select, text

# ================= CONFIG =================
logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN")
BASE_URL = os.getenv("BASE_URL")
DATABASE_URL = os.getenv("DATABASE_URL")

if not BOT_TOKEN or not BASE_URL or not DATABASE_URL:
    raise RuntimeError("Missing ENV")

# ================= DB =================
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)

engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

@asynccontextmanager
async def get_db():
    async with SessionLocal() as db:
        yield db

async def wait_db():
    for _ in range(10):
        try:
            async with engine.begin() as conn:
                await conn.execute(text("SELECT 1"))
            return
        except:
            await asyncio.sleep(2)

# ================= MODELS =================
class Group(Base):
    __tablename__ = "groups"
    id = Column(Integer, primary_key=True)
    chat_id = Column(String)
    title = Column(String)
    active = Column(Integer, default=1)

class Keyword(Base):
    __tablename__ = "keyword"
    id = Column(Integer, primary_key=True)
    key = Column(String)
    text = Column(Text)

class AutoPost(Base):
    __tablename__ = "auto_post"
    id = Column(Integer, primary_key=True)
    chat_id = Column(String)
    text = Column(Text)
    interval = Column(Integer, default=60)
    last_sent = Column(Integer, default=0)

# ================= BOT =================
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ================= FASTAPI =================
app = FastAPI()
stop_event = asyncio.Event()

# ================= GROUP SAVE =================
@dp.my_chat_member()
async def save_group(event: types.ChatMemberUpdated):
    if event.new_chat_member.status not in ["member", "administrator"]:
        return

    async with get_db() as db:
        exists = (await db.execute(
            select(Group).where(Group.chat_id == str(event.chat.id))
        )).scalars().first()

        if not exists:
            db.add(Group(
                chat_id=str(event.chat.id),
                title=event.chat.title
            ))
            await db.commit()

# ================= KEYWORD =================
@dp.message(F.text)
async def keyword(m: types.Message):
    text_msg = m.text.lower()

    async with get_db() as db:
        rows = (await db.execute(select(Keyword))).scalars().all()

    for row in rows:
        if row.key.lower() in text_msg:
            await m.answer(row.text)
            return

# ================= AUTO POST =================
async def auto_worker():
    while not stop_event.is_set():
        now = int(time.time())

        async with get_db() as db:
            posts = (await db.execute(select(AutoPost))).scalars().all()

        for p in posts:
            if now - (p.last_sent or 0) >= p.interval * 60:
                try:
                    await bot.send_message(p.chat_id, p.text)

                    async with get_db() as db:
                        row = await db.get(AutoPost, p.id)
                        row.last_sent = now
                        await db.commit()

                except Exception as e:
                    logging.error(e)

        await asyncio.sleep(10)

# ================= WEBHOOK =================
@app.post("/webhook")
async def webhook(req: Request):
    data = await req.json()
    update = types.Update.model_validate(data)
    await dp.feed_update(bot, update)
    return {"ok": True}

# ================= ROOT =================
@app.get("/")
async def root():
    return {"status": "running"}

# ================= START =================
@app.on_event("startup")
async def startup():
    await wait_db()

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    await bot.delete_webhook(drop_pending_updates=True)
    await bot.set_webhook(f"{BASE_URL}/webhook")

    asyncio.create_task(auto_worker())

    print("🚀 READY")

# ================= STOP =================
@app.on_event("shutdown")
async def shutdown():
    stop_event.set()
    await bot.session.close()
