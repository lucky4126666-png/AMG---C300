# =========================================
# 🚀 TELEGRAM GROUP MANAGER PRO - FINAL SAFE
# =========================================

import os, time, asyncio, logging, jwt, random
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, WebSocket, Header, HTTPException
from aiogram import Bot, Dispatcher, types, F
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, Integer, String, Text, select, text
from passlib.context import CryptContext

# ================= CONFIG =================
logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN")
BASE_URL = os.getenv("BASE_URL")
DATABASE_URL = os.getenv("DATABASE_URL")
SECRET_KEY = os.getenv("SECRET_KEY", "secret")

if not BOT_TOKEN or not BASE_URL or not DATABASE_URL:
    raise RuntimeError("Missing ENV")

BASE_URL = BASE_URL.rstrip("/")

# ================= DB =================
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)

engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

@asynccontextmanager
async def get_db():
    async with SessionLocal() as db:
        try:
            yield db
        except:
            await db.rollback()
            raise
        finally:
            await db.close()

# ================= WAIT DB =================
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
    text = Column(Text)
    interval = Column(Integer, default=60)
    last_sent = Column(Integer, default=0)

class AutoPostGroup(Base):
    __tablename__ = "auto_post_group"
    id = Column(Integer, primary_key=True)
    post_id = Column(Integer)
    group_id = Column(Integer)

class Welcome(Base):
    __tablename__ = "welcome"
    id = Column(Integer, primary_key=True)
    chat_id = Column(String)
    text = Column(Text)

# ================= BOT =================
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ================= WS =================
connections = []

async def broadcast(data):
    for c in connections:
        try:
            await c.send_json(data)
        except:
            pass

# ================= FASTAPI =================
app = FastAPI()
stop_event = asyncio.Event()

# ================= GROUP AUTO SAVE =================
@dp.my_chat_member()
async def save_group(event: types.ChatMemberUpdated):
    if event.new_chat_member.status not in ["member", "administrator"]:
        return

    chat = event.chat

    async with get_db() as db:
        exists = (await db.execute(
            select(Group).where(Group.chat_id == str(chat.id))
        )).scalars().first()

        if not exists:
            db.add(Group(chat_id=str(chat.id), title=chat.title))
            await db.commit()

# ================= ADMIN COMMAND =================
@dp.message(F.text == "/off")
async def off(m: types.Message):
    async with get_db() as db:
        g = (await db.execute(
            select(Group).where(Group.chat_id == str(m.chat.id))
        )).scalars().first()

        if g:
            g.active = 0
            await db.commit()

    await m.answer("❌ Bot OFF")

@dp.message(F.text == "/on")
async def on(m: types.Message):
    async with get_db() as db:
        g = (await db.execute(
            select(Group).where(Group.chat_id == str(m.chat.id))
        )).scalars().first()

        if g:
            g.active = 1
            await db.commit()

    await m.answer("✅ Bot ON")

# ================= ANTI LINK =================
@dp.message(F.text.contains("http"))
async def anti_link(m: types.Message):
    try:
        await m.delete()
    except:
        pass

# ================= KEYWORD =================
@dp.message(F.text)
async def keyword(m: types.Message):
    text_msg = m.text.lower()

    async with get_db() as db:
        g = (await db.execute(
            select(Group).where(Group.chat_id == str(m.chat.id))
        )).scalars().first()

        if not g or not g.active:
            return

        rows = (await db.execute(select(Keyword))).scalars().all()

    for row in rows:
        keys = [k.strip() for k in row.key.split(",")]

        for k in keys:
            if k in text_msg:
                await m.answer(row.text)

                await broadcast({
                    "type": "keyword",
                    "text": text_msg
                })
                return

# ================= WELCOME =================
@dp.chat_member()
async def welcome(event: types.ChatMemberUpdated):
    if event.new_chat_member.status != "member":
        return

    async with get_db() as db:
        row = (await db.execute(
            select(Welcome).where(Welcome.chat_id == str(event.chat.id))
        )).scalars().first()

    if row:
        await bot.send_message(event.chat.id, row.text)

# ================= SAFE SEND =================
async def safe_send(chat_id, text_msg):
    for _ in range(3):
        try:
            await bot.send_message(chat_id, text_msg)
            return True
        except:
            await asyncio.sleep(2)
    return False

# ================= AUTO WORKER =================
async def auto_worker():
    while not stop_event.is_set():
        now = int(time.time())

        async with get_db() as db:
            posts = (await db.execute(select(AutoPost))).scalars().all()

        for p in posts:
            if now - (p.last_sent or 0) >= p.interval * 60:

                async with get_db() as db:
                    links = (await db.execute(
                        select(AutoPostGroup).where(AutoPostGroup.post_id == p.id)
                    )).scalars().all()

                for link in links:
                    async with get_db() as db:
                        g = await db.get(Group, link.group_id)

                    if not g or not g.active:
                        continue

                    await asyncio.sleep(random.randint(2, 5))

                    ok = await safe_send(g.chat_id, p.text)

                    if ok:
                        await broadcast({
                            "type": "auto_post",
                            "group": g.chat_id
                        })

                async with get_db() as db:
                    row = await db.get(AutoPost, p.id)
                    row.last_sent = now
                    await db.commit()

        await asyncio.sleep(10)

# ================= WEBHOOK =================
@app.post("/webhook")
async def webhook(req: Request):
    data = await req.json()
    update = types.Update.model_validate(data)
    await dp.feed_update(bot, update)
    return {"ok": True}

# ================= WS =================
@app.websocket("/ws")
async def ws(ws: WebSocket):
    await ws.accept()
    connections.append(ws)

    try:
        while True:
            await ws.receive_text()
    except:
        connections.remove(ws)

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

    logging.info("🚀 BOT READY")

# ================= STOP =================
@app.on_event("shutdown")
async def shutdown():
    stop_event.set()
    await bot.session.close()
