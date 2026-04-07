import os
import time
import asyncio
import logging

from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import JSONResponse
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, types
from aiogram.types import Update
from aiogram.enums import ParseMode
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
BASE_URL = os.getenv("BASE_URL")
SECRET = os.getenv("WEBHOOK_SECRET", "secret123")

if not BOT_TOKEN or not BASE_URL or not DATABASE_URL:
    raise RuntimeError("Missing ENV")

WEBHOOK_PATH = f"/webhook/{SECRET}"
WEBHOOK_URL = f"{BASE_URL}{WEBHOOK_PATH}"

# ======================
# DB
# ======================
if DATABASE_URL.startswith("postgres://"):
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
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# ======================
# AI MEMORY
# ======================
memory = {}

async def ai_reply(user_id, text):
    import openai
    openai.api_key = os.getenv("OPENAI_KEY")

    ctx = memory.get(user_id, [])
    ctx.append({"role": "user", "content": text})

    res = openai.ChatCompletion.create(
        model="gpt-4o-mini",
        messages=ctx[-10:]
    )

    reply = res.choices[0].message.content
    ctx.append({"role": "assistant", "content": reply})
    memory[user_id] = ctx

    return reply

# ======================
# HANDLER
# ======================
@dp.message()
async def handle_message(m: types.Message):
    if not m.text:
        return

    text = m.text.lower()

    async with SessionLocal() as db:
        kws = (await db.execute(select(Keyword))).scalars().all()

    for k in kws:
        if (k.mode == "exact" and text == k.key) or (k.mode == "contains" and k.key in text):
            await m.answer(k.text)
            return

    # AI fallback
    reply = await ai_reply(m.from_user.id, text)
    await m.answer(reply)

# ======================
# AUTO POST
# ======================
async def auto_worker():
    while True:
        now = int(time.time())

        async with SessionLocal() as db:
            posts = (await db.execute(select(AutoPost))).scalars().all()

            for p in posts:
                if now - (p.last_sent or 0) > p.interval * 60:
                    try:
                        await bot.send_message(p.chat_id, p.text)

                        p.last_sent = now
                        await db.commit()

                    except Exception as e:
                        logging.error(e)

        await asyncio.sleep(20)

# ======================
# APP
# ======================
app = FastAPI()

# ======================
# WEBSOCKET
# ======================
clients = []

@app.websocket("/ws")
async def ws(ws: WebSocket):
    await ws.accept()
    clients.append(ws)

    try:
        while True:
            await ws.receive_text()
    except:
        clients.remove(ws)

async def broadcast(data):
    for c in clients:
        try:
            await c.send_json(data)
        except:
            pass

# ======================
# WEBHOOK
# ======================
@app.post(WEBHOOK_PATH)
async def webhook(req: Request):
    try:
        data = await req.json()
        update = Update.model_validate(data)
        await dp.feed_update(bot, update)
    except Exception as e:
        logging.error(e)

    return JSONResponse({"ok": True})

@app.get("/")
async def home():
    return {"status": "SaaS BOT RUNNING 🚀"}

# ======================
# STARTUP
# ======================
@app.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    await bot.delete_webhook(drop_pending_updates=True)
    await bot.set_webhook(WEBHOOK_URL)

    asyncio.create_task(auto_worker())

    logging.info(f"Webhook: {WEBHOOK_URL}")
