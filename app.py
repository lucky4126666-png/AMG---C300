import os
import time
import ssl
import asyncio
import logging
from datetime import datetime

from aiohttp import web
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, types
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, Text, select

# ======================
# BASIC
# ======================
logging.basicConfig(level=logging.INFO)
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "secret")
PORT = int(os.getenv("PORT", 8080))
BASE_URL = os.getenv("RAILWAY_PUBLIC_DOMAIN")

if not BOT_TOKEN:
    raise RuntimeError("Missing BOT_TOKEN")

# ======================
# DB ENGINE (ULTIMATE)
# ======================
engine = create_async_engine(
    DATABASE_URL.replace("postgres://", "postgresql+asyncpg://"),
    echo=False,
    future=True,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
    pool_recycle=1800,
    connect_args={"ssl": "require"}
)

SessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

# ======================
# BOT
# ======================
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# ======================
# SPAM CONTROL
# ======================
SPAM_LIMIT = 3
SPAM_WINDOW = 5
user_spam = {}

def is_spam(chat_id, user_id):
    now = time.time()
    key = (chat_id, user_id)

    arr = user_spam.get(key, [])
    arr = [t for t in arr if now - t < SPAM_WINDOW]

    arr.append(now)
    user_spam[key] = arr

    return len(arr) > SPAM_LIMIT

def cleanup_memory():
    now = time.time()
    for k in list(user_spam.keys()):
        user_spam[k] = [t for t in user_spam[k] if now - t < 60]
        if not user_spam[k]:
            user_spam.pop(k)

# ======================
# MODEL
# ======================
class Keyword(Base):
    __tablename__ = "keywords"
    id = Column(Integer, primary_key=True)
    key = Column(String)
    text = Column(Text)

keyword_cache = []

# ======================
# CACHE
# ======================
async def load_cache():
    global keyword_cache
    async with SessionLocal() as db:
        rows = (await db.execute(select(Keyword))).scalars().all()

    keyword_cache = [{"key": r.key.lower(), "text": r.text} for r in rows]

# ======================
# HANDLER
# ======================
@dp.message()
async def handle_msg(m: types.Message):
    if not m.text:
        return

    if is_spam(m.chat.id, m.from_user.id):
        return

    text = m.text.lower()

    for k in keyword_cache:
        if k["key"] in text:
            await m.answer(k["text"])
            break

# ======================
# AUTO WORKER
# ======================
async def auto_worker():
    while True:
        try:
            await load_cache()
            cleanup_memory()
        except Exception as e:
            logging.error("Worker error: %s", e)

        await asyncio.sleep(20)

# ======================
# WEBHOOK
# ======================
async def webhook_handler(request):
    try:
        if request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
            return web.Response(status=403)

        data = await request.json()
        update = types.Update(**data)

        asyncio.create_task(dp.feed_update(bot, update))

        return web.Response(text="ok")

    except Exception as e:
        logging.exception("Webhook error")
        return web.Response(status=500)

# ======================
# HEALTH
# ======================
async def health(request):
    return web.Response(text="OK")

# ======================
# STARTUP
# ======================
async def on_startup(app):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    await load_cache()

    webhook_url = f"https://{BASE_URL}/webhook"

    await bot.set_webhook(webhook_url, secret_token=WEBHOOK_SECRET)

    asyncio.create_task(auto_worker())

    logging.info("🚀 BOT READY")

# ======================
# SHUTDOWN
# ======================
async def on_shutdown(app):
    await bot.delete_webhook()
    await bot.session.close()

# ======================
# APP
# ======================
def create_app():
    app = web.Application()
    app.router.add_post("/webhook", webhook_handler)
    app.router.add_get("/", health)

    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)

    return app

# ======================
# MAIN
# ======================
if __name__ == "__main__":
    app = create_app()
    web.run_app(app, host="0.0.0.0", port=PORT)
