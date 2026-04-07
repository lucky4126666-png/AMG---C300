# =============================
# FULL SAAS TELEGRAM BOT (FINAL)
# AUTO POST UI + MULTI LANGUAGE (CN + VI)
# =============================

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

class UserLang(Base):
    __tablename__ = "user_lang"

    user_id = Column(Integer, primary_key=True)
    lang = Column(String, default="vi")  # vi / cn

# ================= BOT =================
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
app = FastAPI()

user_state = {}
stop_event = asyncio.Event()

# ================= LANG =================
def t(uid, vi, cn):
    lang = user_state.get(uid, {}).get("lang", "vi")
    return cn if lang == "cn" else vi

# ================= STATE =================
def set_state(uid, k, v):
    user_state.setdefault(uid, {})[k] = v

def get_state(uid, k):
    return user_state.get(uid, {}).get(k)

def reset(uid):
    user_state.pop(uid, None)

# ================= MENU =================
def admin_menu(uid):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚙️ Auto Post", callback_data="auto_menu")],
        [InlineKeyboardButton(text="🌐 Language", callback_data="lang_menu")]
    ])

# ================= START =================
@dp.message(CommandStart())
async def start(m: types.Message):
    await m.answer("👑 Admin Panel", reply_markup=admin_menu(m.from_user.id))

# ================= LANGUAGE =================
@dp.callback_query(F.data == "lang_menu")
async def lang_menu(c: types.CallbackQuery):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🇻🇳 Tiếng Việt", callback_data="lang_vi")],
        [InlineKeyboardButton(text="🇨🇳 中文", callback_data="lang_cn")]
    ])
    await c.message.answer("Chọn ngôn ngữ / 选择语言", reply_markup=kb)
    await c.answer()

@dp.callback_query(F.data.startswith("lang_"))
async def set_lang(c: types.CallbackQuery):
    lang = c.data.split("_")[1]
    user_state.setdefault(c.from_user.id, {})["lang"] = lang
    await c.answer("✅ OK")

# ================= AUTO MENU =================
@dp.callback_query(F.data == "auto_menu")
async def auto_menu(c: types.CallbackQuery):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Add", callback_data="auto_add")],
        [InlineKeyboardButton(text="📋 List", callback_data="auto_list")],
    ])
    await c.message.answer("Auto Post", reply_markup=kb)
    await c.answer()

# ================= ADD =================
@dp.callback_query(F.data == "auto_add")
async def auto_add(c: types.CallbackQuery):
    set_state(c.from_user.id, "step", "auto_text")
    await c.message.answer("Nhập nội dung / 输入内容")
    await c.answer()

@dp.message(F.text)
async def auto_flow(m: types.Message):
    uid = m.from_user.id
    step = get_state(uid, "step")

    if step == "auto_text":
        set_state(uid, "auto_text", m.text)
        set_state(uid, "step", "auto_interval")
        return await m.answer("Interval (minutes):")

    if step == "auto_interval":
        try:
            interval = int(m.text)
        except:
            return await m.answer("Invalid number")

        text = get_state(uid, "auto_text")

        async with SessionLocal() as db:
            db.add(AutoPost(chat_id=str(m.chat.id), text=text, interval=interval))
            await db.commit()

        reset(uid)
        return await m.answer("✅ Created")

# ================= LIST =================
@dp.callback_query(F.data == "auto_list")
async def auto_list(c: types.CallbackQuery):
    async with SessionLocal() as db:
        rows = (await db.execute(select(AutoPost))).scalars().all()

    kb = []
    for p in rows:
        kb.append([
            InlineKeyboardButton(text=f"{p.id} {'✅' if p.active else '❌'}", callback_data=f"auto_toggle:{p.id}"),
            InlineKeyboardButton(text="🗑", callback_data=f"auto_del:{p.id}")
        ])

    await c.message.answer("List", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    await c.answer()

# ================= TOGGLE =================
@dp.callback_query(F.data.startswith("auto_toggle:"))
async def auto_toggle(c: types.CallbackQuery):
    pid = int(c.data.split(":")[1])

    async with SessionLocal() as db:
        row = await db.get(AutoPost, pid)
        row.active = 0 if row.active else 1
        await db.commit()

    await c.answer("Updated")

# ================= DELETE =================
@dp.callback_query(F.data.startswith("auto_del:"))
async def auto_del(c: types.CallbackQuery):
    pid = int(c.data.split(":")[1])

    async with SessionLocal() as db:
        row = await db.get(AutoPost, pid)
        await db.delete(row)
        await db.commit()

    await c.answer("Deleted")

# ================= WORKER =================
async def auto_worker():
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

# ================= WEBHOOK =================
webhook_tasks = set()
MAX_TASKS = 100

@app.post("/webhook")
async def webhook(req: Request):
    data = await req.json()
    update = types.Update.model_validate(data)

    if len(webhook_tasks) > MAX_TASKS:
        return {"ok": True}

    async def process():
        try:
            await dp.feed_update(bot, update)
        except Exception:
            logging.exception("Update error")

    task = asyncio.create_task(process())
    webhook_tasks.add(task)
    task.add_done_callback(lambda t: webhook_tasks.discard(t))

    return {"ok": True}


@app.get("/")
async def root():
    return {"status": "running"}


# ================= STARTUP =================
@app.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    asyncio.create_task(auto_worker())

    webhook_url = f"{BASE_URL}/webhook"

    await bot.delete_webhook(drop_pending_updates=True)
    await bot.set_webhook(webhook_url)

    logging.info(f"Webhook set: {webhook_url}")


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
    uvicorn.run(app, host="0.0.0.0", port=8000)
