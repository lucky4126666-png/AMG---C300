import os, asyncio, time, contextlib
from fastapi import FastAPI, Request
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, Text, select, delete
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
BASE_URL = os.getenv("BASE_URL")
DATABASE_URL = os.getenv("DATABASE_URL")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
app = FastAPI()

engine = create_async_engine(DATABASE_URL, echo=False)
SessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

# ======================
# STATE
# ======================
user_state = {}
temp = {}
last_sent = {}

def reset(uid):
    user_state.pop(uid, None)
    temp.pop(uid, None)

# ======================
# BUTTON
# ======================
def parse_buttons(text):
    if not text:
        return None
    rows = []
    for line in text.split("\n"):
        row = []
        for part in line.split("&&"):
            if "-" in part:
                t, u = part.split("-", 1)
                row.append({"text": t.strip(), "url": u.strip()})
        if row:
            rows.append(row)
    return rows

def build_buttons(data):
    if not data:
        return None
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=b["text"], url=b["url"]) for b in row]
        for row in data
    ])

# ======================
# MODELS
# ======================
class Keyword(Base):
    __tablename__ = "keywords"
    id = Column(Integer, primary_key=True)
    key = Column(String, unique=True)
    text = Column(Text)
    image = Column(Text)
    button = Column(Text)

class AutoPost(Base):
    __tablename__ = "auto_post"
    id = Column(Integer, primary_key=True)
    chat_id = Column(String)
    text = Column(Text)
    image = Column(Text)
    button = Column(Text)
    interval = Column(Integer, default=10)
    is_active = Column(Integer, default=0)
    pin = Column(Integer, default=0)

# ======================
# MENU
# ======================
def home():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📌 Từ khoá", callback_data="kw_menu")],
        [InlineKeyboardButton(text="📅 Auto Post", callback_data="auto_menu")]
    ])

# ======================
# HELPERS
# ======================
async def send_preview(chat_id, text=None, image=None, button=None):
    kb = build_buttons(parse_buttons(button))
    if image:
        return await bot.send_photo(chat_id, image, caption=text or "", reply_markup=kb)
    return await bot.send_message(chat_id, text or " ", reply_markup=kb)

def get_image(m: types.Message):
    if m.photo:
        return m.photo[-1].file_id
    if m.text:
        return m.text.strip()
    return None

# ======================
# START
# ======================
@dp.message(F.text == "/start")
async def start(m: types.Message):
    reset(m.from_user.id)
    await m.answer("🚀 Menu", reply_markup=home())

@dp.callback_query(F.data == "home")
async def go_home(c: types.CallbackQuery):
    await c.answer()
    await c.message.edit_text("🚀 Menu", reply_markup=home())

# ======================
# KEYWORD AUTO REPLY
# ======================
@dp.message()
async def all_msg(m: types.Message):
    uid = m.from_user.id
    text = (m.text or "").strip().lower()

    if not text or text.startswith("/"):
        return

    async with SessionLocal() as db:
        kws = (await db.execute(select(Keyword))).scalars().all()

    for k in kws:
        if k.key.lower() == text:
            return await send_preview(
                m.chat.id,
                k.text,
                k.image,
                k.button
            )

# ======================
# AUTO WORKER (FIX INTERVAL)
# ======================
async def auto_worker():
    while True:
        now = time.time()

        async with SessionLocal() as db:
            posts = (await db.execute(
                select(AutoPost).where(AutoPost.is_active == 1)
            )).scalars().all()

        for p in posts:
            interval = max(p.interval, 1) * 60
            last = last_sent.get(p.id, 0)

            if now - last < interval:
                continue

            try:
                msg = await send_preview(
                    p.chat_id,
                    p.text,
                    p.image,
                    p.button
                )

                last_sent[p.id] = now

                if p.pin:
                    try:
                        await bot.pin_chat_message(p.chat_id, msg.message_id)
                    except:
                        pass

            except Exception as e:
                print("Auto lỗi:", e)

        await asyncio.sleep(10)

# ======================
# WEBHOOK
# ======================
@app.post("/webhook")
async def webhook(req: Request):
    data = await req.json()
    update = types.Update.model_validate(data)  # FIX
    await dp.feed_update(bot, update)
    return {"ok": True}

@app.get("/")
async def root():
    return {"status": "ok"}

# ======================
# STARTUP
# ======================
@app.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    asyncio.create_task(auto_worker())

    await bot.delete_webhook(drop_pending_updates=True)
    await bot.set_webhook(f"{BASE_URL}/webhook")

    print("READY")

@app.on_event("shutdown")
async def shutdown():
    with contextlib.suppress(Exception):
        await bot.delete_webhook()
    await bot.session.close()
