import os
import time
import asyncio
import logging
import jwt

from datetime import datetime, timedelta
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, WebSocket, Depends, Header
from fastapi.middleware.cors import CORSMiddleware

from aiogram import Bot, Dispatcher, types
from aiogram.filters import CommandStart

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
SECRET = os.getenv("SECRET", "SUPER_SECRET")

if not BOT_TOKEN or not BASE_URL or not DATABASE_URL:
    raise RuntimeError("Missing ENV")

BASE_URL = BASE_URL.rstrip("/")

# ================= DB =================
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)
elif DATABASE_URL.startswith("postgresql://") and "+asyncpg" not in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

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

# ================= DB HELPER =================
@asynccontextmanager
async def get_db():
    async with SessionLocal() as session:
        try:
            yield session
        except:
            await session.rollback()
            raise
        finally:
            await session.close()

# ================= MODELS =================
class AutoPost(Base):
    __tablename__ = "auto_post"
    id = Column(Integer, primary_key=True)
    chat_id = Column(String)
    text = Column(Text)
    interval = Column(Integer, default=60)
    last_sent = Column(Integer, default=0)
    active = Column(Integer, default=1)

class Keyword(Base):
    __tablename__ = "keyword"
    id = Column(Integer, primary_key=True)
    key = Column(String)
    text = Column(Text)

class Welcome(Base):
    __tablename__ = "welcome"
    id = Column(Integer, primary_key=True)
    chat_id = Column(String)
    text = Column(Text)
    enabled = Column(Integer, default=1)

# ================= AUTH =================
def create_token(uid: int):
    return jwt.encode({"uid": uid, "exp": datetime.utcnow() + timedelta(days=7)}, SECRET, algorithm="HS256")

def verify_token(token: str):
    try:
        return jwt.decode(token, SECRET, algorithms=["HS256"])
    except:
        return None

async def get_user(auth: str = Header(None)):
    if not auth:
        return None
    return verify_token(auth.replace("Bearer ", ""))

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
webhook_tasks = set()
connections = []

# ================= START =================
@dp.message(CommandStart())
async def admin_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚙️ Auto Post", callback_data="auto_menu")],
        [InlineKeyboardButton(text="🔑 Keyword", callback_data="kw_menu")],
    ])


@dp.message(CommandStart())
async def start(m: types.Message):
    await m.answer(
        "👑 ADMIN PANEL",
        reply_markup=admin_menu()
    )
    
# ================= KEYWORD =================
@dp.message()
async def keyword_handler(m: types.Message):
    if not m.text:
        return

    async with get_db() as db:
        row = (await db.execute(
            select(Keyword).where(Keyword.key.ilike(f"%{m.text}%"))
        )).scalars().first()

    if row:
        await m.answer(row.text)

# ================= AUTO WORKER =================
async def auto_worker():
    while not stop_event.is_set():
        now = int(time.time())

        async with get_db() as db:
            rows = (await db.execute(select(AutoPost))).scalars().all()

        for p in rows:
            if p.active and now - (p.last_sent or 0) >= p.interval * 60:
                try:
                    await asyncio.wait_for(bot.send_message(p.chat_id, p.text), timeout=10)

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

    user = event.new_chat_member.user
    chat_id = str(event.chat.id)

    async with get_db() as db:
        row = (await db.execute(select(Welcome).where(Welcome.chat_id == chat_id))).scalars().first()

    if row and row.enabled:
        await bot.send_message(chat_id, row.text.replace("{name}", user.full_name))

# ================= API =================
@app.post("/api/login")
async def login(data: dict):
    if int(data.get("user_id")) != OWNER_ID:
        return {"error": "unauthorized"}
    return {"token": create_token(OWNER_ID)}

@app.get("/api/auto")
async def get_auto():
    async with get_db() as db:
        rows = (await db.execute(select(AutoPost))).scalars().all()
    return [r.__dict__ for r in rows]

@app.post("/api/auto")
async def create_auto(data: dict, user=Depends(get_user)):
    if not user:
        return {"error": "unauthorized"}

    async with get_db() as db:
        db.add(AutoPost(chat_id=data["chat_id"], text=data["text"], interval=int(data["interval"])))
        await db.commit()

    for c in connections:
        await c.send_json({"event": "reload"})

    return {"ok": True}

@app.post("/api/auto/toggle/{id}")
async def toggle_auto(id: int):
    async with get_db() as db:
        row = await db.get(AutoPost, id)
        row.active = 0 if row.active else 1
        await db.commit()
    return {"ok": True}

@app.delete("/api/auto/{id}")
async def delete_auto(id: int):
    async with get_db() as db:
        row = await db.get(AutoPost, id)
        await db.delete(row)
        await db.commit()
    return {"ok": True}

# ================= WEBSOCKET =================
@app.websocket("/ws")
async def ws(ws: WebSocket):
    await ws.accept()
    connections.append(ws)
    try:
        while True:
            await ws.receive_text()
    except:
        connections.remove(ws)

# ================= WEBHOOK =================
async def process_update(update):
    try:
        await asyncio.wait_for(dp.feed_update(bot, update), timeout=10)
    except:
        logging.exception("Update error")

@app.post("/webhook")
async def webhook(req: Request):
    update = types.Update.model_validate(await req.json())

    task = asyncio.create_task(process_update(update))
    webhook_tasks.add(task)
    task.add_done_callback(lambda t: webhook_tasks.discard(t))

    return {"ok": True}

@app.get("/")
async def root():
    return {"ok": True}

# ================= STARTUP =================
async def wait_for_db():
    for _ in range(10):
        try:
            async with engine.begin() as conn:
                await conn.execute(sql_text("SELECT 1"))
            return
        except:
            await asyncio.sleep(2)

@app.on_event("startup")
async def startup():
    await wait_for_db()

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    await bot.delete_webhook(drop_pending_updates=True)
    await bot.set_webhook(f"{BASE_URL}/webhook")

    asyncio.create_task(auto_worker())

# ================= SHUTDOWN =================
@app.on_event("shutdown")
async def shutdown():
    stop_event.set()
    await bot.delete_webhook()
    await bot.session.close()

# ================= RUN =================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
