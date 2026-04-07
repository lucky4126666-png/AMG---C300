# =========================================
# 🚀 TELEGRAM SAAS PRO - FINAL STABLE
# =========================================

import os, time, asyncio, logging, jwt
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, WebSocket, Depends, Header, HTTPException
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, Integer, String, Text, select
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
        yield db

# ================= MODELS =================
class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    username = Column(String, unique=True)
    password = Column(String)

class Group(Base):
    __tablename__ = "groups"
    id = Column(Integer, primary_key=True)
    chat_id = Column(String)
    title = Column(String)

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

class Welcome(Base):
    __tablename__ = "welcome"
    id = Column(Integer, primary_key=True)
    chat_id = Column(String)
    text = Column(Text)

# ================= AUTH =================
pwd = CryptContext(schemes=["bcrypt"], deprecated="auto")

def hash_pw(p): return pwd.hash(p)
def verify_pw(p, h): return pwd.verify(p, h)

def create_token(uid):
    return jwt.encode({"uid": uid, "exp": time.time()+86400}, SECRET_KEY, algorithm="HS256")

def decode_token(t):
    return jwt.decode(t, SECRET_KEY, algorithms=["HS256"])

async def get_user(authorization: str = Header(None)):
    try:
        token = authorization.split(" ")[1]
        return decode_token(token)["uid"]
    except:
        raise HTTPException(401, "Invalid token")

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

# ================= AUTH API =================
@app.post("/register")
async def register(data: dict):
    async with get_db() as db:
        db.add(User(username=data["username"], password=hash_pw(data["password"])))
        await db.commit()
    return {"ok": True}

@app.post("/login")
async def login(data: dict):
    async with get_db() as db:
        u = (await db.execute(
            select(User).where(User.username == data["username"])
        )).scalars().first()

    if not u or not verify_pw(data["password"], u.password):
        return {"error": "login fail"}

    return {"token": create_token(u.id)}

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

# ================= KEYWORD =================
@dp.message(F.text)
async def keyword(m: types.Message):
    async with get_db() as db:
        row = (await db.execute(
            select(Keyword).where(Keyword.key.ilike(f"%{m.text}%"))
        )).scalars().first()

    if row:
        await m.answer(row.text)

# ================= WELCOME =================
@dp.chat_member()
async def welcome(event: types.ChatMemberUpdated):
    if event.new_chat_member.status != "member":
        return

    chat_id = str(event.chat.id)

    async with get_db() as db:
        row = (await db.execute(
            select(Welcome).where(Welcome.chat_id == chat_id)
        )).scalars().first()

    if row:
        await bot.send_message(chat_id, row.text)

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

                    await broadcast({
                        "type": "auto_post",
                        "chat_id": p.chat_id,
                        "text": p.text
                    })

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
