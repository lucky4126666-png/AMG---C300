import os
import re
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, FileResponse
from aiogram import Bot, Dispatcher, types
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.client.default import DefaultBotProperties
from dotenv import load_dotenv
import uvicorn

from db import (
    init_db,
    get_setting,
    set_setting,
    get_admin,
    add_admin,
    remove_admin,
    get_logs,
    log_action,
    get_all_admins,
    save_group,
    get_groups
)
from ai_service import ask_ai

# ===== LOAD ENV =====
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
BASE_URL = os.getenv("BASE_URL")
PORT = int(os.getenv("PORT", 8080))

# ===== BOT =====
bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(link_preview_is_disabled=True)
)

dp = Dispatcher(storage=MemoryStorage())

# ===== FASTAPI =====
app = FastAPI()

# ===== INIT DB =====
init_db()

# ================= HELPER =================
async def is_allowed(chat_id, user_id):
    role = get_admin(user_id)
    if role == "super":
        return True

    admins = await bot.get_chat_administrators(chat_id)
    return any(a.user.id == user_id for a in admins)
    
# ================= START =================
@dp.message(lambda m: m.text == "/start")
async def start(m: types.Message):
    if m.chat.type != "private":
        return

    text = get_setting("start_text") or (
        "点击此处可以添加机器人进群\n"
        "http://t.me/xbqgk?startgroup=foo\n\n"
        "更多服务，请访问 https://t.me/xbkf/"
    )

    await m.answer(text)

# ================= BOT JOIN =================
@dp.my_chat_member()
async def bot_join(e: types.ChatMemberUpdated):
    if e.new_chat_member.status in ("member", "administrator"):
        chat_id = e.chat.id

        save_group(chat_id, e.chat.title)

        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="公群导航", url="https://t.me/xbkf"),
            InlineKeyboardButton(text="供需频道", url="https://t.me/xbkf")
        ]])

        await bot.send_message(
            chat_id,
            "组防骗助手为您服务,我正在进行相关初始化配置请稍后",
            reply_markup=kb
        )

        admins = await bot.get_chat_administrators(chat_id)
        ids = [a.user.id for a in admins]

        if not any(get_admin(i) for i in ids):
            await bot.send_message(
                chat_id,
                "⚠️ 风险提示，本群没有检测到新币管理员。\n有交易风险，请联系 @xbkf"
            )
            
# ================= USER JOIN =================
@dp.message(lambda m: m.new_chat_members)
async def welcome(m: types.Message):
    chat = m.chat
    group_name = chat.title or "本群"

    for u in m.new_chat_members:
        name = u.full_name
        
        text = (
            f"欢迎 {name} 来到\n"
            f"{group_name}\n\n"
            "交易前请先关注，担保流程【 @xinb 】\n\n"
            "1.交易前认准群老板和业务员头衔，先看清楚置顶的群规则和报备模版；\n"
            "2.交易前群老板方必须在公群内进行报备，客户确认报备内容，如客户没确认此报备视为无效报备；\n"
            "3.交易过程中有任何变动需要在群内保留记录或者重新报备；\n"
            "4.有任何问题可以联系新币24小时客服 @xbkf\n\n"
            "⚠️注意：主动私聊你的都是骗子！\n"
            "新币所有群（纠纷群、作业群、公群、专群）都由新币担保靓号拉群，\n"
            "一切交易必须群内进行,切勿私下交易,请按照担保流程进行交易。\n\n"
            "此用户是新币尊贵的VIP成员"
        )

        kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="新币供需", url="https://t.me/xbkf"),
                InlineKeyboardButton(text="新币公群", url="https://t.me/xbkf")
            ]
        ])

        await m.answer(text, reply_markup=kb)
        
# ================= LOCK =================
@dp.message(lambda m: m.text in ["/lock", "下课"])
async def lock_group(m: types.Message):
    if not await is_allowed(m.chat.id, m.from_user.id):
        return await m.reply("❌ 无权限")

    await bot.set_chat_permissions(
        m.chat.id,
        permissions=types.ChatPermissions(can_send_messages=False)
    )

    log_action(m.from_user.id, "lock", m.chat.id)

    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="新币供需", url="https://t.me/xbkf"),
        InlineKeyboardButton(text="新币公群", url="https://t.me/xbkf")
    ]])

    await m.answer(
        "本公群已下课关闭发言\n如需交易，请恢复后操作！",
        reply_markup=kb
    )

# ================= OPEN =================
@dp.message(lambda m: m.text in ["/open", "上课"])
async def open_group(m: types.Message):
    if not await is_allowed(m.chat.id, m.from_user.id):
        return await m.reply("❌ 无权限")

    await bot.set_chat_permissions(
        m.chat.id,
        permissions=types.ChatPermissions(can_send_messages=True)
    )

    log_action(m.from_user.id, "open", m.chat.id)

    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="新币供需", url="https://t.me/xbkf"),
        InlineKeyboardButton(text="新币公群", url="https://t.me/xbkf")
    ]])

    await m.answer(
        "本群已开启发言，可以正常作业",
        reply_markup=kb
    )
# ================= RENAME GROUP (担保表单) =================
@dp.message(lambda m: "担保表单" in (m.text or ""))
async def rename_group(m: types.Message):
    if not await is_allowed(m.chat.id, m.from_user.id):
        return await m.reply("❌ 无权限")

    text = m.text.replace("\n", " ")

    def find(pattern):
        r = re.search(pattern, text)
        return r.group(1).strip() if r else ""

    group = find(r"组别[:：]\s*([^\s]+)")
    name = find(r"名字[:：]\s*(.+?)\s*编号")
    number = find(r"编号[:：]\s*(\d+)")
    rule = find(r"规则[:：]\s*([^\s]+)")

    # fallback nếu format lệch
    if not name:
        name = find(r"名字[:：]\s*(.+)")

    if not group or not name or not number or not rule:
        return await m.reply("❌ 表单解析失败，请检查格式")

    # ===== BUILD TÊN =====
    new_title = f"{group}{number}-{rule}{name}"

    try:
        # đổi tên nhóm
        await bot.set_chat_title(m.chat.id, new_title)

        # thông báo giống bot Trung
        await m.answer(
            f"担保规则写入成功\n{new_title}"
        )

    except:
        await m.reply("❌ 修改群名失败，请检查机器人权限")
        
# ================= ANTI SCAM =================
@dp.message()
async def anti(m: types.Message):
    if not m.text:
        return

    if re.search(r"(http|t.me|www|\.com)", m.text.lower()):
        try:
            await m.delete()
        except:
            pass

# ================= AI =================
@dp.message()
async def ai(m: types.Message):
    if not m.text:
        return

    if m.chat.type != "private" and "ai" not in m.text.lower():
        return

    reply = await ask_ai(m.from_user.id, m.text.replace("ai ", ""))
    await m.reply(reply)

# ================= DASHBOARD =================
@app.get("/dashboard")
def dashboard():
    return FileResponse("frontend/index.html")

@app.get("/admin/list")
async def admin_list():
    return {"data": get_all_admins()}

@app.get("/admin/logs")
async def logs():
    return {"data": get_logs()}

@app.get("/admin/groups")
async def groups():
    return {"data": get_groups()}

# ================= WEBHOOK =================
@app.on_event("startup")
async def startup():
    await bot.delete_webhook(drop_pending_updates=True)
    await bot.set_webhook(BASE_URL + "/webhook")

@app.post("/webhook")
async def webhook(req: Request):
    data = await req.json()
    update = types.Update.model_validate(data)
    await dp.feed_update(bot, update)
    return {"ok": True}

@app.get("/")
def home():
    return {"status": "running"}

# ================= RUN =================
if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=PORT)
