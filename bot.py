import os
import sys
import asyncio
import logging
import time
import re
import random
import traceback
from datetime import datetime

# استيراد المكتبات
from bson.objectid import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient
from telethon import TelegramClient, events, Button, functions, types
from telethon.sessions import StringSession
from telethon.tl.types import UserStatusOnline, UserStatusRecently
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.tl.functions.channels import JoinChannelRequest, LeaveChannelRequest
from telethon.errors import FloodWaitError
from telethon.extensions import html
from aiohttp import web
from openai import AsyncOpenAI
from dotenv import load_dotenv

# ==============================================================================
#                               1. إعدادات النظام
# ==============================================================================
load_dotenv()
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger("SaudiMerchantBot_MultiAccount_Fixed")

API_ID = int(os.getenv("API_ID") or 0)
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
MONGO_URI = os.getenv("MONGO_URI")
SAMBANOVA_API_KEY = os.getenv("SAMBANOVA_API_KEY", "key")

# إنشاء مجلد الوسائط المؤقتة
os.makedirs("temp_media", exist_ok=True)

if not all([API_ID, API_HASH, BOT_TOKEN, MONGO_URI]):
    print("⚠️ خطأ: بعض المتغيرات البيئية مفقودة (API_ID, API_HASH, BOT_TOKEN, MONGO_URI)")
    sys.exit(1)

try:
    ai_client = AsyncOpenAI(base_url="https://api.sambanova.ai/v1", api_key=SAMBANOVA_API_KEY)
    AI_MODEL = "Meta-Llama-3.1-405B-Instruct"
except:
    ai_client = None

STRICT_RULE = "أنت تاجر سعودي محترف."

# ==============================================================================
#                               2. الذاكرة (معزولة لكل مستخدم)
# ==============================================================================
active_userbot_clients = {}
user_autopost_tasks = {}
user_current_state = {}
temporary_autopost_config = {}
temporary_task_data = {}
reply_cooldown_timestamps = {}
last_published_message_ids = {}

# ==============================================================================
#                               3. قاعدة البيانات
# ==============================================================================
try:
    mongo_client = AsyncIOMotorClient(MONGO_URI)
    database = mongo_client['MyTelegramBotDB']

    sessions_collection = database['sessions']
    replies_collection = database['replies']
    ai_settings_collection = database['ai_prompts']
    autopost_config_collection = database['autopost_config']
    paused_groups_collection = database['paused_groups']
    admins_watch_collection = database['admins_watch']
    subscriptions_collection = database['subscriptions']

    print("✅ DB Connected & Ready")
except Exception as e:
    print(f"❌ DB Connection Error: {e}")
    sys.exit(1)

# ==============================================================================
#                               4. الخادم
# ==============================================================================
bot_client = TelegramClient('bot_session', API_ID, API_HASH)


async def web_request_handler(request):
    return web.Response(text=f"Bot Running. Active Accounts: {len(active_userbot_clients)}")


async def start_web_server():
    app = web.Application()
    app.router.add_get('/', web_request_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    print(f"✅ Web server started successfully on port {port}")


async def get_ai_response(messages_list):
    if not ai_client:
        return None
    try:
        response = await ai_client.chat.completions.create(
            model=AI_MODEL, messages=messages_list, temperature=0.7
        )
        return response.choices[0].message.content
    except:
        return None


# ==============================================================================
#                               5. إدارة اليوزربوت
# ==============================================================================

async def start_userbot_session(owner_id, session_string):
    try:
        if owner_id in active_userbot_clients:
            await active_userbot_clients[owner_id].disconnect()
            del active_userbot_clients[owner_id]

        userbot = TelegramClient(StringSession(session_string), API_ID, API_HASH)
        await userbot.connect()

        if not await userbot.is_user_authorized():
            return False

        userbot.owner_id = owner_id
        userbot.cooldowns = {}

        userbot.add_event_handler(lambda e: handle_auto_reply(userbot, e), events.NewMessage(incoming=True))
        userbot.add_event_handler(lambda e: handle_ai_chat(userbot, e), events.NewMessage(incoming=True))
        userbot.add_event_handler(lambda e: handle_safe_forced_join(userbot, e), events.NewMessage(incoming=True))
        userbot.add_event_handler(lambda e: handle_admin_freeze_trigger(userbot, e), events.NewMessage(incoming=True))
        userbot.add_event_handler(lambda e: handle_owner_resume_trigger(userbot, e), events.NewMessage(outgoing=True))

        active_userbot_clients[owner_id] = userbot

        await manage_user_autopost_task(userbot, owner_id)
        asyncio.create_task(engine_auto_leave_channels(userbot, owner_id))

        return True
    except Exception as e:
        print(f"Error starting {owner_id}: {e}")
        return False


async def load_all_sessions_from_db():
    async for document in sessions_collection.find({}):
        asyncio.create_task(start_userbot_session(document['_id'], document['session_string']))


async def manage_user_autopost_task(client, owner_id):
    if owner_id in user_autopost_tasks:
        old_task = user_autopost_tasks[owner_id]
        if not old_task.done():
            old_task.cancel()
            try:
                await old_task
            except asyncio.CancelledError:
                pass
        del user_autopost_tasks[owner_id]

    config = await autopost_config_collection.find_one({"owner_id": owner_id})

    if config and config.get('active', False):
        new_task = asyncio.create_task(engine_autopost_loop(client, owner_id))
        user_autopost_tasks[owner_id] = new_task
        print(f"✅ Started AutoPost Task for User: {owner_id}")


# ==============================================================================
#                               6. المعالجات (Logic)
# ==============================================================================

async def handle_auto_reply(client, event):
    if not (event.is_private or event.is_group):
        return
    try:
        user_text = event.raw_text or ""
        cursor = replies_collection.find({"owner_id": client.owner_id})
        async for reply_doc in cursor:
            if reply_doc['keyword'] in user_text:
                cooldown_key = (client.owner_id, event.chat_id, event.sender_id, reply_doc['keyword'])
                last_time = reply_cooldown_timestamps.get(cooldown_key, 0)
                if time.time() - last_time < 600:
                    return
                reply_cooldown_timestamps[cooldown_key] = time.time()
                await event.reply(reply_doc['reply'])
                return
    except:
        pass


async def handle_ai_chat(client, event):
    if not event.is_private:
        return
    try:
        settings = await ai_settings_collection.find_one({"owner_id": client.owner_id})
        if settings and settings.get('active'):
            if time.time() - client.cooldowns.get(event.chat_id, 0) > 5:
                async with client.action(event.chat_id, 'typing'):
                    await asyncio.sleep(2)
                msgs = [{"role": "system", "content": STRICT_RULE}, {"role": "user", "content": event.raw_text}]
                ai_reply = await get_ai_response(msgs)
                if ai_reply:
                    await event.reply(ai_reply)
                client.cooldowns[event.chat_id] = time.time()
    except:
        pass


async def handle_safe_forced_join(client, event):
    try:
        if not (event.is_reply or event.mentioned):
            return
        reply_message = await event.get_reply_message()
        my_info = await client.get_me()
        if reply_message and reply_message.sender_id != my_info.id:
            return

        text_content = event.raw_text.lower()
        forced_keywords = ["لايمكنك", "عليك الاشتراك", "must join", "غير مشترك", "join channel", "القناة"]

        if any(keyword in text_content for keyword in forced_keywords):
            targets_to_join = re.findall(r'(https?://t\.me/[^\s]+|@[a-zA-Z0-9_]{4,})', event.raw_text)
            if event.message.buttons:
                for row in event.message.buttons:
                    for btn in row:
                        if hasattr(btn, 'url') and btn.url and "t.me" in btn.url:
                            targets_to_join.append(btn.url)

            for target_link in targets_to_join:
                try:
                    clean_link = target_link.replace("https://t.me/", "").replace("@", "").strip()
                    if "+" in clean_link:
                        await client(ImportChatInviteRequest(clean_link.split("+")[-1]))
                    else:
                        await client(JoinChannelRequest(clean_link))

                    try:
                        entity = await client.get_entity(clean_link)
                        chat_id_to_save = entity.id
                    except:
                        chat_id_to_save = clean_link

                    await subscriptions_collection.update_one(
                        {"owner_id": client.owner_id, "chat_id": chat_id_to_save},
                        {"$set": {"join_time": time.time()}}, upsert=True
                    )
                except:
                    pass
    except:
        pass


async def handle_admin_freeze_trigger(client, event):
    if not (event.is_group and event.is_reply):
        return
    try:
        my_info = await client.get_me()
        if (await event.get_reply_message()).sender_id != my_info.id:
            return
        sender = await event.get_sender()
        perms = await client.get_permissions(event.chat_id, sender)
        if perms.is_admin or perms.is_creator:
            await paused_groups_collection.update_one(
                {"owner_id": client.owner_id, "chat_id": event.chat_id},
                {"$set": {"admin_id": sender.id}}, upsert=True
            )
            await client.send_message("me", f"⛔ توقف النشر في {event.chat.title} بسبب رد المشرف.")
    except:
        pass


async def handle_owner_resume_trigger(client, event):
    if not (event.is_group and event.is_reply):
        return
    try:
        paused_data = await paused_groups_collection.find_one(
            {"owner_id": client.owner_id, "chat_id": event.chat_id}
        )
        if not paused_data:
            return
        replied_to_msg = await event.get_reply_message()
        if replied_to_msg.sender_id == paused_data['admin_id']:
            await paused_groups_collection.delete_one({"_id": paused_data['_id']})
            await client.send_message("me", f"✅ عاد النشر في {event.chat.title}")
    except:
        pass


# ==============================================================================
#                               7. المحركات الخلفية (Engines)
# ==============================================================================

async def engine_autopost_loop(client, owner_id):
    logging.info(f"STARTING ENGINE FOR: {owner_id}")
    while True:
        try:
            config = await autopost_config_collection.find_one({"owner_id": owner_id})
            if not config or not config.get('active', False):
                break

            for group_id in config.get('groups', []):

                if await paused_groups_collection.find_one({"owner_id": owner_id, "chat_id": group_id}):
                    continue

                # -----------------------------------------------
                # إرسال كل الصور مع بعض في رسالة واحدة
                # -----------------------------------------------
                media_files = config.get('media_files', [])
                existing_files = [f for f in media_files if os.path.exists(f)]
                # -----------------------------------------------

                # فحص رادار المشرفين
                is_danger = False
                async for admin_doc in admins_watch_collection.find({"owner_id": owner_id}):
                    try:
                        admin_entity = await client.get_entity(admin_doc['username'])
                        if isinstance(admin_entity.status, (UserStatusOnline, UserStatusRecently)):
                            is_danger = True
                            break
                    except:
                        pass

                if is_danger:
                    last_msg = last_published_message_ids.get(f"{owner_id}_{group_id}")
                    if last_msg:
                        try:
                            await client.delete_messages(group_id, [last_msg])
                        except:
                            pass
                    await asyncio.sleep(300)
                    continue

                try:
                    if existing_files:
                        # إرسال كل الصور مع النص في رسالة واحدة (album)
                        sent_messages = await client.send_message(
                            int(group_id),
                            config['message'],
                            file=existing_files,
                            parse_mode='html'
                        )
                        # حفظ id أول رسالة من الألبوم
                        first_msg = sent_messages[0] if isinstance(sent_messages, list) else sent_messages
                        last_published_message_ids[f"{owner_id}_{group_id}"] = first_msg.id
                    else:
                        # بدون صور — نص فقط
                        sent_message = await client.send_message(
                            int(group_id),
                            config['message'],
                            parse_mode='html'
                        )
                        last_published_message_ids[f"{owner_id}_{group_id}"] = sent_message.id
                    await asyncio.sleep(5)
                except FloodWaitError as f:
                    await asyncio.sleep(f.seconds)
                except Exception as e:
                    print(f"Send error {owner_id}: {e}")

            await asyncio.sleep(config.get('interval', 10) * 60)

        except asyncio.CancelledError:
            print(f"Task Cancelled for {owner_id}")
            break
        except Exception as e:
            print(f"Loop Error {owner_id}: {e}")
            await asyncio.sleep(60)


async def engine_auto_leave_channels(client, owner_id):
    while True:
        try:
            current_timestamp = time.time()
            async for sub in subscriptions_collection.find({"owner_id": owner_id}):
                if current_timestamp - sub['join_time'] > 86400:
                    try:
                        target_id = sub['chat_id']
                        try:
                            target_id = int(target_id)
                        except:
                            pass
                        await client(LeaveChannelRequest(target_id))
                        await subscriptions_collection.delete_one({"_id": sub['_id']})
                    except:
                        pass
        except:
            pass
        await asyncio.sleep(3600)


async def engine_broadcast_sender(client, status_message, message_event):
    count_sent = 0
    try:
        media_file = None
        if message_event.media:
            await status_message.edit("⏳ **جاري معالجة الوسائط...**")
            media_file = await message_event.download_media()

        text_content = html.unparse(message_event.raw_text, message_event.entities)

        await status_message.edit("🚀 **بدأ النشر...**")

        async for dialog in client.iter_dialogs():
            if dialog.is_user and not dialog.entity.bot:
                try:
                    if media_file:
                        await client.send_message(dialog.id, text_content, file=media_file, parse_mode='html')
                    else:
                        await client.send_message(dialog.id, text_content, parse_mode='html')
                    count_sent += 1
                    await asyncio.sleep(1)
                except:
                    pass

        if media_file and os.path.exists(media_file):
            os.remove(media_file)

    except:
        pass
    await status_message.edit(f"✅ **تم.**\nالمستلمين: `{count_sent}`")


async def engine_search_task(client, status_msg, hours, keyword, reply_msg_object, delay):
    count = 0
    limit_time = time.time() - (hours * 3600)
    replied_users = set()
    try:
        my_info = await client.get_me()

        reply_file = None
        if reply_msg_object.media:
            await status_msg.edit("⏳ **تحميل الميديا...**")
            reply_file = await reply_msg_object.download_media()

        reply_text = html.unparse(reply_msg_object.raw_text, reply_msg_object.entities)

        await status_msg.edit(f"🚀 **بدأ البحث...**")

        async for dialog in client.iter_dialogs():
            if dialog.is_group:
                try:
                    async for msg in client.iter_messages(dialog.id, search=keyword, limit=20):
                        if msg.date.timestamp() > limit_time and msg.sender_id != my_info.id:
                            if msg.sender_id in replied_users:
                                continue
                            try:
                                if reply_file:
                                    await client.send_message(
                                        dialog.id, reply_text, file=reply_file,
                                        reply_to=msg.id, parse_mode='html'
                                    )
                                else:
                                    await client.send_message(
                                        dialog.id, reply_text,
                                        reply_to=msg.id, parse_mode='html'
                                    )
                                replied_users.add(msg.sender_id)
                                count += 1
                                await asyncio.sleep(delay)
                            except:
                                pass
                except:
                    pass

        if reply_file:
            os.remove(reply_file)
    except:
        pass
    await status_msg.respond(f"✅ تم الرد على {count}")


# ==============================================================================
#                               8. واجهة المستخدم
# ==============================================================================

@bot_client.on(events.NewMessage(pattern='/start'))
async def start_handler(event):
    chat_id = event.chat_id
    if chat_id in active_userbot_clients:
        config = await autopost_config_collection.find_one({"owner_id": chat_id})
        status_post = "🟢" if config and config.get('active') else "🔴"

        buttons = [
            [Button.inline(f"📢 النشر التلقائي {status_post}", b"menu_autopost")],
            [Button.inline("📨 برودكاست (صور/نص)", b"broadcast_menu")],
            [Button.inline("📋 الردود", b"list_replies"), Button.inline("👮 الرادار", b"menu_radar")],
            [Button.inline("🚀 مهام بحث", b"menu_tasks"), Button.inline("🤖 ذكاء", b"toggle_ai")],
            [Button.inline("📊 إحصائيات", b"view_stats"), Button.inline("🗑️ تنظيف القنوات", b"clean_channels")]
        ]
        await event.respond("✅ **لوحة التحكم الكاملة**", buttons=buttons)
    else:
        await event.respond("🔒", buttons=[[Button.inline("تسجيل الدخول", b"login")]])


@bot_client.on(events.NewMessage(pattern='/cancel'))
async def cancel_handler(event):
    chat_id = event.chat_id
    user_current_state[chat_id] = None
    await event.respond("✅ **تم الإلغاء.**")


@bot_client.on(events.CallbackQuery)
async def callback_handler(event):
    chat_id = event.chat_id
    data = event.data
    client = active_userbot_clients.get(chat_id)

    if not client and data != b"login":
        await event.answer("⚠️ سجل دخولك أولاً!", alert=True)
        return

    await event.answer()

    # ----------------------------------------------------------------
    if data == b"login":
        user_current_state[chat_id] = "WAITING_SESSION"
        await event.respond("🔐 **أرسل كود الجلسة (Session String):**")

    elif data == b"clean_channels":
        await event.respond("🧹 **جاري العمل...**")
        asyncio.create_task(engine_auto_leave_channels(client, chat_id))

    # ----------------------------------------------------------------
    # قائمة النشر التلقائي
    # ----------------------------------------------------------------
    elif data == b"menu_autopost":
        conf = await autopost_config_collection.find_one({"owner_id": chat_id})
        media_count = len(conf.get('media_files', [])) if conf else 0
        status = "🟢 يعمل" if conf and conf.get('active') else "🔴 متوقف"
        btns = [
            [Button.inline("⚙️ إعداد جديد", b"setup_post")],
            [Button.inline("⏯️ تشغيل/إيقاف", b"toggle_post")],
            [Button.inline("🖼️ إدارة الصور", b"manage_media")],
            [Button.inline("🗑️ حذف الإعدادات", b"delete_autopost_settings")],
            [Button.inline("👁️ عرض المنشور", b"view_current_post")],
            [Button.inline("🔙 رجوع", b"back_home")]
        ]
        await event.respond(
            f"📢 **النشر التلقائي**\n"
            f"الحالة: {status}\n"
            f"📸 عدد الصور: `{media_count}` (تُرسل كلها معاً)",
            buttons=btns
        )

    elif data == b"view_current_post":
        conf = await autopost_config_collection.find_one({"owner_id": chat_id})
        if conf:
            await event.respond(f"📝 **الرسالة:**\n\n{conf['message']}", parse_mode='html')
        else:
            await event.respond("❌ لا توجد رسالة")

    elif data == b"delete_autopost_settings":
        await autopost_config_collection.delete_one({"owner_id": chat_id})
        await manage_user_autopost_task(client, chat_id)
        await event.respond("🗑️ **تم الحذف والإيقاف.**")

    # ----------------------------------------------------------------
    # إعداد النشر التلقائي — الخطوة 1: الرسالة
    # ----------------------------------------------------------------
    elif data == b"setup_post":
        user_current_state[chat_id] = "WAITING_POST_MSG"
        await event.respond(
            "📝 **الخطوة 1/3 — أرسل نص الرسالة:**\n"
            "(يمكنك استخدام تعبيرات Premium)"
        )

    # ----------------------------------------------------------------
    # إدارة الصور (إضافة / حذف / عرض)
    # ----------------------------------------------------------------
    elif data == b"manage_media":
        conf = await autopost_config_collection.find_one({"owner_id": chat_id})
        media_files = conf.get('media_files', []) if conf else []
        existing = [f for f in media_files if os.path.exists(f)]
        count = len(existing)
        btns = [
            [Button.inline(f"➕ إضافة صور ({count} محفوظة)", b"add_more_media")],
            [Button.inline("🗑️ حذف كل الصور", b"delete_all_media")],
            [Button.inline("🔙 رجوع", b"menu_autopost")]
        ]
        await event.respond(
            f"🖼️ **إدارة الصور**\n"
            f"الصور الموجودة: `{count}`\n"
            f"_(كل الصور تُرسل معاً في كل نشرة)_",
            buttons=btns
        )

    elif data == b"add_more_media":
        if chat_id not in temporary_autopost_config:
            temporary_autopost_config[chat_id] = {}
        temporary_autopost_config[chat_id]['adding_to_existing'] = True
        temporary_autopost_config[chat_id]['media_files'] = []
        user_current_state[chat_id] = "WAITING_POST_MEDIA"
        await event.respond(
            "📸 **أرسل الصور واحدة تلو الأخرى**\n"
            "عند الانتهاء اضغط الزر أدناه ⬇️",
            buttons=[[Button.inline("✅ تم إضافة الصور", b"done_adding_media")]]
        )

    elif data == b"delete_all_media":
        conf = await autopost_config_collection.find_one({"owner_id": chat_id})
        if conf:
            for f in conf.get('media_files', []):
                try:
                    if os.path.exists(f):
                        os.remove(f)
                except:
                    pass
            await autopost_config_collection.update_one(
                {"owner_id": chat_id},
                {"$set": {"media_files": [], "current_media_index": 0}}
            )
        await event.respond("🗑️ **تم حذف جميع الصور.**")

    # ----------------------------------------------------------------
    # زر "تم إضافة الصور"
    # ----------------------------------------------------------------
    elif data == b"done_adding_media":
        tmp = temporary_autopost_config.get(chat_id, {})
        new_files = tmp.get('media_files', [])
        adding_to_existing = tmp.get('adding_to_existing', False)

        if adding_to_existing:
            # إضافة للصور الموجودة في DB
            conf = await autopost_config_collection.find_one({"owner_id": chat_id})
            existing = conf.get('media_files', []) if conf else []
            all_files = existing + new_files
            await autopost_config_collection.update_one(
                {"owner_id": chat_id},
                {"$set": {"media_files": all_files}},
                upsert=True
            )
            temporary_autopost_config[chat_id]['adding_to_existing'] = False
            user_current_state[chat_id] = None
            await event.respond(
                f"✅ **تمت إضافة {len(new_files)} صورة.**\n"
                f"إجمالي الصور: `{len(all_files)}` (تُرسل كلها معاً في كل نشرة)"
            )
        else:
            # ضمن الإعداد الجديد — انتقل مباشرة للوقت
            count = len(new_files)
            user_current_state[chat_id] = "WAITING_POST_TIME"
            if count > 0:
                await event.respond(
                    f"✅ **تم حفظ {count} صورة** (ستُرسل كلها معاً في كل نشرة)\n\n"
                    f"⏱️ **الخطوة 2/3 — كم دقيقة بين كل نشر؟**"
                )
            else:
                await event.respond("⏱️ **الخطوة 2/3 — كم دقيقة بين كل نشر؟**"
            )

    # ----------------------------------------------------------------
    elif data == b"toggle_post":
        conf = await autopost_config_collection.find_one({"owner_id": chat_id})
        if not conf:
            await event.respond("❌ قم بالإعداد أولاً")
            return

        new_status = not conf.get('active', False)
        await autopost_config_collection.update_one(
            {"owner_id": chat_id}, {"$set": {"active": new_status}}, upsert=True
        )
        await manage_user_autopost_task(client, chat_id)
        await event.respond(f"✅ الحالة الآن: {'🟢 يعمل' if new_status else '🔴 متوقف'}")

    elif data == b"broadcast_menu":
        user_current_state[chat_id] = "WAITING_BROADCAST_MSG"
        await event.respond("📨 **أرسل الرسالة (صورة/فيديو/نص) التي تريد نشرها للخاص:**")

    elif data == b"list_replies":
        btns = []
        async for r in replies_collection.find({"owner_id": chat_id}):
            btns.append([Button.inline(f"🗑️ حذف: {r['keyword']}", f"del_rep_{r['_id']}")])
        btns.append([Button.inline("➕ إضافة رد", b"add_reply")])
        btns.append([Button.inline("🔙 رجوع", b"back_home")])
        await event.respond("📋 **الردود:**", buttons=btns)

    elif data == b"add_reply":
        user_current_state[chat_id] = "WAITING_REPLY_KEY"
        await event.respond("📝 **أرسل الكلمة المفتاحية:**")

    elif data.decode().startswith("del_rep_"):
        await replies_collection.delete_one({"_id": ObjectId(data.decode().split("_")[2])})
        await event.respond("✅ تم الحذف.")

    elif data == b"menu_radar":
        msg = "👮 **المراقبين:**\n"
        async for x in admins_watch_collection.find({"owner_id": chat_id}):
            msg += f"- {x['username']}\n"
        await event.respond(msg, buttons=[
            [Button.inline("➕ إضافة", b"add_radar"), Button.inline("🗑️ حذف", b"del_radar")],
            [Button.inline("🔙", b"back_home")]
        ])

    elif data == b"add_radar":
        user_current_state[chat_id] = "WAITING_RADAR_ADD"
        await event.respond("👤 **اليوزر:**")
    elif data == b"del_radar":
        user_current_state[chat_id] = "WAITING_RADAR_DEL"
        await event.respond("👤 **اليوزر:**")

    elif data == b"menu_tasks":
        user_current_state[chat_id] = "WAITING_TASK_HOURS"
        temporary_task_data[chat_id] = {}
        await event.respond("1️⃣ **عدد الساعات:**")

    elif data == b"toggle_ai":
        curr = await ai_settings_collection.find_one({"owner_id": chat_id})
        new_w = not curr.get('active') if curr else True
        await ai_settings_collection.update_one(
            {"owner_id": chat_id}, {"$set": {"active": new_w}}, upsert=True
        )
        await event.respond(f"🤖 الذكاء: {'🟢' if new_w else '🔴'}")

    elif data == b"back_home":
        await start_handler(event)

    elif data == b"view_stats":
        if client:
            d = await client.get_dialogs()
            await event.respond(f"📊 **عدد المحادثات:** {len(d)}")


# ==============================================================================
#                               9. معالج النصوص والصور
# ==============================================================================

@bot_client.on(events.NewMessage)
async def input_message_handler(event):
    chat_id = event.chat_id
    user_text = event.text.strip() if event.text else ""
    state = user_current_state.get(chat_id)
    if not state:
        return

    # ----------------------------------------------------------------
    if state == "WAITING_SESSION":
        if await start_userbot_session(chat_id, user_text):
            await sessions_collection.update_one(
                {"_id": chat_id}, {"$set": {"session_string": user_text}}, upsert=True
            )
            await event.respond("✅ **تم الدخول!**")
            await start_handler(event)
        else:
            await event.respond("❌ كود خطأ.")
        user_current_state[chat_id] = None

    elif state == "WAITING_BROADCAST_MSG":
        status_msg = await event.respond("⏳ **جاري النشر...**")
        asyncio.create_task(
            engine_broadcast_sender(active_userbot_clients[chat_id], status_msg, event.message)
        )
        user_current_state[chat_id] = None

    # ----------------------------------------------------------------
    # الخطوة 1: استقبال نص الرسالة
    # ----------------------------------------------------------------
    elif state == "WAITING_POST_MSG":
        html_msg = html.unparse(event.message.raw_text, event.message.entities)
        temporary_autopost_config[chat_id] = {
            'msg': html_msg,
            'media_files': [],
            'adding_to_existing': False
        }
        user_current_state[chat_id] = "WAITING_POST_MEDIA"
        await event.respond(
            "✅ **تم حفظ الرسالة.**\n\n"
            "📸 **الخطوة 2/3 — أرسل الصور واحدة تلو الأخرى**\n"
            "_(يمكنك إرسال أي عدد من الصور)_\n"
            "عند الانتهاء اضغط الزر أدناه ⬇️",
            buttons=[[Button.inline("✅ تم، انتقل للخطوة التالية", b"done_adding_media")]]
        )

    # ----------------------------------------------------------------
    # الخطوة 2: استقبال الصور
    # ----------------------------------------------------------------
    elif state == "WAITING_POST_MEDIA":
        if event.message.media:
            try:
                file_path = await event.message.download_media(file="temp_media/")
                if chat_id not in temporary_autopost_config:
                    temporary_autopost_config[chat_id] = {'media_files': []}
                if 'media_files' not in temporary_autopost_config[chat_id]:
                    temporary_autopost_config[chat_id]['media_files'] = []

                temporary_autopost_config[chat_id]['media_files'].append(file_path)
                count = len(temporary_autopost_config[chat_id]['media_files'])
                await event.respond(
                    f"✅ **تم حفظ الصورة {count}** 📸\n"
                    f"أرسل صورة أخرى أو اضغط **تم** للمتابعة",
                    buttons=[[Button.inline("✅ تم، انتقل للخطوة التالية", b"done_adding_media")]]
                )
            except Exception as e:
                await event.respond(f"❌ خطأ في تحميل الصورة: {e}")
        else:
            await event.respond(
                "⚠️ أرسل صورة أو اضغط الزر للمتابعة بدون صور",
                buttons=[[Button.inline("✅ تم، انتقل للخطوة التالية", b"done_adding_media")]]
            )

    # ----------------------------------------------------------------
    # الخطوة 3: استقبال الوقت
    # ----------------------------------------------------------------
    elif state == "WAITING_POST_TIME":
        try:
            temporary_autopost_config[chat_id]['time'] = int(user_text)
            user_current_state[chat_id] = "WAITING_POST_GROUPS"
            btns = []
            cli = active_userbot_clients[chat_id]
            async for d in cli.iter_dialogs(limit=50):
                if d.is_group:
                    btns.append([Button.inline(d.name[:30], f"grp_{d.id}")])
            btns.append([Button.inline("✅ حفظ وبدء النشر", b"save_autopost_final")])
            temporary_autopost_config[chat_id]['groups'] = []
            await event.respond(
                "📂 **الخطوة 3/3 — اختر الجروبات:**\n"
                "_(اضغط على الجروب لإضافته، اضغط مرة أخرى لحذفه)_",
                buttons=btns
            )
        except:
            await event.respond("❌ أدخل رقماً صحيحاً.")

    # ----------------------------------------------------------------
    elif state == "WAITING_REPLY_KEY":
        temporary_task_data[chat_id] = {'k': user_text}
        user_current_state[chat_id] = "WAITING_REPLY_VAL"
        await event.respond("📝 **الرد:**")

    elif state == "WAITING_REPLY_VAL":
        await replies_collection.update_one(
            {"owner_id": chat_id, "keyword": temporary_task_data[chat_id]['k']},
            {"$set": {"reply": user_text}}, upsert=True
        )
        await event.respond("✅ **تم الحفظ**")
        user_current_state[chat_id] = None

    elif state == "WAITING_RADAR_ADD":
        await admins_watch_collection.update_one(
            {"owner_id": chat_id, "username": user_text.replace("@", "")},
            {"$set": {"ts": time.time()}}, upsert=True
        )
        await event.respond("✅")
        user_current_state[chat_id] = None

    elif state == "WAITING_RADAR_DEL":
        await admins_watch_collection.delete_one(
            {"owner_id": chat_id, "username": user_text.replace("@", "")}
        )
        await event.respond("🗑️")
        user_current_state[chat_id] = None

    elif state == "WAITING_TASK_HOURS":
        try:
            temporary_task_data[chat_id] = {'h': int(user_text)}
            user_current_state[chat_id] = "WAITING_TASK_KEY"
            await event.respond("🔍 **كلمة البحث:**")
        except:
            await event.respond("❌ أدخل رقماً.")

    elif state == "WAITING_TASK_KEY":
        temporary_task_data[chat_id]['k'] = user_text
        user_current_state[chat_id] = "WAITING_TASK_REP"
        await event.respond("💬 **الرد (صورة/نص):**")

    elif state == "WAITING_TASK_REP":
        temporary_task_data[chat_id]['r'] = event.message
        user_current_state[chat_id] = "WAITING_TASK_DELAY"
        await event.respond("⏱️ **الثواني بين كل رد:**")

    elif state == "WAITING_TASK_DELAY":
        try:
            msg = await event.respond("🚀 **بدأت مهمة البحث...**")
            asyncio.create_task(engine_search_task(
                active_userbot_clients[chat_id], msg,
                temporary_task_data[chat_id]['h'],
                temporary_task_data[chat_id]['k'],
                temporary_task_data[chat_id]['r'],
                int(user_text)
            ))
            user_current_state[chat_id] = None
        except:
            await event.respond("❌ أدخل رقماً.")


# ----------------------------------------------------------------
# اختيار الجروبات
# ----------------------------------------------------------------
@bot_client.on(events.CallbackQuery(pattern=r'grp_'))
async def group_select(event):
    chat_id = event.chat_id
    group_id = int(event.data.decode().split('_')[1])
    l = temporary_autopost_config.get(chat_id, {}).get('groups', [])
    if group_id not in l:
        l.append(group_id)
        await event.answer("✅ تمت الإضافة")
    else:
        l.remove(group_id)
        await event.answer("❌ تم الحذف")
    temporary_autopost_config[chat_id]['groups'] = l


# ----------------------------------------------------------------
# حفظ الإعدادات النهائية
# ----------------------------------------------------------------
@bot_client.on(events.CallbackQuery(pattern=b'save_autopost_final'))
async def save_autopost_final(event):
    chat_id = event.chat_id
    d = temporary_autopost_config.get(chat_id)

    if not d or not d.get('groups'):
        await event.respond("❌ اختر جروباً واحداً على الأقل")
        return

    media_count = len(d.get('media_files', []))

    await autopost_config_collection.update_one(
        {"owner_id": chat_id},
        {"$set": {
            "message": d.get('msg', ''),
            "interval": d.get('time', 30),
            "groups": d['groups'],
            "media_files": d.get('media_files', []),
            "active": True
        }},
        upsert=True
    )
    await manage_user_autopost_task(active_userbot_clients[chat_id], chat_id)

    await event.respond(
        f"✅ **تم الحفظ وبدء النشر!**\n\n"
        f"📸 الصور: `{media_count}` (تُرسل كلها معاً)\n"
        f"⏱️ كل `{d.get('time', 30)}` دقيقة\n"
        f"📢 الجروبات: `{len(d['groups'])}`"
    )
    user_current_state[chat_id] = None


# ==============================================================================
#                               10. التشغيل الرئيسي
# ==============================================================================

async def main():
    await start_web_server()
    await load_all_sessions_from_db()
    print("✅ Bot Started — Multi-Account + Media AutoPost")
    await bot_client.start(bot_token=BOT_TOKEN)
    await bot_client.run_until_disconnected()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bot stopped manually.")
    except Exception as e:
        print(f"Fatal error: {e}")
