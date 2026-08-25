import os
import logging
import asyncio
import time
import subprocess
import re
import warnings
import sqlite3
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from asyncio import Semaphore

warnings.filterwarnings("ignore")

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

for name in ["urllib3", "requests", "edge_tts", "asyncio", "httpx", "httpcore",
             "telegram", "yt_dlp", "whisper", "pydub"]:
    logging.getLogger(name).setLevel(logging.WARNING)

from telegram import Update, InputFile, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler, filters,
    ContextTypes, CallbackQueryHandler
)
from telegram.constants import ParseMode

from pydub import AudioSegment
import yt_dlp
import whisper
from deep_translator import GoogleTranslator
import edge_tts

# ==================== CONFIG ====================
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN environment variable is required!")

executor = ThreadPoolExecutor(max_workers=2)
global_processing_semaphore = Semaphore(1)  # တစ်ချိန်တည်း တစ်ခုပဲ

user_processing_status = {}
user_status_lock = asyncio.Lock()

VOICES = {
    "thiha": {"id": "my-MM-ThihaNeural", "name": "Thiha", "gender": "ကျား", "emoji": "👨"},
    "nilar": {"id": "my-MM-NilarNeural", "name": "Nilar", "gender": "မ", "emoji": "👩"}
}

DEFAULT_VOICE = "thiha"
DEFAULT_SPEED = 1.4
VOICE_VOLUME = "+0%"
VOICE_PITCH = "+0Hz"
SPEED_OPTIONS = [0.8, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0]

TEMP_FOLDER = "temp_files"
os.makedirs(TEMP_FOLDER, exist_ok=True)

whisper_model = None
whisper_lock = asyncio.Lock()
DB_FILE = "bot_data.db"

# ==================== DATABASE ====================
def get_db_connection():
    return sqlite3.connect(DB_FILE, timeout=30, check_same_thread=False)

def init_db():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            voice TEXT DEFAULT 'thiha',
            speed REAL DEFAULT 1.4,
            mode TEXT DEFAULT 'auto',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            video_path TEXT,
            audio_path TEXT,
            srt_path TEXT,
            transcript_text TEXT,
            translated_text TEXT,
            edited_text TEXT,
            video_duration REAL,
            is_voice BOOLEAN DEFAULT 0,
            processing BOOLEAN DEFAULT 0,
            mode TEXT DEFAULT 'auto',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id)")
    conn.commit()
    conn.close()

def get_user(user_id):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT voice, speed, mode FROM users WHERE user_id = ?", (user_id,))
    row = c.fetchone()
    conn.close()
    if row:
        return {"voice": row[0], "speed": row[1], "mode": row[2] or "auto"}
    return None

def save_user(user_id, voice=None, speed=None, mode=None):
    conn = get_db_connection()
    c = conn.cursor()
    existing = get_user(user_id)
    if existing:
        if voice is not None:
            c.execute("UPDATE users SET voice = ? WHERE user_id = ?", (voice, user_id))
        if speed is not None:
            c.execute("UPDATE users SET speed = ? WHERE user_id = ?", (speed, user_id))
        if mode is not None:
            c.execute("UPDATE users SET mode = ? WHERE user_id = ?", (mode, user_id))
    else:
        c.execute(
            "INSERT INTO users (user_id, voice, speed, mode) VALUES (?, ?, ?, ?)",
            (user_id, voice or DEFAULT_VOICE, speed or DEFAULT_SPEED, mode or "auto")
        )
    conn.commit()
    conn.close()

def get_session(user_id):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("""
        SELECT video_path, audio_path, srt_path, transcript_text, translated_text,
               edited_text, video_duration, is_voice, processing, mode
        FROM sessions WHERE user_id = ? ORDER BY created_at DESC LIMIT 1
    """, (user_id,))
    row = c.fetchone()
    conn.close()
    if row:
        return {
            "video_path": row[0], "audio_path": row[1], "srt_path": row[2],
            "transcript_text": row[3], "translated_text": row[4], "edited_text": row[5],
            "video_duration": row[6],
            "is_voice": bool(row[7]) if row[7] is not None else False,
            "processing": bool(row[8]) if row[8] is not None else False,
            "mode": row[9] or "auto"
        }
    return None

def save_session(user_id, data):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT id FROM sessions WHERE user_id = ? ORDER BY created_at DESC LIMIT 1", (user_id,))
    row = c.fetchone()
    if row:
        updates = [f"{k} = ?" for k in data.keys()]
        values = list(data.values()) + [row[0]]
        c.execute(f"UPDATE sessions SET {', '.join(updates)}, updated_at = CURRENT_TIMESTAMP WHERE id = ?", values)
    else:
        cols = ", ".join(data.keys())
        placeholders = ", ".join(["?"] * len(data))
        c.execute(f"INSERT INTO sessions (user_id, {cols}) VALUES (?, {placeholders})", [user_id] + list(data.values()))
    conn.commit()
    conn.close()

def clear_session(user_id):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()

def delete_old_sessions():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('DELETE FROM sessions WHERE created_at < datetime("now", "-1 hour")')
    conn.commit()
    conn.close()

def get_user_mode(user_id):
    user = get_user(user_id)
    return user.get("mode", "auto") if user else "auto"

def set_user_mode(user_id, mode):
    save_user(user_id, mode=mode)

def get_user_speed(user_id):
    user = get_user(user_id)
    return user["speed"] if user else DEFAULT_SPEED

def get_user_voice(user_id):
    user = get_user(user_id)
    return user["voice"] if user else DEFAULT_VOICE

def get_voice_display(key):
    v = VOICES.get(key, VOICES[DEFAULT_VOICE])
    return f"{v['emoji']} {v['name']} ({v['gender']})"

def speed_to_edge_rate(speed):
    p = round((speed - 1.0) * 100)
    return f"+{p}%" if p >= 0 else f"{p}%"

def cleanup_files(*paths):
    for p in paths:
        if p and os.path.exists(p):
            try:
                os.remove(p)
            except Exception:
                pass

def get_duration(path):
    try:
        cmd = ["ffprobe", "-v", "error", "-show_entries", "format=duration",
               "-of", "default=noprint_wrappers=1:nokey=1", path]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
        if r.returncode == 0 and r.stdout.strip():
            return float(r.stdout.strip())
    except Exception:
        pass
    return None

def get_mp3_duration(path):
    d = get_duration(path)
    if d and d > 0:
        return d
    try:
        audio = AudioSegment.from_mp3(path)
        return len(audio) / 1000.0
    except Exception:
        return 5.0

# ==================== SRT ====================
def split_sentences(text):
    text = text.replace("။", "။").replace("၊", "၊")
    parts = re.split(r"(?<=[။။\?!\.])", text)
    parts = [s.strip() for s in parts if s.strip() and len(s) >= 2]
    return parts if parts else [text.strip()]

def format_ts(td):
    total = td.total_seconds()
    h = int(total // 3600)
    m = int((total % 3600) // 60)
    s = int(total % 60)
    ms = int((total % 1) * 1000)
    return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"

def generate_srt(text, duration):
    sentences = split_sentences(text)
    if not sentences:
        return ""
    weights = []
    for s in sentences:
        my = sum(1 for c in s if "\u1000" <= c <= "\u109F")
        weights.append(my * 1.2 + (len(s) - my))
    total_w = sum(weights) or 1
    pauses = []
    for i in range(len(sentences) - 1):
        if sentences[i].endswith(("။", ".", "!", "?")):
            pauses.append(0.35)
        else:
            pauses.append(0.2)
    speech = max(duration - sum(pauses), duration * 0.85)
    start = timedelta(0)
    lines = []
    for i, sent in enumerate(sentences):
        dur = max(speech * (weights[i] / total_w), 0.7)
        if i < len(pauses):
            dur += pauses[i]
        end = start + timedelta(seconds=dur)
        lines.append(str(i + 1))
        lines.append(f"{format_ts(start)} --> {format_ts(end)}")
        lines.append(sent)
        lines.append("")
        start = end
    return "\n".join(lines)

# ==================== TRANSLATION ====================
def postprocess(text):
    text = re.sub(r"\s+", " ", text).strip()
    text = text.replace("ငါ", "ကျွန်တော်").replace("မင်း", "ခင်ဗျား").replace("သင်", "ခင်ဗျား")
    return text

def translate_text(text, src="en", dest="my"):
    try:
        if not text or not text.strip():
            return text
        sentences = re.split(r"(?<=[.!?])\s+", text)
        results = []
        translator = GoogleTranslator(source=src, target=dest)
        for s in sentences:
            s = s.strip()
            if not s:
                continue
            try:
                t = translator.translate(s)
                results.append(t if t else s)
            except Exception:
                results.append(s)
            time.sleep(0.04)
        return postprocess(" ".join(results))
    except Exception:
        return text

# ==================== WHISPER (tiny) ====================
def load_whisper():
    global whisper_model
    if whisper_model is None:
        logger.info("Loading Whisper tiny model (low RAM)...")
        whisper_model = whisper.load_model("tiny")
        logger.info("Whisper tiny loaded.")
    return whisper_model

async def transcribe(video_path):
    try:
        async with whisper_lock:
            model = load_whisper()
        audio_path = video_path.rsplit(".", 1)[0] + ".mp3"
        cmd = ["ffmpeg", "-i", video_path, "-acodec", "libmp3lame", "-ab", "128k",
               "-ar", "16000", "-ac", "1", audio_path, "-y"]
        r = subprocess.run(cmd, capture_output=True, timeout=180)
        if r.returncode != 0 or not os.path.exists(audio_path):
            return None
        result = model.transcribe(audio_path, language="my", fp16=False)
        cleanup_files(audio_path)
        return result.get("text", "").strip()
    except Exception as e:
        logger.error(f"Transcribe error: {e}")
        return None

# ==================== DOWNLOAD ====================
async def download_video(url):
    try:
        uid = os.urandom(6).hex()
        out = os.path.join(TEMP_FOLDER, f"vid_{int(time.time())}_{uid}.mp4")
        opts = {
            "format": "best[height<=480][ext=mp4]/best[height<=480]/best",
            "outtmpl": out,
            "quiet": True,
            "no_warnings": True,
            "socket_timeout": 300,
            "retries": 3,
        }
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=True)
            if not info:
                return None
            fname = ydl.prepare_filename(info)
            if os.path.exists(fname) and os.path.getsize(fname) > 50000:
                return fname
            for f in os.listdir(TEMP_FOLDER):
                if uid in f and f.endswith(".mp4"):
                    p = os.path.join(TEMP_FOLDER, f)
                    if os.path.getsize(p) > 50000:
                        return p
        return None
    except Exception as e:
        logger.error(f"Download error: {e}")
        return None

# ==================== TTS ====================
async def text_to_mp3(text, outfile, speed, voice_key):
    rate = speed_to_edge_rate(speed)
    voice_id = VOICES[voice_key]["id"]
    communicate = edge_tts.Communicate(text, voice_id, rate=rate, volume=VOICE_VOLUME, pitch=VOICE_PITCH)
    await communicate.save(outfile)
    return get_mp3_duration(outfile)

# ==================== COMBINE (ffmpeg only) ====================
async def combine_video_audio(update, context, user_id, with_srt=False):
    try:
        session = get_session(user_id)
        if not session:
            raise Exception("Session not found")

        video_path = session.get("video_path")
        audio_path = session.get("audio_path")
        srt_path = session.get("srt_path")

        if not video_path or not os.path.exists(video_path):
            raise Exception("Video missing")
        if not audio_path or not os.path.exists(audio_path):
            raise Exception("Audio missing")

        progress = await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="⏳ Video + Audio ပေါင်းစပ်နေပါတယ်..."
        )

        v_dur = get_duration(video_path) or 30
        a_dur = get_mp3_duration(audio_path)
        speed_factor = v_dur / a_dur if a_dur > 0 else 1.0

        await progress.edit_text(
            f"🔄 ပြင်ဆင်နေပါတယ်...\n⏱️ {v_dur:.1f}s → {a_dur:.1f}s\n📊 {speed_factor:.2f}x"
        )

        # speed adjust video
        temp_vid = os.path.join(TEMP_FOLDER, f"tmp_{user_id}_{int(time.time())}.mp4")
        if abs(speed_factor - 1.0) > 0.02:
            cmd = [
                "ffmpeg", "-i", video_path,
                "-filter:v", f"setpts={1/speed_factor}*PTS",
                "-an", "-c:v", "libx264", "-preset", "veryfast", "-crf", "28",
                "-y", temp_vid
            ]
            r = subprocess.run(cmd, capture_output=True, timeout=300)
            if r.returncode != 0 or not os.path.exists(temp_vid):
                temp_vid = video_path
        else:
            temp_vid = video_path

        final = os.path.join(TEMP_FOLDER, f"final_{user_id}_{int(time.time())}.mp4")

        # simple combine (no subtitle burn for low RAM)
        cmd = [
            "ffmpeg", "-i", temp_vid, "-i", audio_path,
            "-c:v", "copy", "-c:a", "aac", "-b:a", "128k",
            "-map", "0:v:0", "-map", "1:a:0",
            "-shortest", "-y", final
        ]
        r = subprocess.run(cmd, capture_output=True, timeout=300)
        if r.returncode != 0:
            cmd = [
                "ffmpeg", "-i", temp_vid, "-i", audio_path,
                "-c:v", "libx264", "-preset", "veryfast", "-crf", "28",
                "-c:a", "aac", "-b:a", "128k",
                "-map", "0:v:0", "-map", "1:a:0",
                "-shortest", "-y", final
            ]
            r = subprocess.run(cmd, capture_output=True, timeout=300)
            if r.returncode != 0:
                raise Exception("Combine failed")

        if not os.path.exists(final) or os.path.getsize(final) < 1000:
            raise Exception("Output empty")

        await progress.edit_text("📤 ပို့နေပါတယ်...")
        with open(final, "rb") as f:
            caption = (
                f"✅ ပြီးပါပြီ!\n"
                f"⏱️ {a_dur:.1f}s | Speed {speed_factor:.2f}x\n"
                f"{'📝 SRT ဖိုင်လည်း ပို့ထားပါတယ်' if with_srt and srt_path else '📝 SRT မပါ'}"
            )
            await update.effective_message.reply_video(
                video=InputFile(f, filename="result.mp4"),
                caption=caption
            )

        # send srt separately if exists (no burn → saves RAM)
        if with_srt and srt_path and os.path.exists(srt_path):
            with open(srt_path, "rb") as s:
                await context.bot.send_document(
                    chat_id=update.effective_chat.id,
                    document=s,
                    filename="subtitle.srt",
                    caption="📝 Subtitle (SRT)"
                )

        await progress.delete()
        cleanup_files(video_path, audio_path, temp_vid, final, srt_path)
        clear_session(user_id)

    except Exception as e:
        logger.error(f"Combine error: {e}")
        try:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=f"❌ Error: {str(e)[:120]}"
            )
        except Exception:
            pass
        session = get_session(user_id)
        if session:
            cleanup_files(session.get("video_path"), session.get("audio_path"), session.get("srt_path"))
        clear_session(user_id)
    finally:
        async with user_status_lock:
            user_processing_status[user_id] = False

# ==================== KEYBOARDS ====================
def settings_kb(user_id):
    speed = get_user_speed(user_id)
    voice = get_voice_display(get_user_voice(user_id))
    mode = get_user_mode(user_id)
    mode_txt = "ရိုးရိုး" if mode == "simple" else "ပုံမှန်"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"🎤 {voice}", callback_data="show_voice")],
        [InlineKeyboardButton(f"⚡ {speed:.1f}x", callback_data="show_speed")],
        [InlineKeyboardButton(f"📌 Mode: {mode_txt}", callback_data="show_mode")],
        [InlineKeyboardButton("❌ ပိတ်ရန်", callback_data="close")]
    ])

def voice_kb(user_id):
    cur = get_user_voice(user_id)
    rows = []
    for k, v in VOICES.items():
        check = "✅ " if k == cur else ""
        rows.append([InlineKeyboardButton(f"{check}{v['emoji']} {v['name']}", callback_data=f"set_voice_{k}")])
    rows.append([InlineKeyboardButton("🔙 နောက်သို့", callback_data="back")])
    return InlineKeyboardMarkup(rows)

def speed_kb(user_id):
    cur = get_user_speed(user_id)
    row = []
    rows = []
    for s in SPEED_OPTIONS:
        check = "✅" if s == cur else ""
        row.append(InlineKeyboardButton(f"{check}{s:.1f}x", callback_data=f"set_speed_{s}"))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("🔙 နောက်သို့", callback_data="back")])
    return InlineKeyboardMarkup(rows)

def mode_kb(user_id):
    cur = get_user_mode(user_id)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"{'✅ ' if cur=='simple' else ''}ရိုးရိုး (SRT မပါ)", callback_data="set_mode_simple")],
        [InlineKeyboardButton(f"{'✅ ' if cur=='auto' else ''}ပုံမှန် (SRT ပါ)", callback_data="set_mode_auto")],
        [InlineKeyboardButton("🔙 နောက်သို့", callback_data="back")]
    ])

# ==================== HANDLERS ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    save_user(update.effective_user.id)
    await update.message.reply_text(
        "👋 မင်္ဂလာပါ!\n\n"
        "🎬 YouTube / TikTok Link သို့မဟုတ် Video ဖိုင် ပို့ပါ။\n"
        "မြန်မာလို အသံသွင်းပေးပါမယ်။\n\n"
        "⚙️ /settings | 📖 /help",
        parse_mode=ParseMode.MARKDOWN
    )

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📖 **အသုံးပြုနည်း**\n\n"
        "• YouTube / TikTok Link ပို့ပါ\n"
        "• Video ဖိုင် တိုက်ရိုက်ပို့ပါ\n\n"
        "Mode:\n"
        "• ရိုးရိုး → SRT မပါ (ပိုမြန်)\n"
        "• ပုံမှန် → SRT ဖိုင်ပါ\n\n"
        "Command:\n"
        "/b = Thiha | /g = Nilar\n"
        "/speed 1.4 | /s = ရိုးရိုး | /sk = ပုံမှန်\n"
        "/settings",
        parse_mode=ParseMode.MARKDOWN
    )

async def process_media(update, context, video_path, source="Video"):
    user_id = update.effective_user.id
    mode = get_user_mode(user_id)
    status = await update.message.reply_text(f"⏳ {source} လက်ခံပြီးပါပြီ။ Transcription လုပ်နေပါတယ်...")

    try:
        duration = get_duration(video_path)
        save_session(user_id, {
            "video_path": video_path,
            "video_duration": duration,
            "mode": mode
        })

        transcript = await transcribe(video_path)
        if not transcript:
            await status.edit_text("❌ စာသားထုတ်မရပါ။ Video တိုတိုလေး စမ်းကြည့်ပါ။")
            cleanup_files(video_path)
            clear_session(user_id)
            return

        await status.edit_text("🔄 ဘာသာပြန်နေပါတယ်...")
        loop = asyncio.get_event_loop()
        translated = await loop.run_in_executor(executor, translate_text, transcript)

        await status.edit_text("🎙 အသံဖန်တီးနေပါတယ်...")
        speed = get_user_speed(user_id)
        voice_key = get_user_voice(user_id)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        mp3_path = os.path.join(TEMP_FOLDER, f"tts_{user_id}_{ts}.mp3")
        srt_path = os.path.join(TEMP_FOLDER, f"srt_{user_id}_{ts}.srt")

        dur = await text_to_mp3(translated, mp3_path, speed, voice_key)

        if mode == "auto":
            srt_content = generate_srt(translated, dur)
            with open(srt_path, "w", encoding="utf-8") as f:
                f.write(srt_content)
            save_session(user_id, {"srt_path": srt_path})

        save_session(user_id, {"audio_path": mp3_path, "translated_text": translated})

        # send audio
        with open(mp3_path, "rb") as a:
            await context.bot.send_audio(
                chat_id=update.effective_chat.id,
                audio=a,
                caption=f"🎧 {get_voice_display(voice_key)} | {speed:.1f}x | {dur:.1f}s",
                title="Myanmar Voice",
                performer="AI"
            )

        await status.edit_text("🎬 Video ပေါင်းစပ်နေပါတယ်...")
        await combine_video_audio(update, context, user_id, with_srt=(mode == "auto"))

    except Exception as e:
        logger.error(f"process_media error: {e}")
        await status.edit_text(f"❌ Error: {str(e)[:120]}")
        cleanup_files(video_path)
        clear_session(user_id)
    finally:
        async with user_status_lock:
            user_processing_status[user_id] = False

async def handle_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    url = update.message.text.strip()
    user_id = update.effective_user.id

    async with user_status_lock:
        if user_processing_status.get(user_id):
            await update.message.reply_text("⏳ ယခင်အလုပ် ဆောင်ရွက်နေဆဲပါ။ စောင့်ပေးပါ။")
            return
        user_processing_status[user_id] = True

    async with global_processing_semaphore:
        status = await update.message.reply_text("⏳ Video Download လုပ်နေပါတယ်...")
        try:
            save_user(user_id)
            clear_session(user_id)

            video_path = await download_video(url)
            if not video_path:
                await status.edit_text("❌ Download မအောင်မြင်ပါ။ Link မှန်ရဲ့လား စစ်ပါ။")
                return

            await status.delete()
            await process_media(update, context, video_path, source="YouTube/TikTok")
        except Exception as e:
            logger.error(f"handle_link: {e}")
            await status.edit_text(f"❌ Error: {str(e)[:100]}")
        finally:
            async with user_status_lock:
                user_processing_status[user_id] = False

async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    async with user_status_lock:
        if user_processing_status.get(user_id):
            await update.message.reply_text("⏳ ယခင်အလုပ် ဆောင်ရွက်နေဆဲပါ။")
            return
        user_processing_status[user_id] = True

    async with global_processing_semaphore:
        try:
            save_user(user_id)
            clear_session(user_id)

            video = update.message.video
            if not video:
                await update.message.reply_text("❌ Video မတွေ့ပါ။")
                return

            status = await update.message.reply_text("⏳ Video လက်ခံနေပါတယ်...")
            f = await video.get_file()
            path = os.path.join(TEMP_FOLDER, f"up_{user_id}_{int(time.time())}.mp4")
            await f.download_to_drive(path)

            if not os.path.exists(path) or os.path.getsize(path) < 1000:
                await status.edit_text("❌ Download မအောင်မြင်ပါ။")
                return

            await status.delete()
            await process_media(update, context, path, source="Uploaded Video")
        except Exception as e:
            logger.error(f"handle_video: {e}")
            await update.message.reply_text(f"❌ Error: {str(e)[:100]}")
        finally:
            async with user_status_lock:
                user_processing_status[user_id] = False

async def handle_text_edit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # optional: edited transcript → re-translate + TTS
    pass

async def cmd_b(update: Update, context: ContextTypes.DEFAULT_TYPE):
    save_user(update.effective_user.id, voice="thiha")
    await update.message.reply_text(f"✅ {get_voice_display('thiha')}")

async def cmd_g(update: Update, context: ContextTypes.DEFAULT_TYPE):
    save_user(update.effective_user.id, voice="nilar")
    await update.message.reply_text(f"✅ {get_voice_display('nilar')}")

async def cmd_speed(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = update.message.text.split()
    if len(args) < 2:
        await update.message.reply_text(f"⚡ လက်ရှိ: {get_user_speed(update.effective_user.id):.1f}x\nဥပမာ: /speed 1.4")
        return
    try:
        sp = float(args[1])
        closest = min(SPEED_OPTIONS, key=lambda x: abs(x - sp))
        save_user(update.effective_user.id, speed=closest)
        await update.message.reply_text(f"✅ Speed {closest:.1f}x")
    except Exception:
        await update.message.reply_text("⚠️ ဥပမာ: /speed 1.4")

async def cmd_s(update: Update, context: ContextTypes.DEFAULT_TYPE):
    set_user_mode(update.effective_user.id, "simple")
    await update.message.reply_text("✅ ရိုးရိုး Mode (SRT မပါ)")

async def cmd_sk(update: Update, context: ContextTypes.DEFAULT_TYPE):
    set_user_mode(update.effective_user.id, "auto")
    await update.message.reply_text("✅ ပုံမှန် Mode (SRT ပါ)")

async def cmd_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⚙️ ဆက်တင်များ", reply_markup=settings_kb(update.effective_user.id))

async def callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = update.effective_user.id
    d = q.data

    if d == "show_voice":
        await q.edit_message_text("🎤 အသံရွေးပါ", reply_markup=voice_kb(uid))
    elif d == "show_speed":
        await q.edit_message_text("⚡ အမြန်နှုန်းရွေးပါ", reply_markup=speed_kb(uid))
    elif d == "show_mode":
        await q.edit_message_text("📌 Mode ရွေးပါ", reply_markup=mode_kb(uid))
    elif d.startswith("set_voice_"):
        key = d.replace("set_voice_", "")
        save_user(uid, voice=key)
        await q.edit_message_text(f"✅ {get_voice_display(key)}", reply_markup=settings_kb(uid))
    elif d.startswith("set_speed_"):
        sp = float(d.replace("set_speed_", ""))
        save_user(uid, speed=sp)
        await q.edit_message_text(f"✅ {sp:.1f}x", reply_markup=settings_kb(uid))
    elif d == "set_mode_simple":
        set_user_mode(uid, "simple")
        await q.edit_message_text("✅ ရိုးရိုး Mode", reply_markup=settings_kb(uid))
    elif d == "set_mode_auto":
        set_user_mode(uid, "auto")
        await q.edit_message_text("✅ ပုံမှန် Mode", reply_markup=settings_kb(uid))
    elif d == "back":
        await q.edit_message_text("⚙️ ဆက်တင်များ", reply_markup=settings_kb(uid))
    elif d == "close":
        await q.edit_message_text("✅ ပိတ်လိုက်ပါပြီ")

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Error: {context.error}")

# ==================== MAIN ====================
def main():
    init_db()
    delete_old_sessions()

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .connect_timeout(30)
        .read_timeout(30)
        .write_timeout(30)
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("settings", cmd_settings))
    app.add_handler(CommandHandler("b", cmd_b))
    app.add_handler(CommandHandler("g", cmd_g))
    app.add_handler(CommandHandler("speed", cmd_speed))
    app.add_handler(CommandHandler("s", cmd_s))
    app.add_handler(CommandHandler("sk", cmd_sk))
    app.add_handler(CallbackQueryHandler(callback))

    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & filters.Regex(r"youtube\.com|youtu\.be|tiktok\.com"),
        handle_link
    ))
    app.add_handler(MessageHandler(filters.VIDEO, handle_video))

    app.add_error_handler(error_handler)

    logger.info("Bot starting (low RAM mode - Whisper tiny)...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
