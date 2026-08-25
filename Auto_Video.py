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

# ========== Logging ==========
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

for name in ["urllib3", "requests", "telebot", "edge_tts", "asyncio", "httpx",
             "httpcore", "telegram", "moviepy", "yt_dlp", "whisper"]:
    logging.getLogger(name).setLevel(logging.WARNING)

from telegram import Update, InputFile, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler, filters,
    ContextTypes, CallbackQueryHandler
)
from telegram.constants import ParseMode

from moviepy.editor import VideoFileClip, AudioFileClip
from pydub import AudioSegment
import yt_dlp
import whisper
from deep_translator import GoogleTranslator
import edge_tts

# ========== Config ==========
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN environment variable is required!")

executor = ThreadPoolExecutor(max_workers=4)
global_processing_semaphore = Semaphore(3)

user_processing_status = {}
user_status_lock = asyncio.Lock()

VOICES = {
    "thiha": {
        "id": "my-MM-ThihaNeural",
        "name": "Thiha",
        "gender": "ကျား",
        "emoji": "👨"
    },
    "nilar": {
        "id": "my-MM-NilarNeural",
        "name": "Nilar",
        "gender": "မ",
        "emoji": "👩"
    }
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

FONT_PATHS = [
    "/usr/share/fonts/truetype/noto/NotoSansMyanmar-Regular.ttf",
    "/usr/share/fonts/truetype/noto/NotoSansMyanmarUI-Regular.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
]

# ========== Database ==========
def get_db_connection():
    return sqlite3.connect(DB_FILE, timeout=60, check_same_thread=False)

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            voice TEXT DEFAULT 'thiha',
            speed REAL DEFAULT 1.4,
            mode TEXT DEFAULT 'auto',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
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
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (user_id)
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sessions_created_at ON sessions(created_at)")
    conn.commit()
    conn.close()

def get_user(user_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT voice, speed, mode FROM users WHERE user_id = ?", (user_id,))
    result = cursor.fetchone()
    conn.close()
    if result:
        return {"voice": result[0], "speed": result[1], "mode": result[2] or "auto"}
    return None

def save_user(user_id, voice=None, speed=None, mode=None):
    conn = get_db_connection()
    cursor = conn.cursor()
    existing = get_user(user_id)
    if existing:
        if voice is not None:
            cursor.execute("UPDATE users SET voice = ? WHERE user_id = ?", (voice, user_id))
        if speed is not None:
            cursor.execute("UPDATE users SET speed = ? WHERE user_id = ?", (speed, user_id))
        if mode is not None:
            cursor.execute("UPDATE users SET mode = ? WHERE user_id = ?", (mode, user_id))
    else:
        cursor.execute(
            "INSERT INTO users (user_id, voice, speed, mode) VALUES (?, ?, ?, ?)",
            (user_id, voice or DEFAULT_VOICE, speed or DEFAULT_SPEED, mode or "auto")
        )
    conn.commit()
    conn.close()

def get_session(user_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT video_path, audio_path, srt_path, transcript_text, translated_text,
               edited_text, video_duration, is_voice, processing, mode
        FROM sessions
        WHERE user_id = ?
        ORDER BY created_at DESC LIMIT 1
    """, (user_id,))
    result = cursor.fetchone()
    conn.close()
    if result:
        return {
            "video_path": result[0],
            "audio_path": result[1],
            "srt_path": result[2],
            "transcript_text": result[3],
            "translated_text": result[4],
            "edited_text": result[5],
            "video_duration": result[6],
            "is_voice": bool(result[7]) if result[7] is not None else False,
            "processing": bool(result[8]) if result[8] is not None else False,
            "mode": result[9] or "auto"
        }
    return None

def save_session(user_id, data):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM sessions WHERE user_id = ? ORDER BY created_at DESC LIMIT 1", (user_id,))
    result = cursor.fetchone()
    if result:
        session_id = result[0]
        updates = []
        values = []
        for key, value in data.items():
            updates.append(f"{key} = ?")
            values.append(value)
        values.append(session_id)
        cursor.execute(
            f"UPDATE sessions SET {', '.join(updates)}, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            values
        )
    else:
        columns = ", ".join(data.keys())
        placeholders = ", ".join(["?"] * len(data))
        cursor.execute(
            f"INSERT INTO sessions (user_id, {columns}) VALUES (?, {placeholders})",
            [user_id] + list(data.values())
        )
    conn.commit()
    conn.close()

def clear_session(user_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()

def delete_old_sessions():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM sessions WHERE created_at < datetime("now", "-1 hour")')
    conn.commit()
    conn.close()

def get_user_mode(user_id):
    user = get_user(user_id)
    return user.get("mode", "auto") if user else "auto"

def set_user_mode(user_id, mode):
    save_user(user_id, mode=mode)

# ========== Helpers ==========
def get_user_speed(user_id: int) -> float:
    user = get_user(user_id)
    return user["speed"] if user else DEFAULT_SPEED

def get_user_voice(user_id: int) -> str:
    user = get_user(user_id)
    return user["voice"] if user else DEFAULT_VOICE

def get_voice_display(voice_key: str) -> str:
    voice = VOICES.get(voice_key, VOICES[DEFAULT_VOICE])
    return f"{voice['emoji']} {voice['name']} ({voice['gender']})"

def speed_to_edge_rate(speed: float) -> str:
    percentage = round((speed - 1.0) * 100)
    return f"+{percentage}%" if percentage >= 0 else f"{percentage}%"

def cleanup_files(*file_paths):
    for path in file_paths:
        if path and os.path.exists(path):
            try:
                os.remove(path)
            except Exception:
                pass

def get_video_duration_with_ffmpeg(video_path):
    try:
        cmd = [
            "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            video_path
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0 and result.stdout.strip():
            return float(result.stdout.strip())
    except Exception:
        pass
    return None

def get_video_dimensions(video_path):
    try:
        cmd = [
            "ffprobe", "-v", "error",
            "-select_streams", "v:0",
            "-show_entries", "stream=width,height",
            "-of", "csv=p=0",
            video_path
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.stdout:
            parts = result.stdout.strip().split(",")
            if len(parts) == 2:
                return int(parts[0]), int(parts[1])
    except Exception:
        pass
    return 1920, 1080

def get_mp3_duration_accurate(filepath: str) -> float:
    try:
        cmd = [
            "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            filepath
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
        if result.returncode == 0 and result.stdout.strip():
            duration = float(result.stdout.strip())
            if duration > 0:
                return duration
    except Exception:
        pass
    try:
        audio = AudioSegment.from_mp3(filepath)
        return len(audio) / 1000.0
    except Exception:
        pass
    return 5.0

def split_into_sentences(text: str) -> list:
    text = text.replace("။", "။").replace("၊", "၊")
    pattern = r"(?<=[။။\?!\.])"
    sentences = re.split(pattern, text)
    sentences = [s.strip() for s in sentences if s.strip() and len(s) >= 2]
    return sentences if sentences else [text.strip()]

def format_timestamp(td: timedelta) -> str:
    total_seconds = td.total_seconds()
    hours = int(total_seconds // 3600)
    minutes = int((total_seconds % 3600) // 60)
    seconds = int(total_seconds % 60)
    milliseconds = int((total_seconds % 1) * 1000)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d},{milliseconds:03d}"

def generate_srt_advanced(text: str, voice_name: str, speed: float, actual_duration: float) -> str:
    sentences = split_into_sentences(text)
    char_counts = []
    for s in sentences:
        myanmar_chars = sum(1 for c in s if "\u1000" <= c <= "\u109F")
        other_chars = len(s) - myanmar_chars
        char_counts.append(myanmar_chars * 1.2 + other_chars * 1.0)
    total_weighted = sum(char_counts) or 1

    pause_durations = []
    for i in range(len(sentences) - 1):
        current = sentences[i]
        if current.endswith(("။", ".", "!", "?")):
            pause_durations.append(0.4)
        elif current.endswith(("၊", ",")):
            pause_durations.append(0.2)
        else:
            pause_durations.append(0.3)

    total_pause = sum(pause_durations)
    speech_duration = max(actual_duration - total_pause, actual_duration * 0.85)

    start_time = timedelta(seconds=0)
    srt_lines = []
    index = 1

    for i, sentence in enumerate(sentences):
        char_ratio = char_counts[i] / total_weighted
        sentence_duration = max(speech_duration * char_ratio, 0.8)
        if i < len(sentences) - 1:
            sentence_duration += pause_durations[i]

        end_time = start_time + timedelta(seconds=sentence_duration)
        srt_lines.append(str(index))
        srt_lines.append(f"{format_timestamp(start_time)} --> {format_timestamp(end_time)}")
        srt_lines.append(sentence)
        srt_lines.append("")
        index += 1
        start_time = end_time

    return "\n".join(srt_lines)

# ========== Translation ==========
def preprocess_text(text):
    sentences = re.split(r"(?<=[.!?])\s+", text)
    return [s.strip() for s in sentences if s.strip()]

def postprocess_text(translated_text):
    translated_text = re.sub(r"\s+", " ", translated_text).strip()
    translated_text = translated_text.replace("ငါ", "ကျွန်တော်")
    translated_text = translated_text.replace("မင်း", "ခင်ဗျား")
    translated_text = translated_text.replace("သင်", "ခင်ဗျား")
    return translated_text

def translate_sentence(sentence, src="en", dest="my"):
    try:
        if not sentence or not sentence.strip():
            return sentence
        translator = GoogleTranslator(source=src, target=dest)
        translated = translator.translate(sentence)
        return postprocess_text(translated) if translated else sentence
    except Exception:
        return sentence

def translate_story_style(text, src="en", dest="my"):
    try:
        if not text or not text.strip():
            return text
        sentences = preprocess_text(text)
        translated_sentences = []
        for sentence in sentences:
            translated_sentences.append(translate_sentence(sentence, src, dest))
            time.sleep(0.05)
        return postprocess_text(" ".join(translated_sentences))
    except Exception:
        return text

# ========== Whisper ==========
def load_whisper_model():
    global whisper_model
    if whisper_model is None:
        logger.info("Loading Whisper model (base)...")
        whisper_model = whisper.load_model("base")
        logger.info("Whisper model loaded.")
    return whisper_model

async def download_youtube_video(url: str) -> str:
    try:
        unique_id = os.urandom(8).hex()
        video_filename = os.path.join(TEMP_FOLDER, f"video_{int(time.time())}_{unique_id}.mp4")
        ydl_opts = {
            "format": "bestvideo[height<=720][ext=mp4]+bestaudio[ext=m4a]/best[height<=720][ext=mp4]/best[height<=720]",
            "outtmpl": video_filename,
            "quiet": True,
            "no_warnings": True,
            "ignoreerrors": True,
            "socket_timeout": 600,
            "retries": 5,
            "http_headers": {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            }
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            if info is None:
                return None
            downloaded_file = ydl.prepare_filename(info)
            if os.path.exists(downloaded_file) and os.path.getsize(downloaded_file) > 100000:
                return downloaded_file
            for file in os.listdir(TEMP_FOLDER):
                if unique_id in file and file.endswith(".mp4"):
                    full_path = os.path.join(TEMP_FOLDER, file)
                    if os.path.getsize(full_path) > 100000:
                        return full_path
            return None
    except Exception as e:
        logger.error(f"YouTube download error: {e}")
        return None

async def download_tiktok_video(url: str) -> str:
    try:
        unique_id = os.urandom(8).hex()
        video_filename = os.path.join(TEMP_FOLDER, f"tiktok_{int(time.time())}_{unique_id}.mp4")
        ydl_opts = {
            "format": "best[height<=720]",
            "outtmpl": video_filename,
            "quiet": True,
            "no_warnings": True,
            "ignoreerrors": True,
            "socket_timeout": 600,
            "retries": 5,
            "http_headers": {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            }
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            if info is None:
                return None
            downloaded_file = ydl.prepare_filename(info)
            if os.path.exists(downloaded_file) and os.path.getsize(downloaded_file) > 100000:
                return downloaded_file
            for file in os.listdir(TEMP_FOLDER):
                if unique_id in file and file.endswith(".mp4"):
                    full_path = os.path.join(TEMP_FOLDER, file)
                    if os.path.getsize(full_path) > 100000:
                        return full_path
            return None
    except Exception as e:
        logger.error(f"TikTok download error: {e}")
        return None

async def transcribe_audio(video_path: str) -> str:
    try:
        async with whisper_lock:
            model = load_whisper_model()

        audio_path = video_path.replace(".mp4", ".mp3")
        cmd = ["ffmpeg", "-i", video_path, "-acodec", "libmp3lame", "-ab", "192k", audio_path, "-y"]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if result.returncode != 0 or not os.path.exists(audio_path) or os.path.getsize(audio_path) == 0:
            return None

        result = model.transcribe(audio_path, language="my", fp16=False)
        transcript = result["text"]

        if os.path.exists(audio_path):
            try:
                os.remove(audio_path)
            except Exception:
                pass
        return transcript
    except Exception as e:
        logger.error(f"Transcribe error: {e}")
        return None

# ========== TTS ==========
async def convert_text_to_mp3(text: str, output_filename: str, speed: float, voice_key: str) -> float:
    rate = speed_to_edge_rate(speed)
    voice_id = VOICES[voice_key]["id"]
    communicate = edge_tts.Communicate(
        text, voice_id, rate=rate, volume=VOICE_VOLUME, pitch=VOICE_PITCH
    )
    await communicate.save(output_filename)
    return get_mp3_duration_accurate(output_filename)

# ========== Subtitle ==========
def burn_subtitles_ffmpeg(video_path, srt_path, output_path, speed_factor=1.0):
    try:
        srt_content = None
        for encoding in ["utf-8-sig", "utf-8", "utf-16", "cp1252"]:
            try:
                with open(srt_path, "r", encoding=encoding) as f:
                    srt_content = f.read()
                break
            except Exception:
                continue
        if not srt_content:
            return False

        srt_content = srt_content.replace("\ufeff", "").replace("\r\n", "\n").replace("\r", "\n")
        clean_srt_path = srt_path.replace(".srt", "_clean.srt")
        with open(clean_srt_path, "w", encoding="utf-8-sig") as f:
            f.write(srt_content)

        width, height = get_video_dimensions(video_path)
        margin_v = int(height * 0.10)

        font_name = "Noto Sans Myanmar"
        for fp in FONT_PATHS:
            if os.path.exists(fp):
                font_name = os.path.splitext(os.path.basename(fp))[0]
                break

        ass_path = clean_srt_path.replace(".srt", ".ass")
        ass_content = f"""[Script Info]
ScriptType: v4.00+
PlayResX: {width}
PlayResY: {height}
WrapStyle: 0
ScaledBorderAndShadow: yes

[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
Style: Default,{font_name},15,&H00FFFF00,&H00000000,&H00000000,&H80000000,1,0,0,0,100,100,0,0,1,2,1,2,20,20,{margin_v},1

[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
"""
        blocks = srt_content.strip().split("\n\n")
        for block in blocks:
            lines = block.strip().split("\n")
            if len(lines) >= 3:
                time_match = re.match(
                    r"(\d{2}):(\d{2}):(\d{2}),(\d{3}) --> (\d{2}):(\d{2}):(\d{2}),(\d{3})",
                    lines[1]
                )
                if time_match:
                    start = f"{int(time_match.group(1)):02d}:{int(time_match.group(2)):02d}:{int(time_match.group(3)):02d}.{int(time_match.group(4)):03d}"
                    end = f"{int(time_match.group(5)):02d}:{int(time_match.group(6)):02d}:{int(time_match.group(7)):02d}.{int(time_match.group(8)):03d}"
                    text = " ".join(lines[2:]).replace("\\", "\\\\").replace("{", "\\{").replace("}", "\\}")
                    ass_content += f"Dialogue: 0,{start},{end},Default,,0,0,0,,{text}\n"

        with open(ass_path, "w", encoding="utf-8-sig") as f:
            f.write(ass_content)

        cmd = ["ffmpeg", "-i", video_path, "-vf", f"ass='{ass_path}'", "-c:a", "copy", "-y", output_path]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        cleanup_files(clean_srt_path, ass_path)
        return result.returncode == 0 and os.path.exists(output_path) and os.path.getsize(output_path) > 0
    except Exception as e:
        logger.error(f"Burn subtitle error: {e}")
        return False

# ========== Video Processing ==========
async def process_video_simple(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    try:
        session = get_session(user_id)
        if not session:
            raise Exception("No session found")

        video_path = session.get("video_path")
        audio_path = session.get("audio_path")

        if not video_path or not os.path.exists(video_path):
            raise Exception("Video file not found")
        if not audio_path or not os.path.exists(audio_path):
            raise Exception("Audio file not found")

        progress_msg = await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="⏳ Video နဲ့ Audio ပေါင်းစပ်နေပါပြီ..."
        )

        video_duration = get_video_duration_with_ffmpeg(video_path) or 30
        try:
            audio_clip = AudioFileClip(audio_path)
            audio_duration = audio_clip.duration
            audio_clip.close()
        except Exception:
            audio_duration = video_duration

        speed_factor = video_duration / audio_duration if audio_duration > 0 else 1.0

        await progress_msg.edit_text(
            f"🔄 Video ကို Audio အတိုင်း ပြင်ဆင်နေပါပြီ...\n"
            f"⏱️ {video_duration:.1f}s → {audio_duration:.1f}s\n"
            f"📊 Speed: {speed_factor:.2f}x"
        )

        audio_mp3 = audio_path
        if not audio_path.lower().endswith(".mp3"):
            audio_mp3 = f"audio_{user_id}_{int(time.time())}.mp3"
            subprocess.run(
                ["ffmpeg", "-i", audio_path, "-acodec", "libmp3lame", "-ab", "192k", audio_mp3, "-y"],
                capture_output=True, timeout=300
            )

        temp_video_path = f"temp_video_{user_id}_{int(time.time())}.mp4"
        if abs(speed_factor - 1.0) > 0.01:
            cmd = [
                "ffmpeg", "-i", video_path,
                "-filter:v", f"setpts={1/speed_factor}*PTS",
                "-an", "-c:v", "libx264", "-preset", "fast", "-crf", "23",
                "-y", temp_video_path
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
            if result.returncode != 0 or not os.path.exists(temp_video_path):
                temp_video_path = video_path
        else:
            temp_video_path = video_path

        final_output = f"final_{user_id}_{int(time.time())}.mp4"
        cmd_combine = [
            "ffmpeg", "-i", temp_video_path, "-i", audio_mp3,
            "-c:v", "copy", "-c:a", "aac",
            "-map", "0:v:0", "-map", "1:a:0",
            "-shortest", "-y", final_output
        ]
        result = subprocess.run(cmd_combine, capture_output=True, text=True, timeout=600)

        if result.returncode != 0:
            cmd_combine = [
                "ffmpeg", "-i", temp_video_path, "-i", audio_mp3,
                "-c:v", "libx264", "-c:a", "aac",
                "-map", "0:v:0", "-map", "1:a:0",
                "-shortest", "-preset", "fast", "-crf", "23",
                "-y", final_output
            ]
            result = subprocess.run(cmd_combine, capture_output=True, text=True, timeout=600)
            if result.returncode != 0:
                raise Exception("Combine failed")

        if not os.path.exists(final_output) or os.path.getsize(final_output) == 0:
            raise Exception("Final output empty")

        await progress_msg.edit_text("📤 Video ကို ပို့နေပါပြီ...")
        with open(final_output, "rb") as f:
            caption = (
                f"✅ Video + Audio ပေါင်းစပ်ပြီးပါပြီ!\n"
                f"⏱️ {audio_duration:.1f}s\n"
                f"📊 Speed: {speed_factor:.2f}x\n"
                f"📝 SRT မပါပါ။"
            )
            await update.effective_message.reply_video(
                video=InputFile(f, filename="combined_video.mp4"),
                caption=caption
            )

        await progress_msg.delete()
        cleanup_files(video_path, audio_path, temp_video_path, final_output)
        if audio_mp3 != audio_path:
            cleanup_files(audio_mp3)
        clear_session(user_id)

    except Exception as e:
        logger.error(f"process_video_simple error: {e}")
        try:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=f"❌ Error: {str(e)[:150]}"
            )
        except Exception:
            pass
        session = get_session(user_id)
        if session:
            cleanup_files(session.get("video_path"), session.get("audio_path"))
        clear_session(user_id)
    finally:
        async with user_status_lock:
            user_processing_status[user_id] = False

async def process_video_with_audio_speed(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    try:
        session = get_session(user_id)
        if not session:
            raise Exception("No session found")

        video_path = session.get("video_path")
        audio_path = session.get("audio_path")
        srt_path = session.get("srt_path")
        original_video_duration = session.get("video_duration", 0)

        if not video_path or not audio_path:
            raise Exception("Missing video or audio path")
        if not os.path.exists(video_path):
            raise Exception(f"Video file not found: {video_path}")
        if not os.path.exists(audio_path):
            raise Exception(f"Audio file not found: {audio_path}")

        progress_msg = await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="⏳ Processing စတင်နေပါပြီ...\n\n🔄 မိနစ်အနည်းငယ် ကြာနိုင်ပါတယ်။"
        )

        await progress_msg.edit_text("🔄 Audio ကို ပြောင်းလဲနေပါပြီ... (၁/၅)")
        audio_mp3 = f"audio_{user_id}_{int(time.time())}.mp3"
        conversion_success = False

        try:
            audio = AudioSegment.from_file(audio_path)
            audio.export(audio_mp3, format="mp3", bitrate="192k")
            conversion_success = os.path.exists(audio_mp3) and os.path.getsize(audio_mp3) > 0
        except Exception:
            pass

        if not conversion_success:
            result = subprocess.run(
                ["ffmpeg", "-i", audio_path, "-acodec", "libmp3lame", "-ab", "192k", audio_mp3, "-y"],
                capture_output=True, timeout=300
            )
            conversion_success = result.returncode == 0 and os.path.exists(audio_mp3) and os.path.getsize(audio_mp3) > 0

        if not conversion_success:
            raise Exception("Audio conversion failed")

        await progress_msg.edit_text("📊 Video နဲ့ Audio အရှည်တွေကို တွက်ချက်နေပါပြီ... (၂/၅)")
        video_duration = get_video_duration_with_ffmpeg(video_path)
        if video_duration is None:
            raise Exception("Cannot get video duration")

        try:
            audio_clip = AudioFileClip(audio_mp3)
            audio_duration = audio_clip.duration
            audio_clip.close()
        except Exception:
            raise Exception("Cannot get audio duration")

        speed_factor = video_duration / audio_duration if audio_duration > 0 else 1.0

        await progress_msg.edit_text(
            f"🔄 Video ကို Audio အတိုင်း ပြင်ဆင်နေပါပြီ... (၃/၅)\n"
            f"⏱️ {video_duration:.1f}s → {audio_duration:.1f}s\n"
            f"📊 Speed: {speed_factor:.2f}x"
        )

        temp_video_path = f"temp_video_{user_id}_{int(time.time())}.mp4"
        if abs(speed_factor - 1.0) > 0.01:
            cmd = [
                "ffmpeg", "-i", video_path,
                "-filter:v", f"setpts={1/speed_factor}*PTS",
                "-an", "-c:v", "libx264", "-preset", "fast", "-crf", "23",
                "-y", temp_video_path
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
            if result.returncode != 0 or not os.path.exists(temp_video_path):
                raise Exception("FFmpeg speed adjust failed")
            video_for_combine = temp_video_path
        else:
            video_for_combine = video_path

        video_with_subs = video_for_combine
        if srt_path and os.path.exists(srt_path):
            await progress_msg.edit_text("📝 SRT subtitle ကို Video ထဲ ထည့်နေပါပြီ... (၄/၅)")
            video_with_subs = f"video_with_subs_{user_id}_{int(time.time())}.mp4"
            if burn_subtitles_ffmpeg(video_for_combine, srt_path, video_with_subs, speed_factor):
                video_for_combine = video_with_subs
            else:
                await progress_msg.edit_text("⚠️ SRT ထည့်ရာမှာ အမှားဖြစ်သွားပါတယ်။ Subtitle မပါဘဲ ဆက်လုပ်ပါမယ်။")
        else:
            await progress_msg.edit_text("ℹ️ SRT မပါသောကြောင့် subtitle မထည့်တော့ပါ။ (၄/၅)")

        await progress_msg.edit_text("🎵 Audio ကို Video နဲ့ ပေါင်းစပ်နေပါပြီ... (၅/၅)")
        final_output = f"final_{user_id}_{int(time.time())}.mp4"

        cmd_combine = [
            "ffmpeg", "-i", video_for_combine, "-i", audio_mp3,
            "-c:v", "copy", "-c:a", "aac",
            "-map", "0:v:0", "-map", "1:a:0",
            "-shortest", "-y", final_output
        ]
        result = subprocess.run(cmd_combine, capture_output=True, text=True, timeout=600)

        if result.returncode != 0:
            cmd_combine = [
                "ffmpeg", "-i", video_for_combine, "-i", audio_mp3,
                "-c:v", "libx264", "-c:a", "aac",
                "-map", "0:v:0", "-map", "1:a:0",
                "-shortest", "-preset", "fast", "-crf", "23",
                "-y", final_output
            ]
            result = subprocess.run(cmd_combine, capture_output=True, text=True, timeout=600)
            if result.returncode != 0:
                raise Exception("Combine failed")

        if not os.path.exists(final_output) or os.path.getsize(final_output) == 0:
            raise Exception("Final output empty")

        final_duration = get_video_duration_with_ffmpeg(final_output) or audio_duration

        await progress_msg.edit_text("📤 Video ကို ပို့နေပါပြီ...")
        with open(final_output, "rb") as f:
            caption = (
                f"✅ ပေါင်းစပ်ပြီးပါပြီ!\n"
                f"⏱️ နောက်ဆုံး: {final_duration:.1f}s\n"
                f"🎵 Audio: {audio_duration:.1f}s\n"
            )
            if original_video_duration > 0:
                caption += f"📊 မူရင်း: {original_video_duration:.1f}s\n"
            caption += f"📊 Speed: {speed_factor:.2f}x\n"
            if srt_path and os.path.exists(srt_path):
                caption += "📝 SRT ပါပါတယ်။"
            else:
                caption += "📝 SRT မပါပါ။"

            await update.effective_message.reply_video(
                video=InputFile(f, filename="combined_video.mp4"),
                caption=caption
            )

        await progress_msg.delete()
        cleanup_files(video_path, audio_path, audio_mp3, temp_video_path, final_output)
        if srt_path:
            cleanup_files(srt_path)
        if "video_with_subs" in locals() and os.path.exists(video_with_subs) and video_with_subs != video_for_combine:
            cleanup_files(video_with_subs)
        clear_session(user_id)

    except Exception as e:
        logger.error(f"process_video_with_audio_speed error: {e}")
        try:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=f"❌ Error: {str(e)[:150]}\n💡 Video သေးသေးလေး သုံးကြည့်ပါ။"
            )
        except Exception:
            pass
        session = get_session(user_id)
        if session:
            cleanup_files(
                session.get("video_path"),
                session.get("audio_path"),
                session.get("srt_path")
            )
        clear_session(user_id)
    finally:
        async with user_status_lock:
            user_processing_status[user_id] = False

# ========== Keyboards ==========
def get_settings_keyboard(user_id):
    speed = get_user_speed(user_id)
    voice_key = get_user_voice(user_id)
    mode = get_user_mode(user_id)
    voice_name = get_voice_display(voice_key)
    mode_text = "ရိုးရိုး (SRT မပါ)" if mode == "simple" else "ပုံမှန် (SRT ပါ)"
    keyboard = [
        [InlineKeyboardButton(f"🎤 အသံ: {voice_name}", callback_data="show_voice_menu")],
        [InlineKeyboardButton(f"⚡ အမြန်နှုန်း: {speed:.1f}x", callback_data="show_speed_menu")],
        [InlineKeyboardButton(f"📌 Mode: {mode_text}", callback_data="show_mode_menu")],
        [InlineKeyboardButton("📖 အကူအညီ", callback_data="help_menu")],
        [InlineKeyboardButton("❌ ပိတ်ရန်", callback_data="close_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_voice_menu_keyboard(user_id):
    current_voice = get_user_voice(user_id)
    keyboard = []
    for key, voice in VOICES.items():
        check = "✅ " if key == current_voice else ""
        keyboard.append([
            InlineKeyboardButton(
                f"{check}{voice['emoji']} {voice['name']} ({voice['gender']})",
                callback_data=f"set_voice_{key}"
            )
        ])
    keyboard.append([InlineKeyboardButton("🔙 နောက်သို့", callback_data="back_to_settings")])
    return InlineKeyboardMarkup(keyboard)

def get_speed_menu_keyboard(user_id):
    current_speed = get_user_speed(user_id)
    keyboard = []
    row = []
    for speed in SPEED_OPTIONS:
        check = "✅ " if speed == current_speed else ""
        row.append(InlineKeyboardButton(f"{check}{speed:.1f}x", callback_data=f"set_speed_{speed}"))
        if len(row) == 4:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("🔙 နောက်သို့", callback_data="back_to_settings")])
    return InlineKeyboardMarkup(keyboard)

def get_mode_menu_keyboard(user_id):
    current_mode = get_user_mode(user_id)
    keyboard = [
        [InlineKeyboardButton(
            f"{'✅ ' if current_mode == 'simple' else ''}ရိုးရိုး Mode (SRT မပါ)",
            callback_data="set_mode_simple"
        )],
        [InlineKeyboardButton(
            f"{'✅ ' if current_mode == 'auto' else ''}ပုံမှန် Mode (SRT ပါ)",
            callback_data="set_mode_auto"
        )],
        [InlineKeyboardButton("🔙 နောက်သို့", callback_data="back_to_settings")]
    ]
    return InlineKeyboardMarkup(keyboard)

# ========== Handlers ==========
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    save_user(user_id)
    await update.message.reply_text(
        "👋 မင်္ဂလာပါ!\n\n"
        "🎬 **YouTube / TikTok / Video** များကို မြန်မာလို အသံသွင်းပေးမယ်။\n\n"
        "📌 **ဘာလုပ်နိုင်သလဲ:**\n"
        "• YouTube Link ပို့ပါ\n"
        "• TikTok Link ပို့ပါ\n"
        "• Video ဖိုင် တိုက်ရိုက်ပို့ပါ\n\n"
        "⚙️ **ဆက်တင်များ:** /settings\n"
        "📖 **အသေးစိတ်:** /help",
        parse_mode=ParseMode.MARKDOWN
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📖 **အသုံးပြုနည်း**\n\n"
        "**Mode နှစ်မျိုး:**\n\n"
        "1️⃣ **ရိုးရိုး Mode (SRT မပါ)**\n"
        "   YouTube/TikTok/Video → Audio → စာသား → ဘာသာပြန် → TTS → ပေါင်းစပ်\n"
        "   📝 SRT မပါ\n\n"
        "2️⃣ **ပုံမှန် Mode (SRT ပါ)**\n"
        "   အထက်ပါအတိုင်း + SRT subtitle\n\n"
        "📌 YouTube/TikTok Link သို့မဟုတ် Video ဖိုင် ပို့ပါ။\n"
        "⚙️ /settings မှာ ပြောင်းနိုင်ပါတယ်။\n\n"
        "📌 **အမြန် command:**\n"
        "• /b - Thiha (ကျား)\n"
        "• /g - Nilar (မ)\n"
        "• /speed 1.6\n"
        "• /s - ရိုးရိုး Mode\n"
        "• /sk - ပုံမှန် Mode",
        parse_mode=ParseMode.MARKDOWN
    )

async def handle_youtube_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    url = update.message.text.strip()
    user_id = update.effective_user.id

    async with user_status_lock:
        if user_processing_status.get(user_id, False):
            await update.message.reply_text("⏳ သင်၏ ယခင်တောင်းဆိုချက်ကို ဆောင်ရွက်နေဆဲပါ။ စောင့်ပေးပါ။")
            return
        user_processing_status[user_id] = True

    async with global_processing_semaphore:
        status_msg = None
        try:
            save_user(user_id)
            clear_session(user_id)
            mode = get_user_mode(user_id)

            status_msg = await update.message.reply_text("⏳ Video ကို Download လုပ်နေပါတယ်...")

            video_path = None
            source = "Unknown"
            if "tiktok.com" in url:
                video_path = await download_tiktok_video(url)
                source = "TikTok"
            elif "youtube.com" in url or "youtu.be" in url:
                video_path = await download_youtube_video(url)
                source = "YouTube"
            else:
                await status_msg.edit_text("❌ YouTube/TikTok Link ကိုသာ ပို့ပေးပါ။")
                return

            if not video_path:
                await status_msg.edit_text(f"❌ {source} Download မအောင်မြင်ပါ။")
                return

            video_duration = get_video_duration_with_ffmpeg(video_path)
            save_session(user_id, {
                "video_path": video_path,
                "video_duration": video_duration,
                "is_voice": False,
                "mode": mode
            })

            await status_msg.edit_text("📤 Video ကို ပို့နေပါတယ်...")
            with open(video_path, "rb") as video_file:
                caption = f"🎬 {source} Video"
                if video_duration:
                    caption += f"\n⏱️ {video_duration:.1f}s"
                await update.message.reply_video(video=video_file, caption=caption)

            await status_msg.edit_text("🎤 Audio ကို စာသားပြောင်းနေပါတယ်...")
            transcript = await transcribe_audio(video_path)
            if not transcript:
                await status_msg.edit_text("❌ Transcription မအောင်မြင်ပါ။")
                return

            txt_filename = video_path.replace(".mp4", "_transcript.txt")
            with open(txt_filename, "w", encoding="utf-8") as f:
                f.write(transcript)

            save_session(user_id, {
                "transcript_text": transcript,
                "edited_text": transcript
            })

            with open(txt_filename, "rb") as txt_file:
                await update.message.reply_document(
                    document=txt_file,
                    filename="transcript.txt",
                    caption="📝 စာသားဖိုင်\n✍️ ပြင်ဆင်ပြီး ပြန်ပို့နိုင်ပါတယ်။"
                )

            await status_msg.edit_text("🔄 ဘာသာပြန်နေပါသည်...")
            loop = asyncio.get_event_loop()
            translated = await loop.run_in_executor(
                executor, translate_story_style, transcript, "en", "my"
            )

            save_session(user_id, {
                "translated_text": translated,
                "edited_text": translated
            })

            txt_filename = f"translated_{user_id}_{int(time.time())}.txt"
            with open(txt_filename, "w", encoding="utf-8") as f:
                f.write(translated)

            with open(txt_filename, "rb") as f:
                await context.bot.send_document(
                    chat_id=update.effective_chat.id,
                    document=f,
                    filename="translated_myanmar.txt",
                    caption="📝 မြန်မာလို ဘာသာပြန်"
                )

            await status_msg.edit_text("🎙 အသံဖိုင် ဖန်တီးနေပါသည်...")
            speed = get_user_speed(user_id)
            voice_key = get_user_voice(user_id)
            voice_name = get_voice_display(voice_key)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            output_file = f"tts_{user_id}_{timestamp}.mp3"
            srt_file = f"tts_srt_{user_id}_{timestamp}.srt"

            actual_duration = await convert_text_to_mp3(translated, output_file, speed, voice_key)

            if mode == "auto":
                srt_content = generate_srt_advanced(translated, voice_name, speed, actual_duration)
                with open(srt_file, "w", encoding="utf-8") as srt_f:
                    srt_f.write(srt_content)
                save_session(user_id, {"srt_path": srt_file})

            save_session(user_id, {"audio_path": output_file})

            with open(output_file, "rb") as audio:
                caption = (
                    f"🎧 အသံဖိုင်\n"
                    f"🎤 {voice_name}\n"
                    f"⚡ {speed:.1f}x\n"
                    f"⏱ {actual_duration:.1f}s"
                )
                caption += "\n📝 SRT ပါမယ်။" if mode == "auto" else "\n📝 SRT မပါပါ။"
                await context.bot.send_audio(
                    chat_id=update.effective_chat.id,
                    audio=audio,
                    caption=caption,
                    title=f"Voice_{voice_key}",
                    performer="AI Voice"
                )

            if mode == "auto" and os.path.exists(srt_file):
                with open(srt_file, "rb") as document:
                    await context.bot.send_document(
                        chat_id=update.effective_chat.id,
                        document=document,
                        filename="subtitle.srt",
                        caption="📝 Subtitle (SRT)"
                    )

            await status_msg.edit_text("🎬 Video ပေါင်းစပ်နေပါသည်...")
            if mode == "simple":
                await process_video_simple(update, context, user_id)
            else:
                await process_video_with_audio_speed(update, context, user_id)

        except Exception as e:
            logger.error(f"handle_youtube_link error: {e}")
            try:
                if status_msg:
                    await status_msg.edit_text(f"❌ Error: {str(e)[:150]}")
            except Exception:
                pass
        finally:
            async with user_status_lock:
                user_processing_status[user_id] = False

async def handle_video_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    async with user_status_lock:
        if user_processing_status.get(user_id, False):
            await update.message.reply_text("⏳ ယခင်တောင်းဆိုချက် ဆောင်ရွက်နေဆဲပါ။")
            return
        user_processing_status[user_id] = True

    async with global_processing_semaphore:
        status_msg = None
        try:
            save_user(user_id)
            clear_session(user_id)
            mode = get_user_mode(user_id)

            status_msg = await update.message.reply_text("⏳ Video ကို လက်ခံနေပါတယ်...")

            video = update.message.video
            if not video:
                await status_msg.edit_text("❌ Video ဖိုင်မတွေ့ပါ။")
                return

            video_file = await video.get_file()
            video_path = os.path.join(TEMP_FOLDER, f"uploaded_{user_id}_{int(time.time())}.mp4")
            await video_file.download_to_drive(video_path)

            if not os.path.exists(video_path) or os.path.getsize(video_path) == 0:
                await status_msg.edit_text("❌ Download မအောင်မြင်ပါ။")
                return

            video_duration = video.duration or get_video_duration_with_ffmpeg(video_path)
            save_session(user_id, {
                "video_path": video_path,
                "video_duration": video_duration,
                "is_voice": False,
                "mode": mode
            })

            await status_msg.edit_text(
                f"✅ Video လက်ခံရရှိပါပြီ။\n⏱️ {video_duration:.1f}s\n\n🎤 Transcription လုပ်နေပါတယ်..."
            )

            transcript = await transcribe_audio(video_path)
            if not transcript:
                await status_msg.edit_text("❌ Transcription မအောင်မြင်ပါ။")
                return

            txt_filename = video_path.replace(".mp4", "_transcript.txt")
            with open(txt_filename, "w", encoding="utf-8") as f:
                f.write(transcript)

            save_session(user_id, {
                "transcript_text": transcript,
                "edited_text": transcript
            })

            with open(txt_filename, "rb") as txt_file:
                await update.message.reply_document(
                    document=txt_file,
                    filename="transcript.txt",
                    caption="📝 စာသားဖိုင်\n✍️ ပြင်ဆင်ပြီး ပြန်ပို့နိုင်ပါတယ်။"
                )

            await status_msg.edit_text("🔄 ဘာသာပြန်နေပါသည်...")
            loop = asyncio.get_event_loop()
            translated = await loop.run_in_executor(
                executor, translate_story_style, transcript, "en", "my"
            )

            save_session(user_id, {
                "translated_text": translated,
                "edited_text": translated
            })

            txt_filename = f"translated_{user_id}_{int(time.time())}.txt"
            with open(txt_filename, "w", encoding="utf-8") as f:
                f.write(translated)

            with open(txt_filename, "rb") as f:
                await context.bot.send_document(
                    chat_id=update.effective_chat.id,
                    document=f,
                    filename="translated_myanmar.txt",
                    caption="📝 မြန်မာလို ဘာသာပြန်"
                )

            await status_msg.edit_text("🎙 အသံဖိုင် ဖန်တီးနေပါသည်...")
            speed = get_user_speed(user_id)
            voice_key = get_user_voice(user_id)
            voice_name = get_voice_display(voice_key)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            output_file = f"tts_{user_id}_{timestamp}.mp3"
            srt_file = f"tts_srt_{user_id}_{timestamp}.srt"

            actual_duration = await convert_text_to_mp3(translated, output_file, speed, voice_key)

            if mode == "auto":
                srt_content = generate_srt_advanced(translated, voice_name, speed, actual_duration)
                with open(srt_file, "w", encoding="utf-8") as srt_f:
                    srt_f.write(srt_content)
                save_session(user_id, {"srt_path": srt_file})

            save_session(user_id, {"audio_path": output_file})

            with open(output_file, "rb") as audio:
                caption = (
                    f"🎧 အသံဖိုင်\n"
                    f"🎤 {voice_name}\n"
                    f"⚡ {speed:.1f}x\n"
                    f"⏱ {actual_duration:.1f}s"
                )
                caption += "\n📝 SRT ပါမယ်။" if mode == "auto" else "\n📝 SRT မပါပါ။"
                await context.bot.send_audio(
                    chat_id=update.effective_chat.id,
                    audio=audio,
                    caption=caption,
                    title=f"Voice_{voice_key}",
                    performer="AI Voice"
                )

            if mode == "auto" and os.path.exists(srt_file):
                with open(srt_file, "rb") as document:
                    await context.bot.send_document(
                        chat_id=update.effective_chat.id,
                        document=document,
                        filename="subtitle.srt",
                        caption="📝 Subtitle (SRT)"
                    )

            await status_msg.edit_text("🎬 Video ပေါင်းစပ်နေပါသည်...")
            if mode == "simple":
                await process_video_simple(update, context, user_id)
            else:
                await process_video_with_audio_speed(update, context, user_id)

        except Exception as e:
            logger.error(f"handle_video_file error: {e}")
            try:
                if status_msg:
                    await status_msg.edit_text(f"❌ Error: {str(e)[:150]}")
            except Exception:
                pass
        finally:
            async with user_status_lock:
                user_processing_status[user_id] = False

async def handle_transcript_edit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text

    if text.startswith("/") or text.startswith("http"):
        return

    async with user_status_lock:
        if user_processing_status.get(user_id, False):
            await update.message.reply_text("⏳ ယခင်တောင်းဆိုချက် ဆောင်ရွက်နေဆဲပါ။")
            return
        user_processing_status[user_id] = True

    async with global_processing_semaphore:
        try:
            session = get_session(user_id)
            if not session:
                await update.message.reply_text("⚠️ ကျေးဇူးပြုပြီး YouTube/TikTok Link ကို အရင်ပို့ပါ။")
                return

            mode = get_user_mode(user_id)
            save_session(user_id, {"edited_text": text})

            await update.message.reply_text(
                "📝 စာသားကို ရရှိပါပြီ။\n\n🔄 မြန်မာလို ဘာသာပြန်နေပါသည်..."
            )

            loop = asyncio.get_event_loop()
            translated = await loop.run_in_executor(
                executor, translate_story_style, text, "en", "my"
            )

            save_session(user_id, {"translated_text": translated})

            txt_filename = f"translated_{user_id}_{int(time.time())}.txt"
            with open(txt_filename, "w", encoding="utf-8") as f:
                f.write(translated)

            with open(txt_filename, "rb") as f:
                await context.bot.send_document(
                    chat_id=update.effective_chat.id,
                    document=f,
                    filename="translated_myanmar.txt",
                    caption="📝 မြန်မာလို ဘာသာပြန်"
                )

            await update.message.reply_text("🎙 အသံဖိုင် ဖန်တီးနေပါသည်...")

            speed = get_user_speed(user_id)
            voice_key = get_user_voice(user_id)
            voice_name = get_voice_display(voice_key)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            output_file = f"tts_{user_id}_{timestamp}.mp3"
            srt_file = f"tts_srt_{user_id}_{timestamp}.srt"

            actual_duration = await convert_text_to_mp3(translated, output_file, speed, voice_key)

            if mode == "auto":
                srt_content = generate_srt_advanced(translated, voice_name, speed, actual_duration)
                with open(srt_file, "w", encoding="utf-8") as srt_f:
                    srt_f.write(srt_content)
                save_session(user_id, {"srt_path": srt_file})

            save_session(user_id, {"audio_path": output_file})

            with open(output_file, "rb") as audio:
                caption = (
                    f"🎧 အသံဖိုင်\n"
                    f"🎤 {voice_name}\n"
                    f"⚡ {speed:.1f}x\n"
                    f"⏱ {actual_duration:.1f}s"
                )
                caption += "\n📝 SRT ပါမယ်။" if mode == "auto" else "\n📝 SRT မပါပါ။"
                await context.bot.send_audio(
                    chat_id=update.effective_chat.id,
                    audio=audio,
                    caption=caption,
                    title=f"Voice_{voice_key}",
                    performer="AI Voice"
                )

            if mode == "auto" and os.path.exists(srt_file):
                with open(srt_file, "rb") as document:
                    await context.bot.send_document(
                        chat_id=update.effective_chat.id,
                        document=document,
                        filename="subtitle.srt",
                        caption="📝 Subtitle (SRT)"
                    )

            await update.message.reply_text("🎬 Video ပေါင်းစပ်နေပါသည်...")
            if mode == "simple":
                await process_video_simple(update, context, user_id)
            else:
                await process_video_with_audio_speed(update, context, user_id)

        except Exception as e:
            logger.error(f"handle_transcript_edit error: {e}")
            await update.message.reply_text(f"❌ Error: {str(e)[:150]}")
        finally:
            async with user_status_lock:
                user_processing_status[user_id] = False

async def voice_command_b(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    save_user(user_id, voice="thiha")
    await update.message.reply_text(
        f"✅ အသံကို *{get_voice_display('thiha')}* သို့ ပြောင်းလိုက်ပါပြီ။",
        parse_mode=ParseMode.MARKDOWN
    )

async def voice_command_g(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    save_user(user_id, voice="nilar")
    await update.message.reply_text(
        f"✅ အသံကို *{get_voice_display('nilar')}* သို့ ပြောင်းလိုက်ပါပြီ။",
        parse_mode=ParseMode.MARKDOWN
    )

async def speed_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    args = update.message.text.split()
    if len(args) < 2:
        current = get_user_speed(user_id)
        await update.message.reply_text(
            f"⚡ လက်ရှိ Speed: *{current:.1f}x*\n\n"
            f"ပြောင်းရန်: `/speed 1.6`",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    try:
        speed = float(args[1])
        if speed < 0.8 or speed > 2.0:
            await update.message.reply_text("⚠️ Speed ကို 0.8 - 2.0 ကြားမှာသာ ရွေးနိုင်ပါတယ်။")
            return
        closest = min(SPEED_OPTIONS, key=lambda x: abs(x - speed))
        save_user(user_id, speed=closest)
        await update.message.reply_text(
            f"✅ Speed ကို *{closest:.1f}x* သို့ ပြောင်းလိုက်ပါပြီ။",
            parse_mode=ParseMode.MARKDOWN
        )
    except ValueError:
        await update.message.reply_text("⚠️ နံပါတ်ဖြင့် ရိုက်ပါ။ ဥပမာ: `/speed 1.6`")

async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await update.message.reply_text(
        "⚙️ **ဆက်တင်များ**\n\nအောက်ပါခလုတ်များကို နှိပ်၍ ပြောင်းလဲနိုင်ပါတယ်။",
        reply_markup=get_settings_keyboard(user_id),
        parse_mode=ParseMode.MARKDOWN
    )

async def settings_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    data = query.data

    if data == "show_voice_menu":
        await query.edit_message_text(
            "🎤 **အသံရွေးချယ်ရန်**",
            reply_markup=get_voice_menu_keyboard(user_id),
            parse_mode=ParseMode.MARKDOWN
        )
    elif data == "show_speed_menu":
        await query.edit_message_text(
            "⚡ **အမြန်နှုန်းရွေးချယ်ရန်**",
            reply_markup=get_speed_menu_keyboard(user_id),
            parse_mode=ParseMode.MARKDOWN
        )
    elif data == "show_mode_menu":
        await query.edit_message_text(
            "📌 **Mode ရွေးချယ်ရန်**\n\n"
            "• **ရိုးရိုး Mode** - SRT မပါ (ပိုမြန်)\n"
            "• **ပုံမှန် Mode** - SRT ပါ",
            reply_markup=get_mode_menu_keyboard(user_id),
            parse_mode=ParseMode.MARKDOWN
        )
    elif data.startswith("set_voice_"):
        voice_key = data.replace("set_voice_", "")
        if voice_key in VOICES:
            save_user(user_id, voice=voice_key)
            await query.edit_message_text(
                f"✅ အသံကို *{get_voice_display(voice_key)}* သို့ ပြောင်းလိုက်ပါပြီ။",
                reply_markup=get_settings_keyboard(user_id),
                parse_mode=ParseMode.MARKDOWN
            )
    elif data.startswith("set_speed_"):
        speed = float(data.replace("set_speed_", ""))
        save_user(user_id, speed=speed)
        await query.edit_message_text(
            f"✅ အမြန်နှုန်းကို *{speed:.1f}x* သို့ ပြောင်းလိုက်ပါပြီ။",
            reply_markup=get_settings_keyboard(user_id),
            parse_mode=ParseMode.MARKDOWN
        )
    elif data == "set_mode_simple":
        set_user_mode(user_id, "simple")
        await query.edit_message_text(
            "✅ **ရိုးရိုး Mode (SRT မပါ)** သို့ ပြောင်းလိုက်ပါပြီ။",
            reply_markup=get_settings_keyboard(user_id),
            parse_mode=ParseMode.MARKDOWN
        )
    elif data == "set_mode_auto":
        set_user_mode(user_id, "auto")
        await query.edit_message_text(
            "✅ **ပုံမှန် Mode (SRT ပါ)** သို့ ပြောင်းလိုက်ပါပြီ။",
            reply_markup=get_settings_keyboard(user_id),
            parse_mode=ParseMode.MARKDOWN
        )
    elif data == "back_to_settings":
        await query.edit_message_text(
            "⚙️ **ဆက်တင်များ**",
            reply_markup=get_settings_keyboard(user_id),
            parse_mode=ParseMode.MARKDOWN
        )
    elif data == "help_menu":
        await query.edit_message_text(
            "📖 အကူအညီ — /help ကို ကြည့်ပါ။",
            reply_markup=get_settings_keyboard(user_id),
            parse_mode=ParseMode.MARKDOWN
        )
    elif data == "close_menu":
        await query.edit_message_text("✅ ဆက်တင်များကို ပိတ်လိုက်ပါပြီ။")

async def mode_s(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    set_user_mode(user_id, "simple")
    await update.message.reply_text(
        "✅ **ရိုးရိုး Mode (SRT မပါ)** သို့ ပြောင်းလိုက်ပါပြီ။\n\n"
        "📌 Video + Audio ကိုပဲ ပေါင်းပေးမယ်။\n"
        "⏱ ပိုမြန်ဆန်ပါတယ်။",
        parse_mode=ParseMode.MARKDOWN
    )

async def mode_sk(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    set_user_mode(user_id, "auto")
    await update.message.reply_text(
        "✅ **ပုံမှန် Mode (SRT ပါ)** သို့ ပြောင်းလိုက်ပါပြီ။\n\n"
        "📌 SRT subtitle ပါမယ်။",
        parse_mode=ParseMode.MARKDOWN
    )

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Error: {context.error}")

# ========== Main ==========
def main():
    init_db()
    delete_old_sessions()

    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .connect_timeout(60.0)
        .read_timeout(60.0)
        .write_timeout(60.0)
        .build()
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("settings", settings_command))
    application.add_handler(CommandHandler("s", mode_s))
    application.add_handler(CommandHandler("sk", mode_sk))
    application.add_handler(CommandHandler("b", voice_command_b))
    application.add_handler(CommandHandler("g", voice_command_g))
    application.add_handler(CommandHandler("speed", speed_command))

    application.add_handler(CallbackQueryHandler(settings_callback))

    application.add_handler(
        MessageHandler(
            filters.TEXT & \~filters.COMMAND & filters.Regex(r"youtube\.com|youtu\.be|tiktok\.com"),
            handle_youtube_link
        )
    )
    application.add_handler(MessageHandler(filters.VIDEO, handle_video_file))
    application.add_handler(MessageHandler(filters.TEXT & \~filters.COMMAND, handle_transcript_edit))

    application.add_error_handler(error_handler)

    logger.info("Bot starting...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()