# Myanmar Video Voice Bot

Telegram bot that downloads YouTube/TikTok videos, transcribes with Whisper, translates to Myanmar, generates TTS (Thiha/Nilar), and combines with optional SRT subtitles.

## Deploy on Railway

1. Push this repo to **GitHub** (recommend **private** repo)
2. Go to [Railway.app](https://railway.app) → New Project → Deploy from GitHub
3. Add Environment Variable:
   - `BOT_TOKEN` = your Telegram bot token (from @BotFather)
4. Railway will detect Python + install from `requirements.txt`
5. Start command is already set in `Procfile` (`worker: python main.py`)

## Notes
- Needs enough RAM (Whisper model is heavy)
- First run downloads Whisper model → may take time
- Myanmar fonts may not be available → subtitles might fallback