# collectors/telegram_collector.py

# --- BEGIN: ensure repo root in PYTHONPATH (add this at top of file) ---
import os, sys
HERE = os.path.dirname(__file__)
CANDIDATE = os.path.abspath(os.path.join(HERE, ".."))
if os.path.isdir(os.path.join(CANDIDATE, "utils")) and CANDIDATE not in sys.path:
    sys.path.insert(0, CANDIDATE)
else:
    REPO_ROOT = os.path.abspath(os.getcwd())
    if REPO_ROOT not in sys.path:
        sys.path.insert(0, REPO_ROOT)
# --- END ---

import asyncio
import hashlib
from datetime import datetime, timezone
import os

# Telethon
from telethon.sync import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import User

# Supabase helper
from utils.supabase_client import upsert_posts

# ====== Settings (from env) ======
def _get_env(name, default=""):
    v = os.environ.get(name, default)
    return v.strip() if isinstance(v, str) else v

API_ID_STR = _get_env("TELEGRAM_API_ID", "")
API_HASH = _get_env("TELEGRAM_API_HASH", "")
STRING = _get_env("TELEGRAM_STRING_SESSION", "")

# Optional: read session from local file (LOCAL TEST ONLY).
session_file = _get_env("TELEGRAM_SESSION_FILE", ".telegram_session")
if not STRING and os.path.exists(session_file):
    try:
        with open(session_file, "r", encoding="utf-8") as f:
            raw = f.read().strip()
            if raw:
                STRING = raw
                print("Info: Loaded TELEGRAM_STRING_SESSION from local file (for local testing).")
    except Exception as e:
        print("Warning: couldn't read local session file:", e)

# Validate API_ID
if not API_ID_STR or not API_HASH:
    print("ERROR: TELEGRAM_API_ID or TELEGRAM_API_HASH is missing. Add them as GitHub secrets.")
    raise SystemExit(2)
try:
    API_ID = int(API_ID_STR)
except Exception:
    print("ERROR: TELEGRAM_API_ID must be an integer. Check the secret value.")
    raise SystemExit(2)

# Validate STRING
if not STRING:
    print("ERROR: TELEGRAM_STRING_SESSION is missing or empty.")
    print(" -> Generate a string session locally (see README) and add it as a GitHub secret named TELEGRAM_STRING_SESSION.")
    raise SystemExit(2)

# Masked debug (length only)
print(f"DEBUG: TELEGRAM_STRING_SESSION length = {len(STRING)} (value masked).")

# ---------------- Settings from env ----------------
CHANNELS = _get_env("TG_CHANNELS", "dmski_1,akbardrwz,Druzeresistance").split(",")
MAX_POSTS = int(_get_env("TG_MAX_POSTS", "200"))
PREFILTER = _get_env("TG_PREFILTER", "السويداء,الساحل,اللاذقية,طرطوس,قتل,اشتباك,طائفي").split(",")

# ---------------- Helpers ----------------
def build_urls(message):
    source_url = ""
    post_url = ""
    chat = getattr(message, "chat", None)
    if chat and getattr(chat, "username", None):
        uname = chat.username
        source_url = f"https://t.me/{uname}"
        post_url = f"https://t.me/{uname}/{message.id}"
    else:
        chan_id = None
        if hasattr(message, "peer_id") and hasattr(message.peer_id, "channel_id"):
            chan_id = message.peer_id.channel_id
        elif chat and hasattr(chat, "id"):
            try:
                chan_id = abs(int(chat.id))
            except Exception:
                chan_id = None
        if chan_id:
            source_url = f"https://t.me/c/{chan_id}/{message.id}"
            post_url  = f"https://t.me/c/{chan_id}/{message.id}"
    return source_url, post_url

def extract_row(message):
    msg_date = ""
    if message.date:
        try:
            msg_date = message.date.astimezone(timezone.utc).isoformat()
        except Exception:
            msg_date = message.date.isoformat()

    source_name = ""
    source_url, post_url = build_urls(message)
    chat = getattr(message, "chat", None)
    if chat and getattr(chat, "title", None):
        source_name = chat.title

    author = ""
    if message.sender and isinstance(message.sender, User):
        if message.sender.username:
            author = message.sender.username
        elif message.sender.first_name and message.sender.last_name:
            author = f"{message.sender.first_name} {message.sender.last_name}"
        elif message.sender.first_name:
            author = message.sender.first_name

    text = message.text or ""
    return {
        "platform": "Telegram",
        "source_name": source_name,
        "source_url": source_url,
        "post_id": message.id,
        "post_url": post_url,
        "author": author,
        "text": text,
        "language": "ar",
        "datetime_utc": msg_date,
        "datetime_local": "",
        "admin_area": "",
        "locality": "",
        "geofenced_area": "",
        "tension_level": "",
        "media_urls": "Media present (URL TBD)" if message.media else "",
        "shares": "",
        "likes": "",
        "comments": "",
        "collected_at_utc": datetime.utcnow().isoformat(),
        "collector": "SHAJARA-Agent",
        "hash": hashlib.sha256((text or "").encode("utf-8")).hexdigest() if text else None,
        "notes": "",
    }

# ---------------- Main ----------------
async def run():
    rows = []
    try:
        async with TelegramClient(StringSession(STRING), API_ID, API_HASH) as client:
            print("Telegram client started — fetching messages...")
            for ch in CHANNELS:
                ch = ch.strip()
                if not ch:
                    continue
                print(f"Scanning channel: {ch}")
                try:
                    async for message in client.iter_messages(ch):
                        if len(rows) >= MAX_POSTS:
                            break
                        if not getattr(message, "text", None):
                            continue
                        if PREFILTER:
                            t = message.text or ""
                            if not any(k in t for k in PREFILTER):
                                if message.id % 10 != 0:
                                    continue
                        rows.append(extract_row(message))
                    if len(rows) >= MAX_POSTS:
                        break
                except Exception as e:
                    print(f"Warning: failed scanning {ch}: {e}")
    except Exception as e:
        print(f"ERROR: Telegram client failed to start or authenticate: {e}")
        print(" -> Common causes: invalid STRING_SESSION, 2FA required, or API_ID/API_HASH incorrect.")
        return

    if rows:
        try:
            upsert_posts(rows)
            print(f"Inserted {len(rows)} Telegram rows into Supabase.")
        except Exception as e:
            print(f"ERROR: Failed to upsert to Supabase: {e}")
    else:
        print("No Telegram rows collected.")

if __name__ == "__main__":
    asyncio.run(run())
