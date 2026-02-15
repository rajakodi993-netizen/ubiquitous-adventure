#!/usr/bin/env python3
import os
import sys
import json
import time
import shutil
import logging
import signal
import pathlib
import subprocess
import requests
from datetime import datetime

# === ENV ===
BASE_DIR = os.environ.get('GITHUB_WORKSPACE', os.path.dirname(os.path.abspath(__file__)))
PHOTO_DIR = os.getenv('PHOTO_DIR', os.path.join(BASE_DIR, 'photos'))
ARCHIVE_DIR = os.getenv('ARCHIVE_DIR', os.path.join(BASE_DIR, 'archive'))
LOG_FILE = os.getenv('LOG_FILE', os.path.join(BASE_DIR, 'logs', 'gallery.log'))
NOTIF_CHANNEL_ID = os.getenv('NOTIF_CHANNEL_ID', '')
UPLOAD_CHANNEL_ID = os.getenv('UPLOAD_CHANNEL_ID', '')
BOT_TOKEN = os.getenv('BOT_TOKEN', '')
TT_COOKIES = os.getenv('TT_COOKIES', os.path.join(BASE_DIR, 'data', 'cookies.txt'))
ACCOUNTS_FILE = os.getenv('ACCOUNTS_FILE', os.path.join(BASE_DIR, 'data', 'tiktok_accounts.json'))
SLEEP_SECONDS = int(os.getenv('SLEEP_SECONDS', '2'))

# === LOGGING ===
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE)
    ]
)
logger = logging.getLogger('gallery_worker')

# === TELEGRAM ===
def tg_media_group(media_files: list, caption: str = "") -> bool:
    channel = UPLOAD_CHANNEL_ID or NOTIF_CHANNEL_ID
    if not (BOT_TOKEN and channel) or not media_files:
        return False
    
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMediaGroup"
    files = {}
    media = []

    for i, file_path in enumerate(media_files):
        if not os.path.exists(file_path): continue
        file_key = f"photo{i}"
        files[file_key] = open(file_path, "rb")
        media_item = {"type": "photo", "media": f"attach://{file_key}"}
        if i == 0 and caption:
            media_item["caption"] = caption
            media_item["parse_mode"] = "HTML"
        media.append(media_item)

    if not media: return False

    try:
        r = requests.post(url, data={"chat_id": channel, "media": json.dumps(media)}, files=files, timeout=(60, 300))
        if r.status_code == 429:
            retry = int(r.headers.get("Retry-After", "10"))
            logger.warning(f"Rate limited. Menunggu {retry}s...")
            time.sleep(retry)
            return False 
        r.raise_for_status()
        return True
    except Exception as e:
        logger.error(f"Gagal kirim album: {e}")
        return False
    finally:
        for f in files.values():
            f.close()

# === UTIL ===
STOP = False
def _sig_handler(signum, frame):
    global STOP
    STOP = True
    logger.warning("Signal diterima. Berhenti...")
signal.signal(signal.SIGINT, _sig_handler)
signal.signal(signal.SIGTERM, _sig_handler)

def load_accounts() -> list:
    try:
        if os.path.exists(ACCOUNTS_FILE):
            with open(ACCOUNTS_FILE, 'r') as f:
                data = json.load(f)
            return [item.strip() for item in data if isinstance(item, str) and item.strip()]
    except Exception as e:
        logger.error(f"Gagal baca akun: {e}")
    return []

def process_gallery(url: str, limit: int = 0):
    if STOP: return
    
    if '@' in url:
        username = url.split('@')[-1].split('/')[0]
    else:
        logger.warning(f"URL skip: {url}")
        return

    logger.info(f"📸 Memproses Gallery: @{username}")
    
    photo_archive = os.path.join(ARCHIVE_DIR, f"{username}_photos.txt")
    user_photo_dir = os.path.join(PHOTO_DIR, username)
    os.makedirs(user_photo_dir, exist_ok=True)

    # Clean up folder user sebelum mulai (jika ada sisa run sebelumnya)
    # Tidak perlu clean up ekstrem, cukup start fresh logic
    
    existing_photos = set(os.listdir(user_photo_dir))

    # Run Gallery-DL
    cmd = [
        sys.executable, "-m", "gallery_dl", url,
        "--cookies", TT_COOKIES,
        "--download-archive", photo_archive,
        "--filter", "extension in ('jpg', 'jpeg', 'png', 'webp')",
        "--no-skip",
        "--write-metadata",
        "-D", user_photo_dir,
        "-o", "filename={id}_{num}.{extension}",
    ]
    
    # Exec
    try:
        subprocess.run(cmd, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    except FileNotFoundError:
        logger.error("Gallery-DL tidak ditemukan/terinstall.")
        return

    # Check new files
    if not os.path.exists(user_photo_dir): return
    
    current_files = set(os.listdir(user_photo_dir))
    new_files = sorted(current_files - existing_files)
    
    # Filter only images
    new_photos = [f for f in new_files if f.lower().endswith(('.jpg', '.jpeg', '.png', '.webp'))]
    
    if not new_photos:
        logger.info(f"Tidak ada foto baru untuk @{username}")
        return

    logger.info(f"Ditemukan {len(new_photos)} foto baru.")

    # Grouping
    grouped = {}
    for f in new_photos:
        parts = f.split('_')
        if len(parts) >= 2:
            pid = parts[0]
            if pid not in grouped: grouped[pid] = []
            grouped[pid].append(os.path.join(user_photo_dir, f))
    
    # Upload
    for pid, paths in grouped.items():
        if STOP: break
        
        # Sort
        paths.sort(key=lambda x: int(os.path.basename(x).split('_')[-1].split('.')[0]))
        
        # Metadata
        caption = f"📸 @{username} - {pid}"
        json_path = os.path.splitext(paths[0])[0] + ".json"
        
        # Try find json if suffix is _0 or _1. Or just search directory for {pid}_*.json if missing
        if not os.path.exists(json_path):
             # Fallback search
             candidates = [os.path.join(user_photo_dir, x) for x in current_files if x.startswith(f"{pid}_") and x.endswith(".json")]
             if candidates: json_path = candidates[0]

        if os.path.exists(json_path):
            try:
                with open(json_path, 'r', encoding='utf-8') as jf:
                    meta = json.load(jf)
                    desc = meta.get('description') or meta.get('title') or ""
                    date = meta.get('date') or ""
                    caption = f"📸 <b>{date}</b>\n{desc[:800]}\n\n🔗 <a href='https://www.tiktok.com/@{username}/video/{pid}'>Link</a>"
            except: pass

        # Chunking 10
        for i in range(0, len(paths), 10):
            chunk = paths[i:i+10]
            cap = caption if i == 0 else ""
            
            if tg_media_group(chunk, cap):
                logger.info(f"Sent album {pid} ({len(chunk)} pics)")
                # Delete uploaded
                for p in chunk:
                    try: os.remove(p)
                    except: pass
                    # remove json
                    try: os.remove(os.path.splitext(p)[0] + ".json")
                    except: pass
            else:
                logger.error(f"Failed to send album {pid}")
            
            time.sleep(3)

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    accounts = load_accounts()
    logger.info(f"Start Gallery Worker. {len(accounts)} accounts.")
    
    for i, acc in enumerate(accounts, 1):
        if STOP: break
        process_gallery(acc)
        if SLEEP_SECONDS > 0: time.sleep(SLEEP_SECONDS)

if __name__ == "__main__":
    main()
