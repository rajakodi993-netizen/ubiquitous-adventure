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
import html
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
SLEEP_SECONDS = int(os.getenv('SLEEP_SECONDS', '5'))

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
def tg_message(text: str) -> bool:
    channel = NOTIF_CHANNEL_ID
    if not (BOT_TOKEN and channel):
        return False
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, data={"chat_id": channel, "text": text}, timeout=10)
        if r.status_code == 429:
             time.sleep(int(r.headers.get("Retry-After", 5)))
             return tg_message(text)
        r.raise_for_status()
        return True
    except Exception as e:
        logger.error(f"Gagal kirim pesan TG: {e}")
        return False

# Return message_id of the first item (album)
def tg_media_group(media_files: list, caption: str = "") -> int:
    channel = UPLOAD_CHANNEL_ID or NOTIF_CHANNEL_ID
    if not (BOT_TOKEN and channel) or not media_files:
        return -1
    
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMediaGroup"
    files = {}
    media = []

    for i, file_path in enumerate(media_files):
        if not os.path.exists(file_path): continue
        file_key = f"photo{i}"
        files[file_key] = open(file_path, "rb")
        media_item = {"type": "photo", "media": f"attach://{file_key}"}
        # Caption only on first item if provided AND we are NOT sending separate reply
        # But user wants reply caption. So we can put simple caption here or None.
        # User request: "dikirim setelah foto lalu audio nya ngereply foto dan captionnya buat blockquote"
        # So maybe NO caption on album? Or simple caption?
        # Let's put simple caption on album, proper caption on audio reply.
        if i == 0 and caption:
             media_item["caption"] = caption
             media_item["parse_mode"] = "HTML"
        media.append(media_item)

    if not media: return -1

    try:
        r = requests.post(url, data={"chat_id": channel, "media": json.dumps(media)}, files=files, timeout=(60, 300))
        if r.status_code == 429:
            retry = int(r.headers.get("Retry-After", "10"))
            logger.warning(f"Rate limited. Menunggu {retry}s...")
            time.sleep(retry)
            return tg_media_group(media_files, caption) 
        r.raise_for_status()
        
        # Get message_id
        res = r.json()
        if res.get("ok"):
            # Result is array of messages
            msgs = res.get("result", [])
            if msgs:
                return msgs[0]["message_id"]
        return -1
    except Exception as e:
        logger.error(f"Gagal kirim album: {e}")
        return -1
    finally:
        for f in files.values():
            f.close()

def tg_send_audio_reply(audio_path: str, caption: str, reply_to_id: int) -> bool:
    channel = UPLOAD_CHANNEL_ID or NOTIF_CHANNEL_ID
    if not (BOT_TOKEN and channel and os.path.exists(audio_path)):
        return False
    
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendAudio"
    try:
        with open(audio_path, 'rb') as f:
            data = {
                "chat_id": channel,
                "caption": caption,
                "parse_mode": "HTML",
                "reply_to_message_id": reply_to_id
            }
            r = requests.post(url, data=data, files={"audio": f}, timeout=60)
            
            if r.status_code == 429:
                retry = int(r.headers.get("Retry-After", 10))
                time.sleep(retry)
                return tg_send_audio_reply(audio_path, caption, reply_to_id)
            
            r.raise_for_status()
            return True
    except Exception as e:
        logger.error(f"Gagal kirim audio: {e}")
        return False

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

def format_caption(meta: dict, username: str, pid: str) -> str:
    desc = meta.get('description') or meta.get('title') or ""
    date = meta.get('date') or ""
    url = f"https://www.tiktok.com/@{username}/video/{pid}"
    
    # Escape HTML
    esc = lambda s: html.escape(str(s), quote=True)
    
    # Blockquote format requested:
    # blockquote = // 
    # // Judul Video
    # // Nama akun
    # // kapan waktu di posting
    # // url 
    
    return (
        f"<blockquote><b>{esc(desc[:800])}</b></blockquote>\n"
        f"<blockquote>👤 @{esc(username)}</blockquote>\n"
        f"<blockquote>📅 {esc(date)}</blockquote>\n"
        f"<blockquote>🔗 <a href='{url}'>Link Posts</a></blockquote>"
    )

def process_gallery(url: str, limit: int = 0) -> int:
    if STOP: return 0
    
    if '@' in url:
        username = url.split('@')[-1].split('/')[0]
    else:
        logger.warning(f"URL skip: {url}")
        return 0

    logger.info(f"📸 Memproses Gallery: @{username}")
    
    photo_archive = os.path.join(ARCHIVE_DIR, f"{username}_photos.txt")
    user_photo_dir = os.path.join(PHOTO_DIR, username)
    os.makedirs(user_photo_dir, exist_ok=True)

    existing_files = set(os.listdir(user_photo_dir))

    # Run Gallery-DL (Include audio mp3/m4a)
    cmd = [
        sys.executable, "-m", "gallery_dl", url,
        "--cookies", TT_COOKIES,
        "--download-archive", photo_archive,
        "--filter", "extension in ('jpg', 'jpeg', 'png', 'webp', 'mp3', 'm4a')", # Add audio extensions
        "--no-skip",
        "--write-metadata",
        "-D", user_photo_dir,
        "-o", "filename={id}_{num}.{extension}",
    ]
    
    try:
        subprocess.run(cmd, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    except FileNotFoundError:
        logger.error("Gallery-DL tidak ditemukan/terinstall.")
        return 0

    if not os.path.exists(user_photo_dir): return 0
    
    current_files = set(os.listdir(user_photo_dir))
    new_files = sorted(current_files - existing_files)
    
    # Filter content
    # Photos for album
    new_photos = [f for f in new_files if f.lower().endswith(('.jpg', '.jpeg', '.png', '.webp'))]
    # Audios for reply
    new_audios = [f for f in new_files if f.lower().endswith(('.mp3', '.m4a'))]
    
    if not new_photos and not new_audios:
        logger.info(f"Tidak ada konten baru untuk @{username}")
        return 0

    logger.info(f"Ditemukan {len(new_photos)} foto dan {len(new_audios)} audio baru.")

    # Grouping by PID
    grouped_photos = {}
    grouped_audios = {} # PID -> audio_path (usually 1 per post)

    for f in new_photos:
        pid = f.split('_')[0]
        if pid not in grouped_photos: grouped_photos[pid] = []
        grouped_photos[pid].append(os.path.join(user_photo_dir, f))
        
    for f in new_audios:
        pid = f.split('_')[0]
        # Only take the first audio per PID if multiple (rare)
        if pid not in grouped_audios:
             grouped_audios[pid] = os.path.join(user_photo_dir, f)

    # Process each PID from photos
    all_pids = set(grouped_photos.keys()) | set(grouped_audios.keys())
    
    photos_sent_count = 0
    
    for pid in sorted(all_pids):
        if STOP: break
        
        paths = grouped_photos.get(pid, [])
        audio_path = grouped_audios.get(pid)
        
        # Sort photos
        paths.sort(key=lambda x: int(os.path.basename(x).split('_')[-1].split('.')[0]))
        
        # Metadata
        caption = f"📸 @{username} - {pid}" # Fallback
        json_path = ""
        
        # Try find json
        potential_files = [x for x in current_files if x.startswith(f"{pid}_") and x.endswith(".json")]
        if potential_files:
            json_path = os.path.join(user_photo_dir, potential_files[0])
            try:
                with open(json_path, 'r', encoding='utf-8') as jf:
                    meta = json.load(jf)
                    caption = format_caption(meta, username, pid)
            except: pass

        # Send Album First
        album_msg_id = -1
        
        # If no photos (audio only?), unlikely for gallery-dl tiktok, but possible
        if paths:
             # Chunking 10
             for i in range(0, len(paths), 10):
                chunk = paths[i:i+10]
                # If audio exists, we put caption ONLY on audio reply? 
                # Or caption on album AND audio?
                # User said: "audio nya ngereply foto dan captionnya buat blockquote"
                # So Album might not need full caption, or maybe just "Photos".
                # Let's put short caption on Album, Full Blockquote on Audio.
                # If NO audio, put Full Blockquote on Album.
                
                album_cap = ""
                if i == 0:
                    if audio_path:
                        album_cap = f"📸 Photos from @{username}"
                    else:
                        album_cap = caption # No audio, so caption here
                
                mid = tg_media_group(chunk, album_cap)
                if i == 0 and mid > 0:
                    album_msg_id = mid
                
                if mid > 0:
                    logger.info(f"Sent album {pid} part {i//10+1}")
                    # Cleanup photos
                    for p in chunk:
                        try: os.remove(p)
                        except: pass
                else: 
                     logger.error(f"Failed to send album {pid}")
                
                time.sleep(2)
        
        # Send Audio Reply
        if audio_path and album_msg_id > 0:
             time.sleep(1)
             if tg_send_audio_reply(audio_path, caption, album_msg_id):
                 logger.info(f"Sent audio reply for {pid}")
             else:
                 logger.error(f"Failed to send audio reply {pid}")
             
             # Cleanup audio
             try: os.remove(audio_path)
             except: pass
        elif audio_path:
             # Case: Audio exists but album failed or no photos. Send as normal audio.
             tg_send_audio_reply(audio_path, caption, None)
             try: os.remove(audio_path)
             except: pass

        # Cleanup JSON
        if json_path and os.path.exists(json_path):
            try: os.remove(json_path)
            except: pass
            
        photos_sent_count += len(paths)
        time.sleep(5)

    return photos_sent_count

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    accounts = load_accounts()
    start_time = datetime.now()
    
    tg_message(f"📸 GALLERY WORKER START\n⏰ {start_time:%H:%M:%S}\n📋 {len(accounts)} Akun")
    logger.info(f"Start Gallery Worker. {len(accounts)} accounts.")
    
    total_new = 0

    for i, acc in enumerate(accounts, 1):
        if STOP: break
        count = process_gallery(acc)
        total_new += count
        if SLEEP_SECONDS > 0: time.sleep(SLEEP_SECONDS)

    end_time = datetime.now()
    tg_message(
        f"🏁 GALLERY WORKER SELESAI\n"
        f"⏱️ Durasi: {end_time - start_time}\n"
        f"📸 Total Foto Baru: {total_new}"
    )

if __name__ == "__main__":
    main()
