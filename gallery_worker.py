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
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE)
    ]
)
logger = logging.getLogger('gallery_worker')

# === TELEGRAM ===
tg_lock = threading.Lock() # Lock for text messages to avoid interleaving

def tg_message(text: str) -> bool:
    channel = NOTIF_CHANNEL_ID
    if not (BOT_TOKEN and channel):
        return False
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        with tg_lock:
            # Simple retry if 429
            r = requests.post(url, data={"chat_id": channel, "text": text}, timeout=10)
            if r.status_code == 429:
                time.sleep(int(r.headers.get("Retry-After", 5)))
                # Call directly (recursive) inside lock is fine
                # But better to just loop? requests.post is blocking.
                # Recursive is easier for simplistic retry.
                # But wait, python default recursion limit.
                # Let's trust one retry loop.
                r = requests.post(url, data={"chat_id": channel, "text": text}, timeout=10)
            
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
        
        # Caption on first item
        if i == 0 and caption:
             media_item["caption"] = caption
             media_item["parse_mode"] = "HTML"
        media.append(media_item)

    if not media: return -1

    try:
        # Upload is NOT locked, so we can upload in parallel
        # But requests session is independent.
        r = requests.post(url, data={"chat_id": channel, "media": json.dumps(media)}, files=files, timeout=(60, 300))
        if r.status_code == 429:
            retry = int(r.headers.get("Retry-After", "10"))
            logger.warning(f"Rate limited. Menunggu {retry}s...")
            time.sleep(retry)
            return tg_media_group(media_files, caption) # Retry recursive
        r.raise_for_status()
        
        # Get message_id
        res = r.json()
        if res.get("ok"):
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
            }
            if reply_to_id and reply_to_id > 0:
                data["reply_to_message_id"] = reply_to_id
                
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
    
    esc = lambda s: html.escape(str(s), quote=True)
    
    return (
        f"<blockquote><b>{esc(desc[:800])}</b></blockquote>\n"
        f"<blockquote>👤 @{esc(username)}</blockquote>\n"
        f"<blockquote>📅 {esc(date)}</blockquote>\n"
        f"<blockquote>🔗 <a href='{url}'>Link Posts</a></blockquote>"
    )

def process_gallery(url: str, limit: int = 0) -> int:
    if STOP or not url: return 0
    
    # Thread-local logger context? Using thread name in log format.
    
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

    # Run Gallery-DL
    # Note: gallery-dl is mostly single threaded, so parallel processes are fine
    cmd = [
        sys.executable, "-m", "gallery_dl", url,
        "--cookies", TT_COOKIES,
        "--download-archive", photo_archive,
        "--filter", "extension in ('jpg', 'jpeg', 'png', 'webp', 'mp3', 'm4a')",
        "--no-skip",
        "--write-metadata",
        "-D", user_photo_dir,
        "-o", "filename={id}_{num}.{extension}",
    ]
    
    try:
        # Capture stderr to avoid thread mixing clutter? Or allow it?
        # subprocess.PIPE is safer.
        subprocess.run(cmd, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    except FileNotFoundError:
        logger.error("Gallery-DL tidak ditemukan/terinstall.")
        return 0

    if not os.path.exists(user_photo_dir): return 0
    
    current_files = set(os.listdir(user_photo_dir))
    new_files = sorted(current_files - existing_files)
    
    # Filter
    new_photos = [f for f in new_files if f.lower().endswith(('.jpg', '.jpeg', '.png', '.webp'))]
    new_audios = [f for f in new_files if f.lower().endswith(('.mp3', '.m4a'))]
    
    if not new_photos and not new_audios:
        # logger.info(f"Tidak ada konten baru untuk @{username}") 
        # Reduce log noise in parallel
        return 0

    logger.info(f"@{username}: +{len(new_photos)} foto, +{len(new_audios)} audio.")

    # Grouping
    grouped_photos = {}
    grouped_audios = {} 

    for f in new_photos:
        pid = f.split('_')[0]
        if pid not in grouped_photos: grouped_photos[pid] = []
        grouped_photos[pid].append(os.path.join(user_photo_dir, f))
        
    for f in new_audios:
        pid = f.split('_')[0]
        if pid not in grouped_audios:
             grouped_audios[pid] = os.path.join(user_photo_dir, f)

    all_pids = set(grouped_photos.keys()) | set(grouped_audios.keys())
    
    photos_sent_count = 0
    
    # Sending logic
    for pid in sorted(all_pids):
        if STOP: break
        
        paths = grouped_photos.get(pid, [])
        audio_path = grouped_audios.get(pid)
        
        paths.sort(key=lambda x: int(os.path.basename(x).split('_')[-1].split('.')[0]))
        
        caption = f"📸 @{username} - {pid}"
        json_path = ""
        
        potential_files = [x for x in current_files if x.startswith(f"{pid}_") and x.endswith(".json")]
        if potential_files:
            json_path = os.path.join(user_photo_dir, potential_files[0])
            try:
                with open(json_path, 'r', encoding='utf-8') as jf:
                    meta = json.load(jf)
                    caption = format_caption(meta, username, pid)
            except: pass

        album_msg_id = -1
        
        # Send Photo Album
        if paths:
             for i in range(0, len(paths), 10):
                chunk = paths[i:i+10]
                album_cap = ""
                if i == 0:
                    if audio_path:
                        album_cap = f"📸 Photos from @{username}"
                    else:
                        album_cap = caption 
                
                mid = tg_media_group(chunk, album_cap)
                if i == 0 and mid > 0:
                    album_msg_id = mid
                
                if mid > 0:
                    # Cleanup photos
                    for p in chunk:
                        try: os.remove(p)
                        except: pass
                else: 
                     logger.error(f"Failed to send album {pid} (@{username})")
                
                time.sleep(2) # Small delay inside thread
        
        # Send Audio Reply
        if audio_path and album_msg_id > 0:
             time.sleep(1)
             if tg_send_audio_reply(audio_path, caption, album_msg_id):
                 pass # Success logging if needed
             else:
                 logger.error(f"Failed to send audio reply {pid}")
             try: os.remove(audio_path)
             except: pass
        elif audio_path:
             tg_send_audio_reply(audio_path, caption, None)
             try: os.remove(audio_path)
             except: pass

        if json_path and os.path.exists(json_path):
            try: os.remove(json_path)
            except: pass
            
        photos_sent_count += len(paths)
        time.sleep(5) # Delay per post

    return photos_sent_count

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    accounts = load_accounts()
    start_time = datetime.now()
    total_acc = len(accounts)
    
    tg_message(f"📸 GALLERY WORKER START (Parallel)\n⏰ {start_time:%H:%M:%S}\n📋 {total_acc} Akun")
    logger.info(f"Start Gallery Worker. {total_acc} accounts. Parallel=5")
    
    total_new = 0

    # Parallel Execution (5 workers)
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(process_gallery, url, args.limit): url for url in accounts}
        
        for i, future in enumerate(as_completed(futures), 1):
            if STOP:
                executor.shutdown(wait=False, cancel_futures=True)
                break
            try:
                count = future.result()
                total_new += count
            except Exception as e:
                logger.error(f"Error thread: {e}")
            
            if i % 10 == 0:
                logger.info(f"Progress: {i}/{total_acc} akun selesai.")

    end_time = datetime.now()
    tg_message(
        f"🏁 GALLERY WORKER SELESAI\n"
        f"⏱️ Durasi: {end_time - start_time}\n"
        f"📸 Total Foto Baru: {total_new}"
    )

if __name__ == "__main__":
    main()
