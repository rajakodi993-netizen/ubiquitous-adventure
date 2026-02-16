#!/usr/bin/env python3
import os
import sys
import json
import glob
import time
import zipfile
import logging
import signal
import pathlib
import argparse
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from logging.handlers import RotatingFileHandler

# === BASE DIR ===
BASE_DIR = os.environ.get('GITHUB_WORKSPACE', os.path.dirname(os.path.abspath(__file__)))

# === ENV ===
VIDEO_DIR = os.getenv('VIDEO_DIR', os.path.join(BASE_DIR, 'videos'))
ARCHIVE_DIR = os.getenv('ARCHIVE_DIR', os.path.join(BASE_DIR, 'archive'))
LOG_FILE = os.getenv('LOG_FILE', os.path.join(BASE_DIR, 'logs', 'download.log'))
NOTIF_CHANNEL_ID = os.getenv('NOTIF_CHANNEL_ID', '')
BOT_TOKEN = os.getenv('BOT_TOKEN', '')
TT_COOKIES = os.getenv('TT_COOKIES', os.path.join(BASE_DIR, 'data', 'cookies.txt'))
LOCK_FILE = os.getenv('LOCK_FILE', os.path.join(BASE_DIR, 'data', 'lock', 'tiktok_downloader.lock'))
ACCOUNTS_FILE = os.getenv('ACCOUNTS_FILE', os.path.join(BASE_DIR, 'data', 'tiktok_accounts.json'))
SLEEP_SECONDS = int(os.getenv('SLEEP_SECONDS', '5'))

# === LOGGING (rotating) ===
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logger = logging.getLogger('acpn')
logger.setLevel(logging.INFO)
fmt = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', '%Y-%m-%d %H:%M:%S')
fh = RotatingFileHandler(LOG_FILE, maxBytes=10_000_000, backupCount=5)
#fh.setFormatter(fmt) # RotatingFileHandler formatter set below handled by basicConfig in other scripts but here effectively
fh.setFormatter(fmt)

sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(fmt)
logger.handlers = [fh, sh]

# === TELEGRAM ===
import requests
tg_lock = threading.Lock()

def tg_message(text: str) -> bool:
    if not (BOT_TOKEN and NOTIF_CHANNEL_ID):
        return False
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        with tg_lock:
            # Simple retry mechanism
            for _ in range(3):
                r = requests.post(url, data={"chat_id": NOTIF_CHANNEL_ID, "text": text}, timeout=10)
                if r.status_code == 429:
                    time.sleep(int(r.headers.get("Retry-After", 5)))
                    continue
                r.raise_for_status()
                return True
    except Exception as e:
        logger.error(f"Gagal kirim pesan TG: {e}")
        return False

def tg_document(file_path: str, caption: str = "") -> bool:
    if not (BOT_TOKEN and NOTIF_CHANNEL_ID):
        return False
    if not os.path.exists(file_path):
        return False
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument"
    try:
        with tg_lock:
            with open(file_path, 'rb') as f:
                r = requests.post(url, data={"chat_id": NOTIF_CHANNEL_ID, "caption": caption}, files={"document": f}, timeout=60)
            r.raise_for_status()
            return True
    except Exception as e:
        logger.error(f"Gagal kirim dokumen TG: {e}")
        return False

# === LOCK ===
def _acquire_lock():
    if os.path.exists(LOCK_FILE):
        try:
            with open(LOCK_FILE, 'r') as f:
                pid = f.read().strip()
            if os.path.exists(f"/proc/{pid}"):
                logger.warning(f"Proses lain sedang berjalan (PID {pid}). Keluar.")
                return False
        except:
            pass
    try:
        os.makedirs(os.path.dirname(LOCK_FILE), exist_ok=True)
        with open(LOCK_FILE, 'w') as f:
            f.write(str(os.getpid()))
        return True
    except Exception as e:
        logger.error(f"Gagal membuat lock file: {e}")
        return False

def _release_lock():
    try:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
            logger.info("Lock dihapus.")
    except Exception as e:
        logger.error(f"Gagal menghapus lock: {e}")

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
            clean_list = []
            for item in data:
                if isinstance(item, str):
                    clean_list.append(item)
                elif isinstance(item, dict) and 'url' in item:
                    clean_list.append(item['url'])
            return clean_list
    except Exception as e:
        logger.error(f"Gagal memuat akun: {e}")
    return []

def process_account(url: str, limit: int, upload_exec_path: str) -> int:
    if STOP or not url or 'tiktok.com' not in url:
        return 0
    
    try:
        username = url.split('@')[1].split('/')[0]
    except:
        username = "unknown"

    logger.info(f"Memproses: @{username}")
    user_archive = os.path.join(ARCHIVE_DIR, f"{username}_ACPN.txt")
    
    cmd = [
        "yt-dlp",
        url,
        "--cookies", TT_COOKIES,
        "--download-archive", user_archive,
        "--output", f"{VIDEO_DIR}/%(uploader)s_%(id)s.%(ext)s",
        "--write-info-json", 
        "--no-part",
        "--no-warnings",
        "--ignore-errors",
        "--restrict-filenames",
        "--exec", f"{sys.executable} {upload_exec_path} {{}}",
    ]

    # Limit per account check
    if limit > 0:
        cmd.extend(["--playlist-end", str(limit)])

    # Count before
    before_count = 0
    if os.path.exists(user_archive):
        with open(user_archive, 'r') as f:
            before_count = sum(1 for line in f)

    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

    # Count after
    after_count = 0
    if os.path.exists(user_archive):
        with open(user_archive, 'r') as f:
            after_count = sum(1 for line in f)
    
    new_videos = max(0, after_count - before_count)
    if new_videos > 0:
        tg_message(f"✅ @{username}: +{new_videos} video")
        logger.info(f"@{username}: +{new_videos} video")
    
    return new_videos

def run_cycle(limit=10):
    global STOP
    if not _acquire_lock():
        return

    try:
        accounts = load_accounts()
        total = len(accounts)
        if total == 0:
            logger.warning("Tidak ada akun.")
            return

        os.makedirs(VIDEO_DIR, exist_ok=True)
        os.makedirs(ARCHIVE_DIR, exist_ok=True)
        upload_exec_path = os.path.join(BASE_DIR, 'data', 'upload_exec.py')

        start = datetime.now()
        tg_message(f"🚀 SIKLUS DIMULAI (Parallel)\n⏰ {start:%H:%M:%S}\n📋 {total} Akun\n⚡ Limit: {limit}")

        total_new = 0
        
        # Parallel Execution
        # 5 workers is safe for Github Actions (2 core)
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(process_account, url, limit, upload_exec_path): url for url in accounts}
            
            for i, future in enumerate(as_completed(futures), 1):
                if STOP: 
                    executor.shutdown(wait=False, cancel_futures=True)
                    break
                try:
                    new_count = future.result()
                    total_new += new_count
                except Exception as e:
                    logger.error(f"Error thread: {e}")
                
                # Progress log every 10 accounts
                if i % 10 == 0:
                    logger.info(f"Progress: {i}/{total} akun selesai..")

        # kirim log
        try:
            tg_document(LOG_FILE, "📝 Log Siklus")
        except: pass

        # zip arsip
        zip_path = os.path.join(BASE_DIR, 'arsip_ACPN.zip')
        with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as z:
            for p in glob.glob(os.path.join(ARCHIVE_DIR, '*_ACPN.txt')):
                z.write(p, os.path.basename(p))
        tg_document(zip_path)

        end = datetime.now()
        tg_message(
            f"🏁 SIKLUS SELESAI\n"
            f"⏱️ Durasi: {end - start}\n"
            f"📦 Total baru: {total_new}"
        )

    except Exception as e:
        logger.exception("Kesalahan fatal:")
        tg_message(f"❌ ERROR: {e}")
    finally:
        _release_lock()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=10)
    parser.add_argument('--loop', action='store_true')
    args = parser.parse_args()

    if args.loop:
        while not STOP:
            run_cycle(limit=args.limit)
            if STOP: break
            time.sleep(SLEEP_SECONDS)
    else:
        run_cycle(limit=args.limit)

if __name__ == "__main__":
    main()