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
import shutil
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from logging.handlers import RotatingFileHandler
from huggingface_hub import HfApi

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
fh.setFormatter(fmt)
sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(fmt)
logger.handlers = [fh, sh]

# === TELEGRAM ===
import requests
tg_lock = threading.Lock() # Lock untuk mencegah bentrok kirim pesan paralel

def tg_message(text: str) -> bool:
    if not (BOT_TOKEN and NOTIF_CHANNEL_ID):
        return False
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        with tg_lock:
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

# === FUNGSI PROSES PARALEL ===
def process_account(url: str, limit: int, upload_exec_path: str) -> int:
    if STOP or not url or 'tiktok.com' not in url:
        return 0
    
    try:
        username = url.split('@')[1].split('/')[0]
    except:
        username = "unknown"

    logger.info(f"🔍 Mengecek: @{username} ...")
    user_archive = os.path.join(ARCHIVE_DIR, f"{username}_ACPN.txt")
    
    cmd = [
        "yt-dlp",
        url,
        "--impersonate", "chrome",
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

    before_count = 0
    if os.path.exists(user_archive):
        with open(user_archive, 'r') as f:
            before_count = sum(1 for line in f)

    # Jalankan yt-dlp
    subprocess.run(cmd, capture_output=True, text=True)

    after_count = 0
    if os.path.exists(user_archive):
        with open(user_archive, 'r') as f:
            after_count = sum(1 for line in f)
    
    new_videos = max(0, after_count - before_count)
    if new_videos > 0:
        tg_message(f"✅ @{username}: +{new_videos} video")
        logger.info(f"✅ @{username}: Berhasil mendapat {new_videos} video baru.")
        
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
        tg_message(f"🚀 SIKLUS DIMULAI (Parallel)\n⏰ {start:%Y-%m-%d %H:%M:%S}\n📋 {total} Akun\n⚡ Limit: {limit} (per akun)")

        total_new = 0
        
        # Eksekusi Paralel (max_workers=3 cocok untuk Github Actions)
        with ThreadPoolExecutor(max_workers=3) as executor:
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
                
                # Progress log setiap kelipatan 10
                if i % 10 == 0 or i == total:
                    logger.info(f"Progress: {i}/{total} akun selesai diperiksa.")

# -------------------- HUGGING FACE BATCH UPLOAD (LARGE FOLDER) --------------------
        hf_token = os.getenv("HF_TOKEN")
        mp4_files = glob.glob(os.path.join(VIDEO_DIR, '*.mp4'))
        
        if mp4_files:
            if hf_token:
                staging_dir = os.path.join(BASE_DIR, 'hf_staging')
                try:
                    logger.info(f"☁️ Memulai upload_large_folder {len(mp4_files)} video ke Hugging Face...")
                    waktu_wib = datetime.now(timezone.utc) + timedelta(hours=7)
                    date_folder = waktu_wib.strftime("%Y/%m/%d")
                    
                    # 1. Buat path folder utama berdasarkan tanggal
                    target_dir = os.path.join(staging_dir, date_folder)
                    
                    # 2. Tentukan path untuk sub-folder Video dan Metadata
                    video_dir = os.path.join(target_dir, 'Video')
                    metadata_dir = os.path.join(target_dir, 'Metadata')
                    
                    # 3. Buat foldernya (exist_ok=True agar tidak error jika folder sudah ada)
                    os.makedirs(video_dir, exist_ok=True)
                    os.makedirs(metadata_dir, exist_ok=True)
                    
                    # 4. Pindahkan file sesuai ekstensinya
                    for f in glob.glob(os.path.join(VIDEO_DIR, '*')):
                        if f.endswith('.mp4'):
                            shutil.move(f, os.path.join(video_dir, os.path.basename(f)))
                        elif f.endswith('.info.json'):
                            shutil.move(f, os.path.join(metadata_dir, os.path.basename(f)))
                    
                    api = HfApi(token=hf_token)
                    api.upload_large_folder(
                        folder_path=staging_dir,
                        repo_id="tafofyfe/ACPN",
                        repo_type="dataset"
                    )
                    
                    logger.info("✅ Batch upload_large_folder ke Hugging Face berhasil!")
                    tg_message(f"☁️ HF Large Folder Upload: {len(mp4_files)} video sukses ke {date_folder}/")
                    
                except Exception as e:
                    logger.error(f"❌ Gagal upload_large_folder HF: {e}")
                    tg_message(f"❌ HF Upload Error: {e}")
                finally:
                    if os.path.exists(staging_dir):
                        shutil.rmtree(staging_dir, ignore_errors=True)
            else:
                logger.warning("⚠️ HF_TOKEN tidak ada. Skip upload HF.")

            # CLEANUP FOLDER VIDEO
            for f in glob.glob(os.path.join(VIDEO_DIR, '*')):
                try: os.remove(f)
                except: pass
            logger.info("🧹 Folder VIDEO_DIR dibersihkan.")

        # zip arsip
        zip_path = os.path.join(BASE_DIR, 'arsip_ACPN.zip')
        with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as z:
            for p in glob.glob(os.path.join(ARCHIVE_DIR, '*_ACPN.txt')):
                z.write(p, os.path.basename(p))
        tg_document(zip_path, "🗂️ Backup Arsip Siklus")

        end = datetime.now()
        tg_message(
            f"🏁 SIKLUS SELESAI\n"
            f"⏱️ Durasi: {end - start}\n"
            f"📦 Total baru: {total_new}\n"
            f"⏰ {end:%Y-%m-%d %H:%M:%S}"
        )

    except Exception as e:
        logger.exception("Kesalahan fatal:")
        tg_message(f"❌ ERROR FATAL: {e}")
    finally:
        _release_lock()

def main():
    parser = argparse.ArgumentParser(description="ACPN - TikTok Downloader")
    parser.add_argument('--limit', type=int, default=10,
                        help='Maksimum jumlah video per akun (default: 10, 0=tanpa batas)')
    parser.add_argument('--loop', action='store_true',
                        help='Mode loop (untuk VPS). Tanpa flag ini = single-run (untuk GitHub Actions)')
    args = parser.parse_args()

    if args.loop:
        while not STOP:
            run_cycle(limit=args.limit)
            if STOP:
                break
            logger.info(f"Tidur {SLEEP_SECONDS} detik sebelum siklus berikutnya...")
            time.sleep(SLEEP_SECONDS)
    else:
        run_cycle(limit=args.limit)

if __name__ == "__main__":
    main()
