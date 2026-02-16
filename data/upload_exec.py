#!/usr/bin/env python3
import os, sys, json, requests, html, tempfile, subprocess, shutil, time, mimetypes
from datetime import datetime, timedelta
from dotenv import load_dotenv

# === Load .env ===
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_BASE_DIR = os.environ.get('GITHUB_WORKSPACE', os.path.dirname(_SCRIPT_DIR))
load_dotenv(os.path.join(_BASE_DIR, '.env'))
BOT_TOKEN = os.getenv("BOT_TOKEN")
UPLOAD_CHANNEL_ID = os.getenv("UPLOAD_CHANNEL_ID")

# -------------------- Fungsi baru untuk menangani request dengan retry otomatis --------------------
def send_telegram_request_with_retry(url, data=None, files=None, timeout=(30, 300), max_retries=5):
    """
    Mengirim request ke API Telegram dengan mekanisme retry otomatis untuk error 429 (Too Many Requests).
    """
    retries = 0
    while retries < max_retries:
        try:
            r = requests.post(url, data=data, files=files, timeout=timeout)
            r.raise_for_status()  # Ini akan raise HTTPError untuk status 4xx/5xx
            return r.json()
        except requests.exceptions.HTTPError as e:
            # Hanya tangani error 429
            if e.response.status_code == 429:
                try:
                    error_data = e.response.json()
                    retry_after = error_data.get("parameters", {}).get("retry_after")
                    if retry_after and isinstance(retry_after, int):
                        wait_time = retry_after + 1 # Tambah 1 detik buffer
                        print(f"⚠️ Telegram rate limit (429). Menunggu {wait_time} detik sesuai instruksi...")
                        time.sleep(wait_time)
                        retries += 1
                        continue # Coba lagi
                    else:
                        print("⚠️ Error 429 diterima tapi tanpa parameter retry_after yang valid. Menunggu 10 detik.")
                        time.sleep(10)
                        retries += 1
                        continue
                except (json.JSONDecodeError, AttributeError):
                    print("⚠️ Gagal parse error 429. Menunggu 10 detik.")
                    time.sleep(10)
                    retries += 1
                    continue
            else:
                # Untuk error HTTP lain, langsung GAGAL
                print(f"Telegram error {e.response.status_code}: {e.response.text}")
                raise e
        except requests.exceptions.RequestException as e:
            print(f"❌ Gagal koneksi: {e}")
            raise e # Gagal karena masalah koneksi
    
    raise Exception(f"Gagal mengirim request setelah {max_retries} kali percobaan.")


# -------------------- Helpers dasar (TIDAK ADA PERUBAHAN) --------------------
def find_info_json(video_path: str) -> str:
    base, _ = os.path.splitext(video_path)
    cand = base + ".info.json"
    if os.path.exists(cand):
        return cand
    folder = os.path.dirname(video_path) or "."
    name = os.path.basename(base)
    for fn in os.listdir(folder):
        if fn.startswith(name) and fn.endswith(".info.json"):
            return os.path.join(folder, fn)
    return ""

def load_meta(info_path: str) -> dict:
    try:
        with open(info_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError, ValueError):
        return {}

def fmt_date(s: str) -> str:
    try:
        return datetime.strptime(s, "%Y%m%d").strftime("%d-%m-%Y")
    except (ValueError, TypeError):
        return s or "-"

def fmt_duration(d):
    if isinstance(d, (int, float)) and d is not None:
        return str(timedelta(seconds=int(d)))
    if isinstance(d, str) and d.strip():
        return d
    return "-"

def caption_from_meta(meta: dict, video_path: str) -> str:
    uploader = meta.get("uploader") or meta.get("channel") or "-"
    title = meta.get("title") or os.path.basename(video_path)
    url = meta.get("webpage_url") or "-"
    upload_date = fmt_date(meta.get("upload_date", ""))
    duration = fmt_duration(meta.get("duration_string") or meta.get("duration"))
    views = meta.get("view_count", "-")
    likes = meta.get("like_count", "-")
    comments = meta.get("comment_count", "-")

    esc = lambda s: html.escape(str(s), quote=True)
    uploader_e = esc(uploader); title_e = esc(title); url_e = esc(url)
    upload_date_e = esc(upload_date); duration_e = esc(duration)
    views_e = esc(views); likes_e = esc(likes); comments_e = esc(comments)

    return (
        f"<blockquote><b>{title_e}</b></blockquote>\n"
        f"<blockquote>👤 #{uploader_e}</blockquote>\n"
        f"<blockquote>📅 {upload_date_e} | ⏱️ {duration_e}</blockquote>\n"
        f"<blockquote>👀 {views_e} | ❤️ {likes_e} | 💬 {comments_e}</blockquote>\n"
        f"<blockquote>🔗 <a href=\"{url_e}\">Watch</a></blockquote>"
    )

# -------------------- Thumbnail utils (TIDAK ADA PERUBAHAN) --------------------
def _ffprobe_duration(path: str) -> float | None:
    try:
        out = subprocess.check_output([
            "ffprobe", "-v", "error", "-select_streams", "v:0",
            "-show_entries", "format=duration", "-of", "default=nw=1:nk=1", path
        ], stderr=subprocess.STDOUT, text=True).strip()
        return float(out)
    except Exception:
        return None

def _ffmpeg_frame_thumbnail(video_path: str, when_sec: float, out_path: str) -> tuple[bool, str]:
    try:
        start = time.time()
        subprocess.check_call([
            "ffmpeg", "-y",
            "-ss", f"{when_sec:.2f}",
            "-i", video_path,
            "-frames:v", "1",
            "-vf", "scale='min(320,iw)':'-2':flags=lanczos",
            "-q:v", "3",
            out_path
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if os.path.getsize(out_path) > 200_000:
            subprocess.check_call([
                "ffmpeg", "-y", "-i", out_path, "-q:v", "6", out_path
            ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        ok = os.path.exists(out_path) and os.path.getsize(out_path) <= 200_000
        dur = f"{(time.time()-start):.2f}s"
        if ok:
            return True, f"sukses (size={os.path.getsize(out_path)}B, waktu={dur})"
        return False, f"gagal (size={os.path.getsize(out_path)}B > 200KB, waktu={dur})"
    except subprocess.CalledProcessError:
        return False, "ffmpeg error"
    except Exception as e:
        return False, f"exception: {e}"

def _download_and_shrink(url: str, out_path: str) -> tuple[bool, str]:
    try:
        r = requests.get(url, timeout=30)
        if r.status_code >= 400:
            return False, f"HTTP {r.status_code}"
        with open(out_path, "wb") as f:
            f.write(r.content)
        tmp2 = out_path + ".tmp.jpg"
        subprocess.check_call([
            "ffmpeg", "-y", "-i", out_path,
            "-vf", "scale='min(320,iw)':'-2':flags=lanczos",
            "-q:v", "4",
            tmp2
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        shutil.move(tmp2, out_path)
        if os.path.getsize(out_path) <= 200_000:
            return True, f"sukses (size={os.path.getsize(out_path)}B)"
        return False, f"kebesaran (size={os.path.getsize(out_path)}B > 200KB)"
    except subprocess.CalledProcessError:
        return False, "ffmpeg shrink error"
    except Exception as e:
        return False, f"exception: {e}"

def build_cover_and_thumb(video_path: str, meta: dict) -> tuple[str | None, str | None, str]:
    tmpdir = tempfile.gettempdir()
    cover_path = os.path.join(tmpdir, f"cover_{os.getpid()}_{os.path.basename(video_path)}.jpg")
    thumb_path = os.path.join(tmpdir, f"thumb_{os.getpid()}_{os.path.basename(video_path)}.jpg")
    logs = []
    thumbs = {t.get("id"): t.get("url") for t in (meta.get("thumbnails") or []) if t.get("url")}
    for key in ["dynamicCover", "cover", "originCover"]:
        if key in thumbs:
            logs.append(f"➡️  Coba metadata: {key}")
            ok, reason = _download_and_shrink(thumbs[key], thumb_path)
            logs.append(f"    └─ {reason}")
            if ok:
                shutil.copy2(thumb_path, cover_path)
                logs.append(f"✅ Pakai {key}: thumb={os.path.getsize(thumb_path)}B, cover={os.path.getsize(cover_path)}B")
                return cover_path, thumb_path, "\n".join(logs)
    dur = meta.get("duration")
    if not isinstance(dur, (int, float)):
        dur = _ffprobe_duration(video_path) or 8.0
    when_sec = max(1.0, float(dur) / 2.0)
    logs.append(f"➡️  Fallback: frame tengah @ {when_sec:.2f}s")
    ok, reason = _ffmpeg_frame_thumbnail(video_path, when_sec, thumb_path)
    logs.append(f"    └─ {reason}")
    if ok:
        shutil.copy2(thumb_path, cover_path)
        logs.append(f"✅ Pakai frame tengah: thumb={os.path.getsize(thumb_path)}B, cover={os.path.getsize(cover_path)}B")
        return cover_path, thumb_path, "\n".join(logs)
    logs.append("❌ Semua opsi gagal. Telegram akan pakai default (tanpa cover).")
    return None, None, "\n".join(logs)

# -------------------- Validasi file --------------------
def _is_valid_file(path: str, label: str = "file") -> bool:
    """Cek file ada dan tidak kosong."""
    if not path or not os.path.exists(path):
        return False
    size = os.path.getsize(path)
    if size == 0:
        print(f"⚠️ {label} kosong (0 byte): {path}")
        return False
    return True

# -------------------- Fallback: sendVideo tanpa cover --------------------
def send_video_fallback(video_path: str, caption_html: str = "") -> int:
    """Kirim video saja tanpa cover/thumbnail sebagai fallback."""
    api = f"https://api.telegram.org/bot{BOT_TOKEN}/sendVideo"
    print(f"🔄 Fallback: kirim video saja tanpa cover...")
    with open(video_path, "rb") as vf:
        data = {
            "chat_id": UPLOAD_CHANNEL_ID,
            "supports_streaming": "true",
        }
        if caption_html:
            data["caption"] = caption_html
            data["parse_mode"] = "HTML"
        resp = send_telegram_request_with_retry(
            url=api, data=data, files={"video": vf}, timeout=(30, 600)
        )
    msg_id = resp.get("result", {}).get("message_id", -1)
    if msg_id > 0:
        print(f"✅ Fallback berhasil! message_id={msg_id}")
    return msg_id

# -------------------- Fungsi pengiriman utama --------------------
def send_media_group_return_first_id(video_path: str, cover_path: str | None, thumb_path: str | None, caption_html: str = "") -> int:
    if not (BOT_TOKEN and UPLOAD_CHANNEL_ID):
        print("BOT_TOKEN/UPLOAD_CHANNEL_ID kosong")
        sys.exit(3)

    # Validasi video wajib non-empty
    if not _is_valid_file(video_path, "Video"):
        print(f"❌ Video tidak valid atau kosong, skip upload.")
        return -1

    # Validasi cover & thumbnail (buang jika kosong)
    if thumb_path and not _is_valid_file(thumb_path, "Thumbnail"):
        thumb_path = None
    if cover_path and not _is_valid_file(cover_path, "Cover"):
        cover_path = None

    api = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMediaGroup"
    media_items = [{"type": "video", "media": "attach://media1", "supports_streaming": True}]
    if caption_html:
        media_items[0]["caption"] = caption_html
        media_items[0]["parse_mode"] = "HTML"
    if thumb_path:
        media_items[0]["thumbnail"] = "attach://thumb1"
    include_photo = bool(cover_path)
    if include_photo:
        media_items.append({"type": "photo", "media": "attach://media2"})

    try:
        with open(video_path, "rb") as video_file:
            files = {"media1": video_file}
            thumb_file = open(thumb_path, "rb") if thumb_path else None
            cover_file = open(cover_path, "rb") if include_photo else None

            try:
                if thumb_file:
                    files["thumb1"] = thumb_file
                if cover_file:
                    mime = mimetypes.guess_type(cover_path)[0] or "image/jpeg"
                    files["media2"] = (os.path.basename(cover_path), cover_file, mime)

                resp = send_telegram_request_with_retry(
                    url=api,
                    data={"chat_id": UPLOAD_CHANNEL_ID, "media": json.dumps(media_items, ensure_ascii=False)},
                    files=files
                )

                results = resp.get("result") or []
                first_id = results[0]["message_id"] if results else -1
                print(f"✅ Uploaded (album): {os.path.basename(video_path)} + {'cover' if include_photo else 'no-cover'} | first_message_id={first_id}")
                return int(first_id) if first_id != -1 else -1
            finally:
                if thumb_file:
                    thumb_file.close()
                if cover_file:
                    cover_file.close()

    except Exception as e:
        print(f"⚠️ sendMediaGroup gagal: {e}")
        # Fallback ke sendVideo tanpa cover
        try:
            return send_video_fallback(video_path, caption_html)
        except Exception as e2:
            print(f"❌ Fallback sendVideo juga gagal: {e2}")
            return -1

def send_caption_reply(caption_html: str, reply_to_message_id: int):
    if not (BOT_TOKEN and UPLOAD_CHANNEL_ID):
        print("BOT_TOKEN/UPLOAD_CHANNEL_ID kosong")
        sys.exit(3)
    if not (reply_to_message_id and reply_to_message_id > 0):
        print("⚠️ reply_to_message_id tidak valid; lewati kirim caption reply.")
        return
    api = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {
        "chat_id": UPLOAD_CHANNEL_ID,
        "text": caption_html,
        "parse_mode": "HTML",
        "reply_to_message_id": reply_to_message_id,
        "allow_sending_without_reply": True,
        "disable_web_page_preview": True
    }
    # Panggil fungsi request dengan retry
    send_telegram_request_with_retry(url=api, data=data, timeout=(30, 120))
    print(f"🧾 Caption dikirim sebagai reply ke message_id={reply_to_message_id}")

# -------------------- Main (HAPUS time.sleep dari finally) --------------------
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: upload_exec.py <filepath>")
        sys.exit(1)

    file_path = sys.argv[1].strip()
    if (file_path.startswith("'") and file_path.endswith("'")) or (file_path.startswith('"') and file_path.endswith('"')):
        file_path = file_path[1:-1]

    if not os.path.exists(file_path):
        print(f"File tidak ditemukan: {file_path}")
        sys.exit(1)
    if not file_path.lower().endswith(".mp4"):
        print(f"⏭️  Skip non-mp4: {file_path}")
        sys.exit(0)
    if not (BOT_TOKEN and UPLOAD_CHANNEL_ID):
        print("BOT_TOKEN/UPLOAD_CHANNEL_ID kosong")
        sys.exit(3)

    info_path = find_info_json(file_path)
    meta = load_meta(info_path) if info_path else {}
    caption = caption_from_meta(meta, file_path)

    cover_path = None
    thumb_path = None
    upload_ok = False
    try:
        cover_path, thumb_path, thumb_info = build_cover_and_thumb(file_path, meta)
        print(thumb_info)

        first_msg_id = send_media_group_return_first_id(file_path, cover_path, thumb_path, caption_html=caption)
        if first_msg_id and first_msg_id > 0:
            upload_ok = True
            # send_caption_reply(caption, first_msg_id) # DISABLED: Caption now merged
        else:
            print("⚠️ Upload gagal atau tidak dapat message_id.")

    except Exception as e:
        print(f"❌ Gagal upload: {e}")
    finally:
        # Hapus file sementara (cover & thumb)
        for p in (cover_path, thumb_path):
            if p and os.path.exists(p):
                try: os.remove(p)
                except Exception: pass

    # Hapus file HANYA jika upload sukses
    if upload_ok:
        if info_path and os.path.exists(info_path):
            try:
                os.remove(info_path)
                print(f"🧹 Hapus metadata: {info_path}")
            except Exception as e:
                print(f"⚠️ Gagal hapus {info_path}: {e}")
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                print(f"🧹 Hapus video: {file_path}")
            except Exception as e:
                print(f"⚠️ Gagal hapus video: {e}")
    else:
        print(f"📌 Video TIDAK dihapus karena upload gagal: {file_path}")
        print(f"   Bisa di-retry nanti dengan upload_fallback.py")
        sys.exit(2)

