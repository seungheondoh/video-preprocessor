import yt_dlp
from yt_dlp.utils import download_range_func
import os
import json
import shutil
import subprocess
from tqdm import tqdm
from multiprocessing import Pool, Value
import time
import random
import boto3

from vp.utils.fetch_data import *
from vp.configs.constants import *

s3 = boto3.client("s3")

# cookies file
cur_cookie_index = Value('i', 0)  # shared integer

def get_cookie_file_path():
    cookie_file_names = [f for f in os.listdir(COOKIES_FILE_DIR) if f.endswith('.txt')] + ['default.txt']
    cur_cookie_index.value = cur_cookie_index.value % len(cookie_file_names)  # Ensure index is within bounds
    cookie_file_name = cookie_file_names[cur_cookie_index.value]
    cookie_file_path = os.path.join(COOKIES_FILE_DIR, cookie_file_name)
    return cookie_file_path

def handle_error_message(error_message, used_cookie_fn) -> None:
    if "not a bot" in error_message or "rate-limited" in error_message:
        with cur_cookie_index.get_lock():  # Lock ensures atomic update
            if get_cookie_file_path() != used_cookie_fn: # check if is already changed
                return
            cur_cookie_index.value += 1
            print(f"🔄 쿠키 파일 변경: {get_cookie_file_path()}")

def extract_audio(mp4_path, mp3_path):
    cmd = [
        "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
        "-i", mp4_path,
        "-vn", "-acodec", "libmp3lame", "-ab", "192k",
        mp3_path
    ]
    subprocess.run(cmd, check=True)

def download_and_upload(video_info):
    video_id = video_info['video_id']
    clip_id = video_info['clip_id']

    if clip_id in failed_ids or clip_id in completed_ids:
        return False

    video_dir = os.path.join(DOWNLOAD_DIR, clip_id)

    if os.path.exists(video_dir):
        shutil.rmtree(video_dir)
    os.makedirs(video_dir, exist_ok=True)

    mp4_path_template = os.path.join(video_dir, f"{clip_id}.%(ext)s")
    mp4_path = os.path.join(video_dir, f"{clip_id}.mp4")
    mp3_path = os.path.join(video_dir, f"{clip_id}_audio.mp3")
    json_path = os.path.join(video_dir, f"{clip_id}.info.json")

    start_frame, end_frame = video_info['clip_start_end_idx']
    fps = video_info['video_fps']
    start_sec = start_frame / fps
    end_sec = end_frame / fps

    cookie_fn = get_cookie_file_path()
    try:
        ydl_opts = {
            'quiet': True,
            'no_warnings': True,
            'noplaylist': True,
            'ignoreerrors': False, # Changed to False so that the exception is raised
            'cookiefile': cookie_fn,
            'outtmpl': mp4_path_template,
            'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/mp4',
            'merge_output_format': 'mp4',
            'writeinfojson': True,
            'force_keyframes_at_cuts': True,
            'download_ranges': download_range_func(None, [(start_sec, end_sec)]),
            'postprocessors': [],
        }
        
        # ✅ 랜덤한 시간 지연 추가 (0.5초 ~ 1.5초)
        sleep_time = random.uniform(0.5, 1.5)
        print(f"[WAIT] {clip_id} 다운로드 전 대기 중... ({sleep_time:.2f}초)")
        time.sleep(sleep_time)

        print(f">>> {clip_id} 다운로드 중... ({start_sec:.2f}s ~ {end_sec:.2f}s)")
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([f"https://www.youtube.com/watch?v={video_id}"])

    except Exception as e:
        error_msg = str(e).lower()
        log_result(clip_id, FAILED_LOG, error_msg)
        handle_error_message(error_msg, cookie_fn)

        # 일반 실패 시 클린업
        if os.path.exists(video_dir):
            shutil.rmtree(video_dir)
        return False
    
    if os.path.exists(mp4_path):
        extract_audio(mp4_path, mp3_path)
        
    if not (os.path.exists(mp4_path) and os.path.exists(mp3_path) and os.path.exists(json_path)):
        log_result(clip_id, FAILED_LOG, "다운로드된 파일 없음")
        if os.path.exists(video_dir):
            shutil.rmtree(video_dir)
        return False

    # ✅ S3 업로드
    if upload_clip_folder(clip_id):
        shutil.rmtree(video_dir)
        log_result(clip_id, COMPLETED_LOG)
        print(f"업로드 성공: {clip_id}")
        return True
    else:
        print(f"❌ S3 업로드 실패: {clip_id}")
        return False


if __name__ == '__main__':
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    failed_ids = load_ids(FAILED_LOG)
    completed_ids = load_ids(COMPLETED_LOG)
    data = [item for item in data if item['clip_id'] not in failed_ids and item['clip_id'] not in completed_ids]

    print(f"🔍 처리할 clip_id 수: {len(data)}")

    with Pool(NUM_WORKERS) as pool:
        with tqdm(total=len(data), desc="다운로드 및 업로드 진행") as pbar:
            for result in pool.imap_unordered(download_and_upload, data):
                pbar.update(1)
