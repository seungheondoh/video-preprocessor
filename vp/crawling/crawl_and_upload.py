import yt_dlp
from yt_dlp.utils import download_range_func
import os
import json
import shutil
import subprocess
from tqdm import tqdm
from multiprocessing import Pool
import time
import random
import boto3

# 기본 설정
FAILED_LOG = "failed_ids_clip.txt"
UPLOAD_FAILED_LOG = "upload_failed_ids.txt"
COMPLETED_LOG = "complete_clip_ids.txt"
DOWNLOAD_DIR = "/mnt/hdd8tb/downloads_clip"
JSON_PATH = "MMTrail2M_uncrawled_part1.json"

S3_BUCKET = "maclab-youtube-crawl"
S3_PREFIX = "chopin16"
NUM_WORKERS = 16

s3 = boto3.client("s3")

def load_failed_ids():
    if os.path.exists(FAILED_LOG):
        with open(FAILED_LOG, "r", encoding="utf-8") as f:
            return set(line.strip() for line in f)
    return set()

def load_completed_ids():
    if os.path.exists(COMPLETED_LOG):
        with open(COMPLETED_LOG, "r", encoding="utf-8") as f:
            return set(line.strip() for line in f)
    return set()

def log_failed(clip_id, error_msg=""):
    with open(FAILED_LOG, "a", encoding="utf-8") as f:
        f.write(f"{clip_id}\n")
    print(f"[ERROR] {clip_id} 실패 기록됨. 사유: {error_msg}")

def log_completed(clip_id):
    with open(COMPLETED_LOG, "a", encoding="utf-8") as f:
        f.write(f"{clip_id}\n")

def log_upload_failed(clip_id):
    with open(UPLOAD_FAILED_LOG, "a", encoding="utf-8") as f:
        f.write(f"{clip_id}\n")

def extract_audio(mp4_path, mp3_path):
    cmd = [
        "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
        "-i", mp4_path,
        "-vn", "-acodec", "libmp3lame", "-ab", "192k",
        mp3_path
    ]
    subprocess.run(cmd, check=True)

def s3_complete_clip_exists(clip_id):
    """
    S3에 clip_id 폴더가 존재하고, mp4, mp3, json 파일이 모두 있을 경우 True
    그렇지 않으면 False (즉, 덮어쓰기 대상)
    """
    prefix = f"{S3_PREFIX}/{clip_id}/"
    required_exts = {".mp4", ".mp3", ".json"}

    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix)

    existing_exts = set()
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            _, filename = key.rsplit("/", 1)
            _, ext = os.path.splitext(filename)
            existing_exts.add(ext.lower())

    return required_exts.issubset(existing_exts)

def upload_to_s3(local_path, s3_key):
    try:
        s3.upload_file(local_path, S3_BUCKET, s3_key)
        return True
    except Exception as e:
        print(f"❌ S3 업로드 실패: {s3_key}, 사유: {e}")
        return False

def upload_clip_folder(clip_id):
    local_dir = os.path.join(DOWNLOAD_DIR, clip_id)
    if not os.path.exists(local_dir):
        return False

    # ✅ S3에 완전한 클립이 존재하면 스킵
    if s3_complete_clip_exists(clip_id):
        print(f"🚫 S3에 완전한 클립이 이미 존재함 → 스킵: {clip_id}")
        log_completed(clip_id)  # ✅ 누락 방지!
        return True

    print(f"⏫ 업로드 시작: {clip_id}")
    success = True
    for fname in os.listdir(local_dir):
        local_path = os.path.join(local_dir, fname)
        s3_key = f"{S3_PREFIX}/{clip_id}/{fname}"
        if not upload_to_s3(local_path, s3_key):
            success = False

    if not success:
        log_upload_failed(clip_id)

    return success

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

    try:
        ydl_opts = {
            'quiet': True,
            'no_warnings': True,
            'noplaylist': True,
            'ignoreerrors': True,
            'cookiefile': './cookies.txt',
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

        if os.path.exists(mp4_path):
            extract_audio(mp4_path, mp3_path)
            
        if not (os.path.exists(mp4_path) and os.path.exists(mp3_path) and os.path.exists(json_path)):
            log_failed(clip_id, "다운로드된 파일 없음")
            if os.path.exists(video_dir):
                shutil.rmtree(video_dir)
            return False

        # ✅ S3 업로드
        if upload_clip_folder(clip_id):
            shutil.rmtree(video_dir)
            log_completed(clip_id)
            print(f"업로드 성공: {clip_id}")
            return True
        else:
            print(f"❌ S3 업로드 실패: {clip_id}")
            return False

    except Exception as e:
        error_msg = str(e).lower()
        log_failed(clip_id, error_msg)

        # 일반 실패 시 클린업
        if os.path.exists(video_dir):
            shutil.rmtree(video_dir)
        return False

if __name__ == '__main__':
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    failed_ids = load_failed_ids()
    completed_ids = load_completed_ids()
    data = [item for item in data if item['clip_id'] not in failed_ids and item['clip_id'] not in completed_ids]

    print(f"🔍 처리할 clip_id 수: {len(data)}")

    with Pool(NUM_WORKERS) as pool:
        with tqdm(total=len(data), desc="다운로드 및 업로드 진행") as pbar:
            for result in pool.imap_unordered(download_and_upload, data):
                pbar.update(1)
