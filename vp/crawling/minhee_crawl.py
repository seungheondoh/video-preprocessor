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
import pandas as pd

from gaudio_yt_videos_list.utils.id import get_channel_or_playlist_id, extract_video_id
from utils.fetch_data import *

# 기본 설정
LOG_DIR = "logs"
FAILED_LOG = f"{LOG_DIR}/failed_ids_clip.txt"
UPLOAD_FAILED_LOG = f"{LOG_DIR}/upload_failed_ids.txt"
COMPLETED_LOG = f"{LOG_DIR}/complete_clip_ids.txt"

DOWNLOAD_DIR = "video_crawled_dir"
CSV_PATH = "gaudio_yt_videos_list/channel_and_playlist/final_result.csv"
TXT_DIR = 'gaudio_yt_videos_list/ytids'

S3_BUCKET = "maclab-youtube-crawl"
S3_PREFIX = "minhee_crawling"
NUM_WORKERS = 8

s3 = boto3.client("s3")

# cookies file
COOKIES_FILE_DIR = "./cookies"
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
    # if clip_id in failed_ids or clip_id in completed_ids:
    #     return False

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
            # 'download_ranges': download_range_func(None, [(start_sec, end_sec)]),
            'postprocessors': [],
            # # TODO(minhee): Check whether to add these options or not
            # 'retries': 10,
            # 'sleep_interval': 2,
            # 'socket_timeout': 60,
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
        log_failed(clip_id, error_msg)
        handle_error_message(error_msg, cookie_fn)

        # 일반 실패 시 클린업
        if os.path.exists(video_dir):
            shutil.rmtree(video_dir)
        return False
    
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

def get_video_ids_per_category():
    df = pd.read_csv(CSV_PATH)
    categories_result = []
    video_ids_result = []
    
    for _, row in df.iterrows():
        # get id of channel or playlist
        channel_pl_id = get_channel_or_playlist_id(row)
        
        # read the txt file and get the video ids
        txt_file_path = os.path.join(TXT_DIR, f"{channel_pl_id}.txt")
        if os.path.exists(txt_file_path):
            with open(txt_file_path, "r", encoding="utf-8") as f:
                video_links = [line.strip() for line in f.readlines()]
            video_ids = [extract_video_id(link) for link in video_links]
            
            categories_result.extend([row['category']] * len(video_ids))
            video_ids_result.extend(video_ids)
        else:
            print(f"❌ {txt_file_path} 파일이 존재하지 않음")
    
    # create a dataframe
    result_df = pd.DataFrame({
        'category': categories_result,
        'video_id': video_ids_result
    })
    # remove duplicates
    # result_df = result_df.drop_duplicates(subset=['category', 'video_id'])
    result_df = result_df.drop_duplicates(subset=['video_id']) # TODO(minhee): There are some duplicates in the video_id, where each video falls into different categories.
    
    result_df['clip_id'] = result_df['video_id'] + '_tmp'
    
    return result_df

def main():
    video_ids_df = get_video_ids_per_category()
    video_ids = video_ids_df['video_id'].tolist()
    # remove duplicates
    # data
    video_ids = list(set(video_ids))

    failed_ids = load_failed_ids()
    completed_ids = load_completed_ids()
    data = [video_id for video_id in video_ids if video_id not in failed_ids and video_id not in completed_ids]

    print(f"🔍 처리할 video 수: {len(data)}")

    with Pool(NUM_WORKERS) as pool:
        with tqdm(total=len(data), desc="다운로드 및 업로드 진행") as pbar:
            for result in pool.imap_unordered(download_and_upload, data):
                pbar.update(1)

if __name__ == '__main__':
    main()