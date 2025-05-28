import yt_dlp
from yt_dlp.utils import download_range_func
import os
import pandas as pd
import shutil
import subprocess
from tqdm import tqdm
import time
import random
from multiprocessing import Value

from vp.annotation.music_detection import extract_pann_logits
from vp.configs.constants import *

DOWNLOAD_DIR = "/home/minhee/video-preprocessor/annotated_clips"
CKPT_DIR = "/home/minhee/video-preprocessor/ckpt"
SAMPLE_RATE = 32000
DEVICE = "cuda"

# cookies file
cur_cookie_index = Value('i', 0)  # shared integer

def get_cookie_file_path():
    cookie_file_names = [f for f in os.listdir(COOKIES_FILE_DIR) if f.endswith('.txt')] + ['default.txt']
    cur_cookie_index.value = cur_cookie_index.value % len(cookie_file_names)  # Ensure index is within bounds
    cookie_file_name = cookie_file_names[cur_cookie_index.value]
    cookie_file_path = os.path.join(COOKIES_FILE_DIR, cookie_file_name)
    return cookie_file_path

def extract_audio(mp4_path, mp3_path):
    cmd = [
        "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
        "-i", mp4_path,
        "-vn", "-acodec", "libmp3lame", "-ab", "192k",
        mp3_path
    ]
    subprocess.run(cmd, check=True)

def download_clip_and_infer(video_id, start_sec, end_sec):
    clip_id = f"{video_id}"
    video_dir = os.path.join(DOWNLOAD_DIR, clip_id)

    os.makedirs(video_dir, exist_ok=True)

    mp4_path_template = os.path.join(video_dir, f"{clip_id}.%(ext)s")
    mp4_path = os.path.join(video_dir, f"{clip_id}.mp4")
    mp3_path = os.path.join(video_dir, f"{clip_id}_audio.mp3")
    if os.path.exists(mp3_path):
        print(f"이미 존재하는 오디오 파일: {mp3_path}")
        return

    cookie_fn = get_cookie_file_path()
    try:
        ydl_opts = {
            'quiet': True,
            'no_warnings': True,
            'noplaylist': True,
            'ignoreerrors': False, # Changed to False so that the exception is handled
            'cookiefile': cookie_fn,
            'outtmpl': mp4_path_template,
            'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/mp4',
            'merge_output_format': 'mp4',
            'writeinfojson': True,
            'force_keyframes_at_cuts': True,
            'download_ranges': download_range_func(None, [(start_sec, end_sec)]),
            'postprocessors': [],
        }

        sleep_time = random.uniform(0.5, 1.5)
        print(f"[WAIT] {clip_id} 다운로드 전 대기 중... ({sleep_time:.2f}초)")
        time.sleep(sleep_time)

        print(f">>> {clip_id} 다운로드 중... ({start_sec:.2f}s ~ {end_sec:.2f}s)")
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([f"https://www.youtube.com/watch?v={video_id}"])

        if os.path.exists(mp4_path):
            extract_audio(mp4_path, mp3_path)
            print(f"✅ 완료: {clip_id}")

            # Run PANN inference
            print(f">>> PANN 추론 중: {clip_id}")
            extract_pann_logits(mp3_path, video_dir, CKPT_DIR, DEVICE, SAMPLE_RATE)
            print(f"🎵 PANN 완료: {clip_id}")

        else:
            print(f"❌ 실패 (파일 없음): {clip_id}")

    except Exception as e:
        print(f"❌ 다운로드 중 오류 발생 - {clip_id}: {e}")
        if os.path.exists(video_dir):
            shutil.rmtree(video_dir)

if __name__ == '__main__':
    df = pd.read_csv('/home/minhee/video-preprocessor/gaudio_yt_videos_list/clip_information.csv')

    for _, row in tqdm(df.iterrows(), total=len(df), desc="클립 처리 중"):
        video_id = row['video_id']

        # Process music clip
        START_COL, END_COL = 'music_onset', 'nonmusic_offset'

        if pd.notna(row[START_COL]) and pd.notna(row[END_COL]):
            download_clip_and_infer(
                video_id,
                start_sec=row[START_COL],
                end_sec=row[END_COL]
            )