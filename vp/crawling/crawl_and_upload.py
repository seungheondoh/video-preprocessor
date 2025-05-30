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
cur_cookie_index = Value('i', 0)  # shared integer

def extract_audio(mp4_path, mp3_path):
    cmd = [
        "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
        "-i", mp4_path,
        "-vn", "-acodec", "libmp3lame", "-ab", "192k",
        mp3_path
    ]
    subprocess.run(cmd, check=True)

class YTCrawler:
    def __init__(self):
        return
    
    def get_cookie_file_path(self):
        cookie_file_names = [f for f in os.listdir(COOKIES_FILE_DIR) if f.endswith('.txt')] + ['default.txt']
        cur_cookie_index.value = cur_cookie_index.value % len(cookie_file_names)  # Ensure index is within bounds
        cookie_file_name = cookie_file_names[cur_cookie_index.value]
        cookie_file_path = os.path.join(COOKIES_FILE_DIR, cookie_file_name)
        return cookie_file_path

    def handle_error_message(self, error_message, used_cookie_fn) -> None:
        if "not a bot" in error_message or "rate-limited" in error_message:
            with cur_cookie_index.get_lock():  # Lock ensures atomic update
                if self.get_cookie_file_path() != used_cookie_fn: # check if is already changed
                    return
                cur_cookie_index.value += 1
                print(f"🔄 쿠키 파일 변경: {self.get_cookie_file_path()}")

    def download_clip(self, args):
        video_id, clip_id, start_sec, end_sec = args
        if clip_id in failed_ids or clip_id in completed_ids:
            return False

        clip_dir = self.get_clip_dir(clip_id)

        if os.path.exists(clip_dir):
            shutil.rmtree(clip_dir)
        os.makedirs(clip_dir, exist_ok=True)

        mp4_path_template = os.path.join(clip_dir, f"{clip_id}.%(ext)s")
        mp4_path = os.path.join(clip_dir, f"{clip_id}.mp4")
        mp3_path = os.path.join(clip_dir, f"{clip_id}_audio.mp3")
        json_path = os.path.join(clip_dir, f"{clip_id}.info.json")

        cookie_fn = self.get_cookie_file_path()
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
                'postprocessors': [],
            }
            if start_sec is not None and end_sec is not None:
                ydl_opts['download_ranges'] = download_range_func(None, [(start_sec, end_sec)])
            
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
            self.handle_error_message(error_msg, cookie_fn)

            # 일반 실패 시 클린업
            if os.path.exists(clip_dir):
                shutil.rmtree(clip_dir)
            return False
        
        if os.path.exists(mp4_path):
            extract_audio(mp4_path, mp3_path)
            
        if not (os.path.exists(mp4_path) and os.path.exists(mp3_path) and os.path.exists(json_path)):
            log_result(clip_id, FAILED_LOG, "다운로드된 파일 없음")
            if os.path.exists(clip_dir):
                shutil.rmtree(clip_dir)
            return False
        return True

    def s3_upload(self, video_info):
        _, clip_id, _, _ = video_info
        clip_dir = self.get_clip_dir(clip_id)
        # ✅ S3 업로드
        if upload_clip_folder(clip_id):
            shutil.rmtree(clip_dir)
            log_result(clip_id, COMPLETED_LOG)
            print(f"업로드 성공: {clip_id}")
            return True
        else:
            print(f"❌ S3 업로드 실패: {clip_id}")
            return False
        
    def get_clip_dir(self, clip_id):
        return os.path.join(DOWNLOAD_DIR, clip_id)
    
    def run(self, video_info):
        if self.download_clip(video_info):
            return self.s3_upload(video_info)
        return False
        
def refine_item_for_mmtrailer(item):
    video_id = item['video_id']
    clip_id = item['clip_id']
    start_frame, end_frame = item['clip_start_end_idx']
    fps = item['video_fps']
    start_sec = start_frame / fps
    end_sec = end_frame / fps
    
    return (video_id, clip_id, start_sec, end_sec)
    

if __name__ == '__main__':
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    failed_ids = load_ids(FAILED_LOG)
    completed_ids = load_ids(COMPLETED_LOG)
    data = [item for item in data if item['clip_id'] not in failed_ids and item['clip_id'] not in completed_ids]
    data = [refine_item_for_mmtrailer(item) for item in data]

    print(f"🔍 처리할 clip_id 수: {len(data)}")

    crawler = YTCrawler()
    with Pool(NUM_WORKERS) as pool:
        with tqdm(total=len(data), desc="다운로드 및 업로드 진행") as pbar:
            for result in pool.imap_unordered(crawler.run, data):
                pbar.update(1)
