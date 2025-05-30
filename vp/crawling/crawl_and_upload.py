import yt_dlp
from yt_dlp.utils import download_range_func
import os
import json
import shutil
import subprocess
import time
import random
import argparse
import pandas as pd
from tqdm import tqdm
from multiprocessing import Pool, Value, Lock

import boto3

from vp.utils.fetch_data import *
from vp.configs.constants import *

s3 = boto3.client("s3")
cur_cookie_index = Value('i', 0)
cookie_lock = Lock()


def extract_audio(mp4_path, mp3_path):
    cmd = [
        "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
        "-i", mp4_path,
        "-vn", "-acodec", "libmp3lame", "-ab", "192k",
        mp3_path
    ]
    subprocess.run(cmd, check=True)


class Crawler:
    def __init__(self, dataset_path=None):
        self._init_data(dataset_path)

    def _init_data(self, dataset_path):
        raise NotImplementedError

    def get_cookie_file_path(self):
        with cookie_lock:
            cookie_file_names = [f for f in os.listdir(COOKIES_FILE_DIR) if f.endswith('.txt')] + ['default.txt']
            cur_cookie_index.value %= len(cookie_file_names)
            cookie_file_name = cookie_file_names[cur_cookie_index.value]
            return os.path.join(COOKIES_FILE_DIR, cookie_file_name)

    def handle_error_message(self, error_message, used_cookie_fn):
        if "not a bot" in error_message or "rate-limited" in error_message:
            with cookie_lock:
                if self.get_cookie_file_path() == used_cookie_fn:
                    cur_cookie_index.value += 1
                    print(f"🔄 쿠키 파일 변경: {self.get_cookie_file_path()}")

    def download_clip(self, args):
        video_id, clip_id, start_sec, end_sec = args

        clip_dir = self.get_clip_dir(clip_id)
        shutil.rmtree(clip_dir, ignore_errors=True)
        os.makedirs(clip_dir, exist_ok=True)

        mp4_path = os.path.join(clip_dir, f"{clip_id}.mp4")
        mp3_path = os.path.join(clip_dir, f"{clip_id}_audio.mp3")
        json_path = os.path.join(clip_dir, f"{clip_id}.info.json")
        mp4_template = os.path.join(clip_dir, f"{clip_id}.%(ext)s")

        cookie_fn = self.get_cookie_file_path()

        try:
            ydl_opts = {
                'quiet': True,
                'no_warnings': True,
                'noplaylist': True,
                'ignoreerrors': False,
                'cookiefile': cookie_fn,
                'outtmpl': mp4_template,
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
            shutil.rmtree(clip_dir, ignore_errors=True)
            return False

        if os.path.exists(mp4_path):
            extract_audio(mp4_path, mp3_path)

        if not (os.path.exists(mp4_path) and os.path.exists(mp3_path) and os.path.exists(json_path)):
            log_result(clip_id, FAILED_LOG, "다운로드된 파일 없음")
            shutil.rmtree(clip_dir, ignore_errors=True)
            return False
        
        # Change file name
        new_mp4_path = os.path.join(clip_dir, f"{clip_id}_video.mp4")
        new_mp3_path = os.path.join(clip_dir, f"{clip_id}_audio.mp3")
        new_json_path = os.path.join(clip_dir, f"{clip_id}_metadata.json")
        os.rename(mp4_path, new_mp4_path)
        os.rename(mp3_path, new_mp3_path)
        os.rename(json_path, new_json_path)

        return True

    def s3_upload(self, video_info):
        _, clip_id, _, _ = video_info
        clip_dir = self.get_clip_dir(clip_id)
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

    def process(self, video_info):
        if self.download_clip(video_info):
            return self.s3_upload(video_info)
        return False

    def run(self):
        print(f"🔍 처리할 clip_id 수: {len(self.data)}")
        with Pool(NUM_WORKERS) as pool:
            with tqdm(total=len(self.data), desc="다운로드 및 업로드 진행") as pbar:
                for _ in pool.imap_unordered(self.process, self.data):
                    pbar.update(1)

    def process(self, video_info):
        raise NotImplementedError("process() must be implemented by subclasses")

class MMTrailerCrawler(Crawler):
    def _init_data(self, dataset_path):
        def refine(item):
            video_id = item['video_id']
            clip_id = item['clip_id']
            start_frame, end_frame = item['clip_start_end_idx']
            fps = item['video_fps']
            return (video_id, clip_id, start_frame / fps, end_frame / fps)

        with open(dataset_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        failed = load_ids(FAILED_LOG)
        completed = load_ids(COMPLETED_LOG)
        filtered = [item for item in data if item['clip_id'] not in failed and item['clip_id'] not in completed]
        self.data = [refine(item) for item in filtered]
        
    def process(self, video_info):
        if self.download_clip(video_info):
            return self.s3_upload(video_info)
        return False
    
class YTCralwer(Crawler):
    def _init_data(self, dataset_path):
        df = pd.read_csv(dataset_path)
        s3_logged = load_ids('/home/minhee/video-preprocessor/vp/crawling/logs/videos_in_s3.txt')
        video_ids = list(set(df['video_id'].tolist()) - set(s3_logged))
        self.data = [(vid, vid, None, None) for vid in video_ids]
        
    def process(self, video_info):
        if self.download_clip(video_info):
            return self.s3_upload(video_info)
        return False


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="YouTube Crawler")
    parser.add_argument('--crawler', type=str, default='mmtrailer', choices=['mmtrailer', 'yt'])
    args = parser.parse_args()

    if args.crawler == 'mmtrailer':
        crawler = MMTrailerCrawler(JSON_PATH)
    else:
        crawler = YTCralwer(YT_VIDEOS_PATH)

    crawler.run()
