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
from vp.crawling.get_music_onset_offset import get_clip_start_and_end

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
    
    def _ytlp_download(self, ydl_opts, video_id, clip_id=None):
        cookie_fn = self.get_cookie_file_path()
        ydl_opts['cookiefile'] = cookie_fn
        if clip_id is None:
            clip_id = video_id
        
        # ✅ 랜덤한 시간 지연 추가 (0.5초 ~ 1.5초)
        sleep_time = random.uniform(0.5, 1.5)
        print(f"[WAIT] {clip_id} 다운로드 전 대기 중... ({sleep_time:.2f}초)")
        time.sleep(sleep_time)

        print(f">>> {clip_id} 다운로드 중...")
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([f"https://www.youtube.com/watch?v={video_id}"])
        except Exception as e:
            error_msg = str(e).lower()
            log_result(clip_id, FAILED_LOG, error_msg)
            self.handle_error_message(error_msg, cookie_fn)
            return False
        return True

    def download_clip(self, args):
        video_id, clip_id, start_sec, end_sec = args

        clip_dir, mp4_path, mp3_path, json_path = self.get_file_path(clip_id)
        shutil.rmtree(clip_dir, ignore_errors=True)
        os.makedirs(clip_dir, exist_ok=True)

        # file path used after download with yt-dlp
        ytdlp_mp4_path = os.path.join(clip_dir, f"{clip_id}.mp4")
        ytdlp_mp3_path = os.path.join(clip_dir, f"{clip_id}_audio.mp3")
        ytdlp_json_path = os.path.join(clip_dir, f"{clip_id}.info.json")
        mp4_template = os.path.join(clip_dir, f"{clip_id}.%(ext)s")
        
        ydl_opts = {
            'quiet': True,
            'no_warnings': True,
            'noplaylist': True,
            'ignoreerrors': False,
            'outtmpl': mp4_template,
            'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/mp4',
            'merge_output_format': 'mp4',
            # 'writeinfojson': True,
            'force_keyframes_at_cuts': True,
            'postprocessors': [],
        }
        if start_sec is not None and end_sec is not None:
            ydl_opts['download_ranges'] = download_range_func(None, [(start_sec, end_sec)])

        success = self._ytlp_download(ydl_opts, video_id, clip_id)
        if not success:
            shutil.rmtree(clip_dir, ignore_errors=True)
            return False

        if os.path.exists(ytdlp_mp4_path):
            extract_audio(ytdlp_mp4_path, ytdlp_mp3_path)

        if not (os.path.exists(ytdlp_mp4_path) and os.path.exists(ytdlp_mp3_path) and os.path.exists(ytdlp_json_path)):
            log_result(clip_id, FAILED_LOG, "다운로드된 파일 없음")
            shutil.rmtree(clip_dir, ignore_errors=True)
            return False
        
        # Change file name
        os.rename(ytdlp_mp4_path, mp4_path)
        os.rename(ytdlp_mp3_path, mp3_path)
        os.rename(ytdlp_json_path, json_path)

        return True

    def s3_upload(self, video_info):
        if not isinstance(video_info, tuple):
            clip_id = video_info
        else:
            _, clip_id, _, _ = video_info
        clip_dir, _, _, _ = self.get_file_path(clip_id)
        if upload_clip_folder(clip_id):
            shutil.rmtree(clip_dir)
            log_result(clip_id, COMPLETED_LOG)
            print(f"업로드 성공: {clip_id}")
            return True
        else:
            print(f"❌ S3 업로드 실패: {clip_id}")
            return False

    def get_file_path(self, clip_id):
        clip_dir = os.path.join(DOWNLOAD_DIR, clip_id)
        mp4_path = os.path.join(clip_dir, f"{clip_id}_video.mp4")
        mp3_path = os.path.join(clip_dir, f"{clip_id}_audio.mp3")
        json_path = os.path.join(clip_dir, f"{clip_id}_metadata.json")
        return clip_dir, mp4_path, mp3_path, json_path

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
    def __init__(self, dataset_path):
        super().__init__(dataset_path=dataset_path)
    
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
    def __init__(self, dataset_path, options):
        self.clip_info_json_path = YT_CLIP_INFO_JSON_PATH
        self.clip_info_list = []
        super().__init__(dataset_path=dataset_path)
        self.download_audio = options[0]
        self.detect_music = options[1]
        self.download_video = options[2]
    
    def _init_data(self, dataset_path):
        df = pd.read_csv(dataset_path)
        
        # Filter out already processed video_ids
        if os.path.exists(self.clip_info_json_path):
            with open(self.clip_info_json_path, 'r') as f:
                self.clip_info_list = json.load(f)
        existing_video_ids = set(item['video_id'] for item in self.clip_info_list) if self.clip_info_list else set()
        video_ids = list(set(df['video_id'].tolist()) - existing_video_ids)
        self.data = [(vid, vid, None, None) for vid in video_ids]
        
    def get_file_path(self, clip_id):
        if self.download_audio or self.detect_music:
            clip_dir = os.path.join(DOWNLOAD_DIR, clip_id)
            mp4_path = None
            mp3_path = os.path.join(clip_dir, f"{clip_id}.webm")
            json_path = os.path.join(clip_dir, f"{clip_id}.info.json")
            return clip_dir, mp4_path, mp3_path, json_path
        else:
            return super().get_file_path(clip_id)
        
    def download_audio_only(self, video_id, output_dir):
        if os.path.exists(output_dir):
            return True
        
        ydl_opts = {
            'format': 'bestaudio/best',
            'outtmpl': f'{output_dir}/%(id)s.%(ext)s',
            'noplaylist': True,
            'quiet': True,
            'no_warnings': True,
            'postprocessors': [],  # No conversion
            'writeinfojson': True,
        }
        return self._ytlp_download(ydl_opts, video_id)
    
    def download_clips_per_video(self, video_id):
        video_dir, _, mp3_path, _ = self.get_file_path(video_id)
        # TODO(minhee): Find a way to handle file dir, and avoid hard coding
        clip_onset_offset_path = os.path.join(video_dir, os.path.basename(mp3_path).replace(".mp3", "_clip_info.json"))
        
        with open(clip_onset_offset_path) as f:
            music_onset_offset = json.load(f)
        
        for idx, (clip_start, clip_end) in enumerate(music_onset_offset):
            new_clip_id = f"{video_id}_{idx:07d}"
            args = (video_id, new_clip_id, clip_start, clip_end)
            
            # Download clipped video, and extract audio
            success = self.download_clip(args)
            if not success:
                return False

            # Upload to S3
            self.s3_upload(new_clip_id)
            
            # Update new dataset list
            dict_item = {
                "video_id": video_id,
                "clip_id": new_clip_id,
                "clip_start_end_sec": (clip_start, clip_end),
            }
            self.clip_info_list.append(dict_item)
        
            # Save new dataset JSON
            with open(self.clip_info_json_path, 'w') as f:
                json.dump(self.clip_info_list, f, indent=4)
            
        # TODO(minhee): Cleaning up like this is dangerous, so I'll hide it for now
        # # Cleanup original download
        # shutil.rmtree(video_dir)
        
        
    def process(self, video_info):
        video_id, _, _, _ = video_info
        video_dir, _, mp3_path, _ = self.get_file_path(video_id)
        
        if self.download_audio:
            # Download the full audio (audio only)
            success = self.download_audio_only(video_id, video_dir)
            if not success:
                return False
        if self.detect_music:
            # Get clips' onset, offset (this includes PANN inference)
            get_clip_start_and_end(mp3_path, video_dir)
        # Download clip video, and extract audio
        if self.download_video:
            self.download_clips_per_video(video_id)
        
        return True
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="YouTube Crawler")
    parser.add_argument('--crawler', type=str, choices=['mmtrailer', 'yt'])
    # TODO(minhee): This is only used for args.crawler=='yt' case. Clean these up.
    parser.add_argument('--download_audio', action='store_true')
    parser.add_argument('--detect_music', action='store_true')
    parser.add_argument('--download_video', action='store_true')
    args = parser.parse_args()

    if args.crawler == 'mmtrailer':
        crawler = MMTrailerCrawler(JSON_PATH)
    elif args.crawler == 'yt':
        download_audio = args.download_audio
        detect_music = args.detect_music
        download_video = args.download_video
        crawler = YTCralwer(VIDEO_CSV_PATH, (download_audio, detect_music, download_video))
    else:
        raise ValueError("Invalid crawler type. Choose 'mmtrailer' or 'yt'.")

    crawler.run()
