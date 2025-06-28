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
from pathlib import Path
import torch
from torch.multiprocessing import set_start_method
from multiprocessing import Pool, Lock, Manager, current_process

import boto3

from vp.utils.fetch_data import *
from vp.configs.constants import *
from vp.configs.filter_db import filter_dataframe
from vp.crawling.get_music_onset_offset import get_clip_start_and_end

s3 = boto3.client("s3")

# Manage cookies
manager = Manager()
cookie_lock = Lock()
cookie_file_names = [f for f in os.listdir(COOKIES_FILE_DIR) if f.endswith('.txt')]
available_cookie_indices = manager.list(list(range(len(cookie_file_names))))

# Manage GPUs
MAX_PROCS_PER_GPU = 4
NUM_GPUS = torch.cuda.device_count()
MAX_GPU_PROCS = NUM_GPUS * MAX_PROCS_PER_GPU
def get_assigned_device():
    if not torch.cuda.is_available():
        return 'cpu'

    proc_id = int(current_process()._identity[0]) if current_process()._identity else 0

    if proc_id < MAX_GPU_PROCS:
        assigned_gpu = proc_id % NUM_GPUS
        return f'cuda:{assigned_gpu}'
    else:
        return 'cpu'

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
        self.dataset_path = dataset_path

    def init_data(self):
        raise NotImplementedError

    def get_cookie_file_path(self):
        with cookie_lock:
            if not available_cookie_indices:
                print("❌ 모든 쿠키가 사용 불가 상태입니다. 쿠키 인덱스를 재설정합니다.")
                # Reinitialize available_cookie_indices
                available_cookie_indices[:] = list(range(len(cookie_file_names)))
                if not available_cookie_indices:
                    print("❌ 쿠키 인덱스 재설정 실패. 작업을 중단합니다.")
                    os._exit(1)

            index = random.choice(available_cookie_indices)
            return os.path.join(COOKIES_FILE_DIR, cookie_file_names[index])

    def handle_error_message(self, error_message, used_cookie_fn):
        cookie_error_keywords = ['not a bot',
                                 'rate-limited',
                                 'HTTP Error 403: Forbidden',
                                 'does not look like a netscape format cookies file',
                                 'available'
                                 ]
        if any(keyword.lower() in error_message.lower() for keyword in cookie_error_keywords):
            with cookie_lock:
                try:
                    failed_index = cookie_file_names.index(os.path.basename(used_cookie_fn))
                    if failed_index in available_cookie_indices:
                        available_cookie_indices.remove(failed_index)
                        print(f"⚠️ 쿠키 파일 {used_cookie_fn} 사용 불가로 제거")
                except ValueError:
                    return  # Unknown filename; ignore

            # Return another available cookie
            return self.get_cookie_file_path()
        elif 'video unavailable' in error_message.lower():
            return
    
    def _ytlp_download(self, ydl_opts, video_id, clip_id=None):
        cookie_fn = self.get_cookie_file_path()
        ydl_opts['cookiefile'] = cookie_fn
        if clip_id is None:
            clip_id = video_id
        
        # ✅ 랜덤한 시간 지연 추가
        sleep_time = random.uniform(0.2, 0.3)
        print(f"[WAIT] {clip_id} 다운로드 전 대기 중... ({sleep_time:.2f}초)")
        time.sleep(sleep_time)

        print(f">>> {clip_id} 다운로드 중...")
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([f"https://www.youtube.com/watch?v={video_id}"])
            log_result(clip_id, COMPLETED_LOG, '✅ Success')
        except Exception as e:
            error_msg = str(e).lower()
            log_result(clip_id, FAILED_LOG, f'❌ Fail: {error_msg}')
            self.handle_error_message(error_msg, cookie_fn)
            return False
        return True

    def download_clip(self, args):
        video_id, clip_id, start_sec, end_sec = args
        
        clip_dir = get_file_path(clip_id)['clip_dir']
        mp4_path = get_file_path(clip_id)['mp4_path']
        mp3_path = get_file_path(clip_id)['mp3_path']
        json_path = get_file_path(clip_id)['json_path']

        # shutil.rmtree(clip_dir, ignore_errors=True) # TODO(minhee): Unhide this later???
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
            'writeinfojson': True,
            'force_keyframes_at_cuts': True,
            'postprocessors': [],
        }
        start_sec = float(start_sec) if start_sec is not None else None
        end_sec = float(end_sec) if end_sec is not None else None
        ydl_opts['download_ranges'] = download_range_func(None, [(start_sec, end_sec)])

        success = self._ytlp_download(ydl_opts, video_id, clip_id)
        if not success:
            shutil.rmtree(clip_dir, ignore_errors=True)
            return False

        if os.path.exists(ytdlp_mp4_path):
            try:
                extract_audio(ytdlp_mp4_path, ytdlp_mp3_path)
            except subprocess.CalledProcessError as e:
                log_result(clip_id, FAILED_LOG, f"오디오 추출 실패: {str(e)}")
                shutil.rmtree(clip_dir, ignore_errors=True)
                return False

        if not (os.path.exists(ytdlp_mp4_path) and os.path.exists(ytdlp_mp3_path) and os.path.exists(ytdlp_json_path)):
            log_result(clip_id, FAILED_LOG, "다운로드된 파일 없음")
            shutil.rmtree(clip_dir, ignore_errors=True)
            return False
        
        # Change file name
        os.rename(ytdlp_mp4_path, mp4_path)
        os.rename(ytdlp_mp3_path, mp3_path)
        os.rename(ytdlp_json_path, json_path)

        return True

    def s3_upload(self, video_info, s3_prefix, exclude_exts=None):
        if not isinstance(video_info, tuple):
            clip_id = video_info
        else:
            _, clip_id, _, _ = video_info
        clip_dir = get_file_path(clip_id)['clip_dir']
        if upload_clip_folder(clip_id, s3_prefix, exclude_exts=exclude_exts): # upload succeeded
            mp3_path = get_file_path(clip_id)['mp3_path']
            mp4_path = get_file_path(clip_id)['mp4_path']
            for paths_to_remove in [mp3_path, mp4_path]:
                if os.path.exists(paths_to_remove):
                    print(f"Removing file: {paths_to_remove}")
                    os.remove(paths_to_remove)
            log_result(clip_id, COMPLETED_LOG)
            print(f"업로드 성공: {clip_id}")
            return True
        else:
            print(f"❌ S3 업로드 실패: {clip_id}")
            return False
    
    def process(self, video_info):
        raise NotImplementedError("process() must be implemented by subclasses")

    def run(self):
        with Pool(NUM_WORKERS) as pool:
            while True:
                self.init_data()  # Reinitialize data after processing
                if self.data is None or len(self.data) == 0:
                    return
                print(f"🔍 처리할 clip_id 수: {len(self.data)}")
                with tqdm(total=len(self.data), desc="crawl_and_upload.py", smoothing=0.1) as pbar:
                    for _ in pool.imap_unordered(self.process, self.data):
                        pbar.update(1)
                
                sleep_sec = 10.0
                print(f"Sleeping for {sleep_sec} seconds before reinitializing data...")
                time.sleep(sleep_sec)

class MMTrailerCrawler(Crawler):
    def __init__(self, dataset_path):
        super().__init__(dataset_path=dataset_path)
    
    def init_data(self):
        def refine(item):
            video_id = item['video_id']
            clip_id = item['clip_id']
            start_frame, end_frame = item['clip_start_end_idx']
            fps = item['video_fps']
            return (video_id, clip_id, start_frame / fps, end_frame / fps)

        with open(self.dataset_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        failed = load_ids(FAILED_LOG)
        completed = load_ids(COMPLETED_LOG)
        filtered = [item for item in data if item['clip_id'] not in failed and item['clip_id'] not in completed]
        self.data = [refine(item) for item in filtered]
        
    def process(self, video_info):
        if self.download_clip(video_info):
            return self.s3_upload(video_info, S3_PREFIX)
        return False
    
class YTCralwer(Crawler):
    def __init__(self, dataset_path, **kwargs):
        self.clip_info_json_path = YT_CLIP_INFO_JSON_PATH
        self.do_download_audio = kwargs.get('do_download_audio', False)
        self.do_detect_music = kwargs.get('do_detect_music', False)
        self.do_download_clip = kwargs.get('do_download_clip', False)
        self.do_upload_s3 = kwargs.get('do_upload_s3', False)
        self.do_generate_clip_info_json = kwargs.get('do_generate_clip_info_json', False)
        self.pann_max_batch_size = kwargs.get('pann_max_batch_size', None)
        self.upload_exclude_exts = kwargs.get('upload_exclude_exts', None)
        super().__init__(dataset_path=dataset_path)
    
    def init_data(self):
        self.data = None
        
        # TODO(minhee): Find a good way to handle this, rather than dividing into cases like this.
        if self.do_download_audio:
            df = pd.read_csv(self.dataset_path, encoding='utf-8', engine='python')
            df = filter_dataframe(df)
            filtered_video_ids = set(df['video_id'].tolist())
            
            video_ids = set([vid for vid in filtered_video_ids if not os.path.exists(get_file_path(vid)['music_on_off_info_json_path'])])
            video_ids = video_ids - set(load_ids(FAILED_LOG))
            self.data = [(video_id, video_id, None, None) for video_id in video_ids]
        elif self.do_detect_music:
            existing_ids = os.listdir(DOWNLOAD_DIR)
            video_ids = []
            for vid in existing_ids:
                if os.path.exists(get_file_path(vid)['mp3_path']) and not os.path.exists(get_file_path(vid)['panns_inference_json_path']):
                    video_ids.append(vid)
            self.data = [(video_id, video_id, None, None) for video_id in video_ids]
        elif self.do_download_clip:
            clips_ids_already_uploaded = list_s3_clip_ids_that_have_specific_file_type(
                s3_bucket=S3_BUCKET,
                s3_prefix=S3_PREFIX_CLIP,
                s3_client=s3,
                file_ext='.mp4'
            )
            
            if os.path.exists(self.clip_info_json_path):
                with open(self.clip_info_json_path, 'r') as f:
                    clip_info_list = json.load(f)
            else:
                print(f"❌ {self.clip_info_json_path} 파일이 존재하지 않습니다. --do_generate_clip_info_json 옵션으로 클립 정보 JSON을 생성하십시오.") # TODO(minhee): Refine the message
            
            # Remove already uploaded clip
            clip_info_dict = {}
            for element in clip_info_list:
                clip_id = element['clip_id']
                clip_info_dict[clip_id] = element
            for clip_id in clips_ids_already_uploaded:
                if clip_id in clip_info_dict:
                    del clip_info_dict[clip_id]
                
            self.data = []
            for item in clip_info_dict.values():
                if len(item['clip_start_end_sec']) == 2:
                    video_id = item['video_id']
                    clip_id = item['clip_id']
                    start_sec, end_sec = item['clip_start_end_sec']
                    self.data.append((video_id, clip_id, start_sec, end_sec))
        elif self.do_upload_s3:
            # TODO(minhee): Refine this to get already uploaded clip ids and remove them from the list.
            video_ids = os.listdir(DOWNLOAD_DIR)
            self.data = [(video_id, video_id, None, None) for video_id in video_ids]
        elif self.do_generate_clip_info_json:
            df = pd.read_csv(self.dataset_path, encoding='utf-8', engine='python')
            df = filter_dataframe(df)
            filtered_video_ids = set(df['video_id'].tolist())
            self.generate_clip_info_json(filtered_video_ids)
        
    def download_audio_only(self, video_id):
        output_dir = get_file_path(video_id)['clip_dir']
        mp3_path = get_file_path(video_id)['mp3_path']
            
        if os.path.exists(mp3_path):
            return True
        
        ydl_opts = {
            'format': 'bestaudio/best',
            'outtmpl': f'{output_dir}/%(id)s.%(ext)s',
            'noplaylist': True,
            'quiet': True,
            'no_warnings': True,
            'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3', # Download in mp3
            'preferredquality': '192',
            }],
        }
        if not self._ytlp_download(ydl_opts, video_id):
            return False
        yt_mp3_path = os.path.join(output_dir, f"{video_id}.mp3")
        os.rename(yt_mp3_path, mp3_path)
        return True
    
    def generate_clip_info_json(self, video_ids):
        # music onset and offset info json path
        music_on_off_info_json_suffix = get_file_path("")['music_on_off_info_json_path']
        json_info_dir = Path(DOWNLOAD_DIR)
        # download_clip_from_s3("", json_info_dir, S3_BUCKET, S3_PREFIX, s3, specific_ext=music_on_off_info_json_suffix)
        
        clip_info_list = []
        for json_file in tqdm(list(json_info_dir.rglob(f"*{music_on_off_info_json_suffix}"))):
            video_id = json_file.relative_to(json_info_dir).parts[0]
            if video_id not in video_ids:
                continue
            with open(json_file, 'r') as f:
                music_onset_offset = json.load(f)
                
                # Update new dataset list
                dict_item = {
                    "video_id": video_id,
                    "clip_id": f"{video_id}_{0:07d}",
                    "clip_start_end_sec": music_onset_offset['selected_clip'],
                }
                clip_info_list.append(dict_item)
                
        # Save new dataset JSON
        with open(self.clip_info_json_path, 'w') as f:
            json.dump(clip_info_list, f, indent=4)
        
    def process(self, video_info):
        video_id, clip_id, _, _ = video_info
        clip_dir = get_file_path(clip_id)['clip_dir']
        
        # TODO(minhee): Code is too dirty fix this.
        if self.do_download_audio:
            # Download the full audio (audio only)
            success = self.download_audio_only(video_id)
            if not success:
                return False
        if self.do_detect_music:
            # Get clips' onset, offset (this includes PANN inference)
            success = get_clip_start_and_end(video_id, clip_dir, max_batch_size=self.pann_max_batch_size, device=get_assigned_device())
            if not success:
                return False
            # TODO(minhee): Remove this later
            mp3_path = get_file_path(clip_id)['mp3_path']
            if os.path.exists(mp3_path):
                # Remove mp3 file after upload
                print(f"Removing mp3 file: {mp3_path}")
                os.remove(mp3_path)
            # TODO(minhee): Remove up to here
        # Download clip video, and extract audio
        if self.do_download_clip:
            success = self.download_clip(video_info)
            if not success:
                return False
        if self.do_upload_s3:
            if self.do_download_clip:
                s3_prefix = S3_PREFIX_CLIP
            else:
                s3_prefix = S3_PREFIX
            success = self.s3_upload(video_info, s3_prefix=s3_prefix, exclude_exts=self.upload_exclude_exts)
            if not success:
                return False
        return True
    
if __name__ == '__main__':
    # Add this before running multiprocessing
    try:
        set_start_method('spawn')  # Needed for CUDA with multiprocessing
    except RuntimeError:
        pass

    parser = argparse.ArgumentParser(description="YouTube Crawler")
    parser.add_argument('--crawler', type=str, choices=['mmtrailer', 'yt'])
    # TODO(minhee): This is only used for args.crawler=='yt' case. Clean these up.
    parser.add_argument('--do_download_audio', action='store_true')
    parser.add_argument('--do_detect_music', action='store_true')
    parser.add_argument('--do_download_clip', action='store_true')
    parser.add_argument('--do_upload_s3', action='store_true')
    parser.add_argument('--do_generate_clip_info_json', action='store_true')
    parser.add_argument('--pann_max_batch_size', type=int)
    parser.add_argument('--n_workers', type=int)
    parser.add_argument('--upload_exclude_exts', type=list)
    args = parser.parse_args()

    if args.n_workers is not None:
        NUM_WORKERS = args.n_workers
    if args.crawler == 'mmtrailer':
        crawler = MMTrailerCrawler(JSON_PATH)
    elif args.crawler == 'yt':
        kwargs = {
            'do_download_audio': args.do_download_audio,
            'do_detect_music': args.do_detect_music,
            'do_download_clip': args.do_download_clip,
            'do_upload_s3': args.do_upload_s3,
            'do_generate_clip_info_json': args.do_generate_clip_info_json,
            'pann_max_batch_size': args.pann_max_batch_size,
            'upload_exclude_exts': args.upload_exclude_exts,
        }
        crawler = YTCralwer(VIDEO_CSV_PATH, **kwargs)
    else:
        raise ValueError("Invalid crawler type. Choose 'mmtrailer' or 'yt'.")

    crawler.run()