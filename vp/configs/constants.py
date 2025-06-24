import os

# === Private/user-specific (defaults here, override in user_config.py) ===
DOWNLOAD_DIR = None
JSON_PATH = None
S3_PREFIX = None
NUM_WORKERS = None
GPU_NUMBERS = None # e.g. "0,1,2,3,4,5,6"
DATASET_TYPE = None # "korea", "global", or "all"

try:
    from .user_config import *  # override private settings
except ImportError:
    pass

# === Shared settings (public) ===
_PATH_TO_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_PATH_TO_VP_CRAWLING = f"{_PATH_TO_PROJECT_ROOT}/vp/crawling"

# Directory
LOG_DIR = f"{_PATH_TO_VP_CRAWLING}/logs"
COOKIES_FILE_DIR = f"{_PATH_TO_VP_CRAWLING}/cookies"
CKPT_DIR = f"{_PATH_TO_PROJECT_ROOT}/ckpt"
DAFTPUNK_DIR = "/media/daftpunk4/home/seungheon/gaudio/data"
DB_DIR = f"{DAFTPUNK_DIR}/db/{DATASET_TYPE}" if DATASET_TYPE != "all" else f"{DAFTPUNK_DIR}/db"

# Video List
VIDEO_CSV_PATH = f'{DB_DIR}/videos.csv' if DATASET_TYPE != "all" else f'{DB_DIR}/videos_merged.csv'

# Clip info
YT_CLIP_INFO_JSON_PATH = f"{_PATH_TO_PROJECT_ROOT}/yt_dataset.json"

# Log file path
FAILED_LOG = f"{LOG_DIR}/failed_clip_ids.csv"
UPLOAD_FAILED_LOG = f"{LOG_DIR}/upload_failed_ids.txt"
COMPLETED_LOG = f"{LOG_DIR}/completed_clip_ids.txt"

# S3
S3_BUCKET = "maclab-youtube-crawl"

# Clipping after PANN inference
PANN_CLIP_DURATION_SEC = 20
MUSIC_LOGIT_THRESHOLD = 0.7
MAX_CLIP_SEC = 30
CLIP_PADDING_SEC = (MAX_CLIP_SEC - PANN_CLIP_DURATION_SEC) // 2  # Padding on each side of the clip

@staticmethod
def get_file_path(clip_id):
    """ Set clip_id to empty string if you want to get suffixes only. """
    suffix_dict = {
        "clip_dir": "",
        "mp4_path": "_video.mp4",
        "mp3_path": "_audio.mp3",
        "json_path": "_metadata.json",
        "music_on_off_info_json_path": "_clip_info.json",
        "panns_inference_json_path": "_panns_result.json",
    }
    
    if len(clip_id) == 0:
        return suffix_dict
    
    file_path_dict = {}
    for key, suffix in suffix_dict.items():
        if key == "clip_dir":
            file_path_dict[key] = os.path.join(DOWNLOAD_DIR, clip_id)
        else:
            file_path_dict[key] = os.path.join(DOWNLOAD_DIR, clip_id, f"{clip_id}{suffix}")
    
    return file_path_dict