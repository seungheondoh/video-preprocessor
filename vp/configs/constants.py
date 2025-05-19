import os

# === Private/user-specific (defaults here, override in user_config.py) ===
DOWNLOAD_DIR = None
JSON_PATH = None
S3_PREFIX = None
NUM_WORKERS = None

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

# Log file path
FAILED_LOG = f"{LOG_DIR}/failed_ids_clip.txt"
UPLOAD_FAILED_LOG = f"{LOG_DIR}/upload_failed_ids.txt"
COMPLETED_LOG = f"{LOG_DIR}/complete_clip_ids.txt"

# S3
S3_BUCKET = "maclab-youtube-crawl"

if __name__=='__main__':
    print(_PATH_TO_PROJECT_ROOT)