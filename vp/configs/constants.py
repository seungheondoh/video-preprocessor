# configs/constants.py

# === Shared settings (public) ===
LOG_DIR = "logs"
FAILED_LOG = f"{LOG_DIR}/failed_ids_clip.txt"
UPLOAD_FAILED_LOG = f"{LOG_DIR}/upload_failed_ids.txt"
COMPLETED_LOG = f"{LOG_DIR}/complete_clip_ids.txt"
COOKIES_FILE_DIR = "./cookies"
S3_BUCKET = "maclab-youtube-crawl"

# === Private/user-specific (defaults here, override in user_config.py) ===
DOWNLOAD_DIR = None
S3_PREFIX = None
NUM_WORKERS = None
JSON_PATH = None

try:
    from .user_config import *  # override private settings
except ImportError:
    pass