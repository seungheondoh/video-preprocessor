import os
from tqdm import tqdm
import boto3

from vp.configs.constants import *

s3 = boto3.client("s3")

# FAILED_LOG txt file에 있는 이미 실패한 clip_id를 가져와서 다시 실행하지 않도록 함.
def load_failed_ids():
    if os.path.exists(FAILED_LOG):
        with open(FAILED_LOG, "r", encoding="utf-8") as f:
            return set(line.strip() for line in f)
    return set()

# COMPLETED_LOG txt file에 있는 이미 성공한 clip_id를 가져와서 다시 실행하지 않도록 함.
def load_completed_ids():
    if os.path.exists(COMPLETED_LOG):
        with open(COMPLETED_LOG, "r", encoding="utf-8") as f:
            return set(line.strip() for line in f)
    return set()

def log_result(clip_id, logging_file_path, error_msg=None):
    os.makedirs(os.path.dirname(logging_file_path), exist_ok=True)
    with open(FAILED_LOG, "a", encoding="utf-8") as f:
        f.write(f"{clip_id}\n")
    if error_msg is not None:
        print(f"[ERROR] {clip_id} 실패 기록됨. 사유: {error_msg}")

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

# S3 저장소에 로컬에 저장된 파일을 업로드(내부 함수)
def upload_to_s3(local_path, s3_key):
    try:
        s3.upload_file(local_path, S3_BUCKET, s3_key)
        return True
    except Exception as e:
        print(f"❌ S3 업로드 실패: {s3_key}, 사유: {e}")
        return False

# S3 저장소에 로컬에 저장된 파일을 업로드
def upload_clip_folder(clip_id):
    local_dir = os.path.join(DOWNLOAD_DIR, clip_id)
    if not os.path.exists(local_dir):
        return False

    # ✅ S3에 완전한 클립이 존재하면 스킵
    if s3_complete_clip_exists(clip_id):
        print(f"🚫 S3에 완전한 클립이 이미 존재함 → 스킵: {clip_id}")
        log_completed(clip_id) 
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


def local_to_s3(local_clip_dir, clip_id, s3_bucket, s3_prefix, s3_client, 
                check_clip_exists_fn=None, log_completed_fn=None, log_failed_fn=None):
    """
    로컬에 저장된 클립 폴더를 S3 버킷에 업로드하는 함수.

    Parameters:
    - local_clip_dir (str): 로컬 상위 디렉토리 경로 (ex: '/downloads')
    - clip_id (str): 업로드할 클립 ID
    - s3_bucket (str): 업로드 대상 S3 버킷 이름
    - s3_prefix (str): S3 내 저장될 경로 prefix (ex: 'clips')
    - s3_client (boto3.client): boto3의 S3 클라이언트 객체
    - check_clip_exists_fn (callable, optional): S3에 클립 존재 여부를 확인하는 함수
    - log_completed_fn (callable, optional): 업로드 완료 로깅 함수
    - log_failed_fn (callable, optional): 업로드 실패 로깅 함수
    """

    local_dir = os.path.join(local_clip_dir, clip_id)
    if not os.path.exists(local_dir):
        print(f"❌ 로컬 폴더 없음: {local_dir}")
        return False

    # ✅ S3에 이미 존재하면 스킵
    if check_clip_exists_fn and check_clip_exists_fn(clip_id):
        print(f"🚫 S3에 완전한 클립이 이미 존재함 → 스킵: {clip_id}")
        if log_completed_fn:
            log_completed_fn(clip_id)
        return True

    print(f"⏫ 업로드 시작: {clip_id}")
    success = True

    for fname in os.listdir(local_dir):
        local_path = os.path.join(local_dir, fname)
        s3_key = f"{s3_prefix}/{clip_id}/{fname}"

        try:
            s3_client.upload_file(local_path, s3_bucket, s3_key)
        except Exception as e:
            print(f"❌ S3 업로드 실패: {s3_key}, 사유: {e}")
            success = False

    if not success:
        if log_failed_fn:
            log_failed_fn(clip_id)

    return success


def download_clip_from_s3(clip_id, local_clip_dir, s3_bucket, s3_prefix, s3_client):
    # 사용 예시:
    # success = s3_to_local_clip_id(
    #     clip_id="-_3bKbYqbvQ_0000376",
    #     local_clip_dir="/home/daeyong/llm_music_understanding/s3_to_local_clip_id/",
    #     s3_bucket="maclab-youtube-crawl",
    #     s3_prefix="chopin16",
    #     s3_client=s3
    # )
    
    """
    S3에 저장된 하나의 clip_id 폴더(mp4, mp3, json)를 로컬로 다운로드하는 함수.

    Parameters:
    - clip_id (str): 다운로드할 클립 ID
    - local_clip_dir (str): 로컬 상위 디렉토리 경로 (ex: '/downloads')
    - s3_bucket (str): 다운로드 대상 S3 버킷 이름
    - s3_prefix (str): S3 내 저장된 경로 prefix (ex: 'clips')
    - s3_client (boto3.client): boto3의 S3 클라이언트 객체
    """

    s3_dir_prefix = f"{s3_prefix}/{clip_id}/"
    local_dir = os.path.join(local_clip_dir, clip_id)

    # 로컬 폴더 없으면 생성
    os.makedirs(local_dir, exist_ok=True)

    # S3에서 파일 리스트 가져오기
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=s3_bucket, Prefix=s3_dir_prefix)

    found_any = False

    for page in pages:
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('/'):
                continue  # 디렉토리 스킵

            _, filename = key.rsplit('/', 1)
            local_path = os.path.join(local_dir, filename)

            try:
                s3_client.download_file(s3_bucket, key, local_path)
                print(f"✅ 다운로드 완료: {key} → {local_path}")
                found_any = True
            except Exception as e:
                print(f"❌ 다운로드 실패: {key}, 사유: {e}")

    if not found_any:
        print(f"⚠️ S3에서 clip_id {clip_id}에 해당하는 파일이 없습니다.")

    return found_any

def download_mp4_from_s3(clip_id, local_clip_dir, s3_bucket, s3_prefix, s3_client):
    # S3에서 clip_id 폴더의 mp4 파일만 로컬로 다운로드
    return _download_specific_filetype_from_s3(clip_id, local_clip_dir, s3_bucket, s3_prefix, s3_client, target_ext=".mp4")

def download_mp3_from_s3(clip_id, local_clip_dir, s3_bucket, s3_prefix, s3_client):
    # S3에서 clip_id 폴더의 mp3 파일만 로컬로 다운로드
    return _download_specific_filetype_from_s3(clip_id, local_clip_dir, s3_bucket, s3_prefix, s3_client, target_ext=".mp3")

def download_json_from_s3(clip_id, local_clip_dir, s3_bucket, s3_prefix, s3_client):
    # S3에서 clip_id 폴더의 json 파일만 로컬로 다운로드
    return _download_specific_filetype_from_s3(clip_id, local_clip_dir, s3_bucket, s3_prefix, s3_client, target_ext=".json")

def _download_specific_filetype_from_s3(clip_id, local_clip_dir, s3_bucket, s3_prefix, s3_client, target_ext):
    # 내부 공통 함수: S3에서 특정 확장자 파일만 다운로드
    s3_dir_prefix = f"{s3_prefix}/{clip_id}/"
    local_dir = os.path.join(local_clip_dir, clip_id)

    # 로컬 폴더 없으면 생성
    os.makedirs(local_dir, exist_ok=True)

    # S3에서 파일 리스트 가져오기
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=s3_bucket, Prefix=s3_dir_prefix)

    found_any = False

    for page in pages:
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('/'):
                continue  # 디렉토리 스킵

            _, filename = key.rsplit('/', 1)
            _, ext = os.path.splitext(filename)

            if ext.lower() != target_ext:
                continue  # 원하는 확장자 아니면 스킵

            local_path = os.path.join(local_dir, filename)

            try:
                s3_client.download_file(s3_bucket, key, local_path)
                print(f"✅ 다운로드 완료: {key} → {local_path}")
                found_any = True
            except Exception as e:
                print(f"❌ 다운로드 실패: {key}, 사유: {e}")

    if not found_any:
        print(f"⚠️ S3에서 clip_id {clip_id}에 대해 '{target_ext}' 파일을 찾을 수 없습니다.")

    return found_any


def list_s3_clip_ids(s3_bucket, s3_prefix, s3_client, save_path=None):
    # 사용 예시:
    # clip_ids = list_s3_clip_ids(
    #     s3_bucket="maclab-youtube-crawl",
    #     s3_prefix="chopin16",
    #     s3_client=s3,
    #     save_path="clip_ids.txt"
    # )
    """
    S3 버킷에서 clip_id 폴더 리스트를 가져오고 저장하는 함수.

    Parameters:
    - s3_bucket (str): S3 버킷 이름
    - s3_prefix (str): 검색할 S3 prefix (ex: 'chopin16')
    - s3_client (boto3.client): boto3의 S3 클라이언트 객체
    - save_path (str, optional): 결과를 저장할 로컬 파일 경로 (ex: 'clip_id_list.txt')

    Returns:
    - clip_ids (list of str): clip_id 리스트
    """

    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix)

    clip_ids = set()

    for page in pages:
        for obj in page.get('Contents', []):
            key = obj['Key']
            parts = key.split('/')
            if len(parts) >= 2 and parts[0] == s3_prefix:
                clip_id = parts[1]
                if clip_id:  # 빈 값이 아니면
                    clip_ids.add(clip_id)

    clip_ids = sorted(list(clip_ids))  # 정렬

    # 저장 옵션
    if save_path:
        with open(save_path, 'w', encoding='utf-8') as f:
            for clip_id in clip_ids:
                f.write(f"{clip_id}\n")
        print(f"✅ clip_id 리스트 저장 완료: {save_path}")
    
    print(f"총 {len(clip_ids)}개 clip_id 발견!")
    return clip_ids


def crawl_s3_clips_from_file(clip_list_path, s3_bucket, s3_prefix, s3_client, local_clip_dir, mode="all"):
    # 사용 예시:
    # crawl_s3_clips_from_file(
    #     clip_list_path="clip_ids.txt",
    #     s3_bucket="maclab-youtube-crawl",
    #     s3_prefix="clips",
    #     s3_client=s3,
    #     local_clip_dir="/home/daeyong/llm_music_understanding/s3_to_local_clip_id/",
    #     mode="all"  # "mp4", "mp3", "json" 도 가능
    # )
    
    """
    미리 저장된 clip_ids 파일을 불러와서 원하는 파일 종류를 다운로드하는 함수.

    Parameters:
    - clip_list_path (str): clip_id 리스트가 저장된 파일 경로 (ex: 'clip_ids.txt')
    - s3_bucket (str): S3 버킷 이름
    - s3_prefix (str): 검색할 S3 prefix (ex: 'chopin16')
    - s3_client (boto3.client): boto3의 S3 클라이언트 객체
    - local_clip_dir (str): 다운로드할 로컬 상위 폴더 경로
    - mode (str): "mp4", "mp3", "json", "all" 중 선택
    """

    # 1. clip_id 리스트 불러오기
    if not os.path.exists(clip_list_path):
        print(f"❌ clip_ids 파일이 존재하지 않습니다: {clip_list_path}")
        return

    with open(clip_list_path, 'r', encoding='utf-8') as f:
        clip_ids = [line.strip() for line in f if line.strip()]

    print(f"🎯 총 {len(clip_ids)}개의 clip_id 대상 다운로드 시작합니다. (mode: {mode})")
    
    # 일부만 다운로드 하고 싶은 경우 여기서 clip_ids 리스트를 조절
    clip_ids = clip_ids[:5]

    # 2. 각 clip_id마다 다운로드 수행
    for clip_id in tqdm(clip_ids, desc="Downloading clips"):
        try:
            if mode == "mp4":
                download_mp4_from_s3(clip_id, local_clip_dir, s3_bucket, s3_prefix, s3_client)
            elif mode == "mp3":
                download_mp3_from_s3(clip_id, local_clip_dir, s3_bucket, s3_prefix, s3_client)
            elif mode == "json":
                download_json_from_s3(clip_id, local_clip_dir, s3_bucket, s3_prefix, s3_client)
            elif mode == "all":
                download_clip_from_s3(clip_id, local_clip_dir, s3_bucket, s3_prefix, s3_client)
            else:
                print(f"❌ 잘못된 mode 입력: {mode}. ('mp4', 'mp3', 'json', 'all' 중 하나여야 합니다.)")
                return
        except Exception as e:
            print(f"⚠️ clip_id {clip_id} 다운로드 중 에러 발생: {e}")

    print(f"✅ 다운로드 완료! (mode: {mode})")