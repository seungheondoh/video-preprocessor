# Youtube crawling

- **vp/crawling/video_crawler.py**

병렬 처리로 크롤링 진행 후 S3에 업로드하는 코드. (다운로드 경로 등 설정 필요)

<br>

- **vp/utils/fetch_data.py**

`list_s3_clip_ids`: S3 저장소에서 존재하는 clip_id 리스트를 가져와 저장.

`crawl_s3_clips_from_file`: 원하는 clip_id를 가진 데이터들을 S3에서 로컬로 다운로드하는 함수.

### 사용 예시

- **maclab-youtube-crawl 저장소의 chopin16 폴더 내의 clip_id들을 txt로 얻기**
```
clip_ids = list_s3_clip_ids(
    s3_bucket="maclab-youtube-crawl",
    s3_prefix="chopin16", # "clips", "chopin14", "chopin16" 폴더 존재.
    s3_client=boto3.client("s3"),
    save_path="clip_ids.txt"
)
```
- **maclab-youtube-crawl 저장소의 chopin16 폴더 내의 데이터를 local_clip_dir로 다운로드 (mp4, mp3, json, all 조절 가능)**
```
crawl_s3_clips_from_file(
    clip_list_path="clip_ids.txt",
    s3_bucket="maclab-youtube-crawl",
    s3_prefix="chopin16",
    s3_client=boto3.client("s3"),
    local_clip_dir="/home/daeyong/download_from_s3/",
    mode="all"  # "mp4", "mp3", "json" 도 가능
)
```
