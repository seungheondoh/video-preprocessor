import os
import json
import numpy as np

from vp.annotation.music_detection import extract_pann_logits
from vp.configs.constants import *

RANDOM_SEED = 42
np.random.seed(RANDOM_SEED)

def get_clip_start_and_end(video_id, output_dir, max_batch_size=None, device='cuda') -> bool:
    # get music onset and offset using PANN
    panns_result_path = get_file_path(video_id)['panns_inference_json_path']
    if not os.path.exists(panns_result_path):
        mp3_path = get_file_path(video_id)['mp3_path']
        if not os.path.exists(mp3_path):
            print(f'mp3_path {mp3_path} does not exist.')
            return False
        print(f"🔍 PANN 추론 시작: {mp3_path}")
        try:
            extract_pann_logits(audio_path=mp3_path,
                                output_dir=output_dir,
                                ckpt_dir=CKPT_DIR,
                                max_batch_size=max_batch_size,
                                device=device,
            )
            results_filename = os.path.splitext(os.path.basename(mp3_path))[0] + ".json"
            results_path = os.path.join(output_dir, results_filename)
            os.rename(results_path, panns_result_path)
        except Exception as e:
            print(f"Error during PANN inference: {e}")
            return False
    
    if not os.path.exists(panns_result_path):
        print(f"Logit file {panns_result_path} does not exist after PANN inference.")
        return False
    
    try:
        with open(panns_result_path) as f:
            logits = json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from {panns_result_path}: {e}")
        return False

    # Convert logits to binary
    try:
        binary = [logit["music_logit"] > MUSIC_LOGIT_THRESHOLD for logit in logits]
    except TypeError as e:
        print(f"TypeError: {e} in {panns_result_path}")
        return False

    # Group clips based on binary sequence
    music_onset_offset_list = []
    i = 0
    start, end = -1, -1
    for i in range(len(binary)):
        if binary[i]:
            if start == -1:
                start = logits[i]["onset"]
            end = logits[i]["offset"]
        else:
            if start != -1:
                music_onset_offset_list.append((start, end))
            start, end = -1, -1
    if start != -1:
        music_onset_offset_list.append((start, end))
    
    result_dict = {}
    result_dict['music_onset_offset_list'] = music_onset_offset_list
    # randomly select the part used
    candidate_clips = [clip_on_off for clip_on_off in music_onset_offset_list if clip_on_off[1] - clip_on_off[0] >= MAX_CLIP_SEC]
    if len(candidate_clips) > 0:
        selected_clip = candidate_clips[np.random.choice(len(candidate_clips))]
        start_sec = np.random.randint(selected_clip[0], selected_clip[1] - MAX_CLIP_SEC + 1)
        end_sec = start_sec + MAX_CLIP_SEC
        result_dict['selected_clip'] = [start_sec, end_sec]
    elif len(music_onset_offset_list) > 0:
        selected_clip = music_onset_offset_list[np.random.choice(len(music_onset_offset_list))]
        start_sec = max(0, selected_clip[0] - CLIP_PADDING_SEC)
        end_sec = selected_clip[1] + CLIP_PADDING_SEC
        result_dict['selected_clip'] = [start_sec, end_sec]
    else:
        result_dict['selected_clip'] = []
    
    clip_onset_offset_path = get_file_path(video_id)['music_on_off_info_json_path']
    with open(clip_onset_offset_path, "w") as f:
        json.dump(result_dict, f)
    
    return True

def main():
    video_id = '-uzbZiBwl6w'
    output_dir = '.'
    max_batch_size = 8
    get_clip_start_and_end(video_id, output_dir, max_batch_size=max_batch_size, device='cuda')

if __name__ == "__main__":
    main()