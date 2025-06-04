import os
import json
import numpy as np

from vp.annotation.music_detection import extract_pann_logits
from vp.configs.constants import *

def get_clip_start_and_end(mp3_path, output_dir):
    if not os.path.exists(mp3_path):
        print(f'mp3_path {mp3_path} does not exist.')
        return
    
    # TODO(minhee): Handle file path in noble way... And avoid hardcoding
    clip_onset_offset_path = os.path.join(output_dir, os.path.splitext(os.path.basename(mp3_path))[0] + "_clip_info.json")
    if os.path.exists(clip_onset_offset_path):
        return
    
    # get music onset and offset using PANN
    logit_path = os.path.join(output_dir, os.path.splitext(os.path.basename(mp3_path))[0] + ".json")
    if not os.path.exists(logit_path):
        print(f"🔍 PANN 추론 시작: {mp3_path}")
        try:
            extract_pann_logits(audio_path=mp3_path,
                                output_dir=output_dir,
                                ckpt_dir=CKPT_DIR
            )
        except Exception as e:
            print(f"Error during PANN inference: {e}")
            return
    
    with open(logit_path) as f:
        logits = json.load(f)

    # Convert logits to binary
    binary = [logit["music_logit"] > MUSIC_LOGIT_THRESHOLD for logit in logits]

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
    
    # Add padding
    for i in range(len(music_onset_offset_list)):
        music_onset_offset_list[i] = (max(0, music_onset_offset_list[i][0] - CLIP_PADDING_SEC), music_onset_offset_list[i][1] + CLIP_PADDING_SEC)
    
    # Split into clips if longer than MAX_CLIP_SEC
    clip_onset_offset_list = []
    for start, end in music_onset_offset_list:
        duration = end - start
        if duration > MAX_CLIP_SEC:
            num_clips = int(np.ceil(duration / MAX_CLIP_SEC))
            for j in range(num_clips):
                clip_start = start + j * MAX_CLIP_SEC
                clip_end = min(end, clip_start + MAX_CLIP_SEC)
                clip_onset_offset_list.append((clip_start, clip_end))
        else:
            clip_onset_offset_list.append((start, end))
            
    with open(clip_onset_offset_path, "w") as f:
        json.dump(clip_onset_offset_list, f)
        
    return clip_onset_offset_list
