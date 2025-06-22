import os
import json
import torch
import argparse
import librosa
import julius
import numpy as np

from vp.annotation.modules.panns import MUSIC_INDEX
from vp.configs.constants import PANN_CLIP_DURATION_SEC, MUSIC_LOGIT_THRESHOLD

def convert_audio(wav, original_rate, target_rate, max_batch_size):
    if original_rate != target_rate:
        wav = julius.resample_frac(wav, original_rate, target_rate)
    # Split audio into chunks of PANN_CLIP_DURATION_SEC
    chunk_size = int(PANN_CLIP_DURATION_SEC * target_rate)
    total_chunks = len(wav) // chunk_size
    if max_batch_size is None or max_batch_size <= 0:
        max_batch_size = total_chunks if total_chunks > 0 else 1

    for batch_idx in range(0, total_chunks, max_batch_size):
        chunks = []
        for chunk_idx in range(batch_idx, min(batch_idx + max_batch_size, total_chunks)):
            start = chunk_idx * chunk_size
            end = start + chunk_size
            chunk = wav[start:end]
            if len(chunk) == chunk_size:
                chunks.append(chunk)
        if chunks:
            yield np.stack(chunks)

def extract_bendit_logits():
    pass

def extract_pann_logits(audio_path, output_dir, ckpt_dir, device="cuda", sample_rate=32000, model=None, max_batch_size=None):
    from vp.annotation.modules.panns import Cnn14

    # Use a static variable to cache the loaded model
    if not hasattr(extract_pann_logits, "_static_model") or model is not None:
        model_path = os.path.join(ckpt_dir, "Cnn14_mAP=0.431.pth")
        if not os.path.exists(model_path):
            torch.hub.download_url_to_file(
                url='https://zenodo.org/record/3987831/files/Cnn14_mAP=0.431.pth',
                dst=model_path
            )
        if model is None:
            model = Cnn14(
                sample_rate=sample_rate,
                window_size=1024,
                hop_size=320,
                mel_bins=64,
                fmin=50,
                fmax=16000,
                classes_num=527
            )
            checkpoint = torch.load(model_path, map_location=device)
            model.load_state_dict(checkpoint['model'])
            model.eval()
            model.to(device)
        extract_pann_logits._static_model = model
    else:
        model = extract_pann_logits._static_model

    cur_audio, input_sr = librosa.load(audio_path, mono=True, sr=None, res_type='kaiser_fast')
    
    results = []
    for cur_converted_audio in convert_audio(wav=torch.from_numpy(cur_audio), original_rate=input_sr, target_rate=sample_rate, max_batch_size=max_batch_size):
        # model inference
        print(cur_converted_audio.shape)
        with torch.no_grad():
            cur_converted_audio = torch.tensor(cur_converted_audio, dtype=torch.float32, device=device)
            out = model(cur_converted_audio, None)
        music_logits = out["clipwise_output"][:, MUSIC_INDEX]
        
        found_music = False
        for idx, logit in enumerate(music_logits):
            results.append({
                "onset": idx * PANN_CLIP_DURATION_SEC,
                "offset": (idx + 1) * PANN_CLIP_DURATION_SEC,
                "music_logit": float(logit)
            })
            if logit > MUSIC_LOGIT_THRESHOLD:
                found_music = True
                # not break here, to collect all the results we got so far
        if found_music:
            print(f"Music detected in {audio_path}")
            break
    results_path = os.path.splitext(os.path.basename(audio_path))[0] + ".json"
    with open(os.path.join(output_dir, results_path), "w") as f:
        json.dump(results, f)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--audio_path", type=str, default="data/audio/18500.mp3")
    parser.add_argument("--audio_dir", type=str, default="data/audio")
    parser.add_argument("--output_dir", type=str, default="data/annotation/music_detection")
    parser.add_argument("--ckpt_dir", type=str, default="ckpt")
    parser.add_argument("--device", type=str, default="cuda")
    parser.add_argument("--sample_rate", type=int, default=32000)
    args = parser.parse_args()
    os.makedirs(args.ckpt_dir, exist_ok=True)
    extract_pann_logits(args.audio_path, args.output_dir, args.ckpt_dir, args.device, args.sample_rate)


if __name__ == "__main__":
    main()
