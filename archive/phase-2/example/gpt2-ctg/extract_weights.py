#!/usr/bin/env python3
"""
Build-time script: load full GPT-2 once, partition into per-role .pt files.
Total weight RAM across all pods will equal the model size (~475MB), zero duplication.
Run during docker build; then remove /app/model_cache.
"""

import os

import torch
from transformers import GPT2LMHeadModel, GPT2Tokenizer

MODEL_CACHE = "/app/model_cache"
WEIGHTS_DIR = "/app/weights"
NUM_LAYERS = 12
SHARD_COUNT = 12


def main():
    os.makedirs(WEIGHTS_DIR, exist_ok=True)
    config_dir = os.path.join(WEIGHTS_DIR, "config")
    os.makedirs(config_dir, exist_ok=True)

    print("[extract_weights] Loading full GPT-2...")
    model = GPT2LMHeadModel.from_pretrained("gpt2", cache_dir=MODEL_CACHE)
    tokenizer = GPT2Tokenizer.from_pretrained("gpt2", cache_dir=MODEL_CACHE)
    model.eval()

    # Save config and tokenizer for all roles
    model.save_pretrained(config_dir)
    tokenizer.save_pretrained(config_dir)
    print(f"[extract_weights] Saved config + tokenizer to {config_dir}")

    sd = model.state_dict()

    # --- Feeder: wte, wpe, ln_f (feeder does embed + decode; lm_head = wte.weight) ---
    feeder = {
        "wte.weight": sd["transformer.wte.weight"],
        "wpe.weight": sd["transformer.wpe.weight"],
        "ln_f.weight": sd["transformer.ln_f.weight"],
        "ln_f.bias": sd["transformer.ln_f.bias"],
    }
    torch.save(feeder, os.path.join(WEIGHTS_DIR, "feeder.pt"))
    print(f"[extract_weights] Saved feeder.pt ({sum(t.numel() * t.element_size() for t in feeder.values()) / 1024**2:.1f} MB)")

    # --- Per-layer: qkv, attn_merge, mlp_shard_0..11, mlp_merge ---
    for L in range(NUM_LAYERS):
        layer_dir = os.path.join(WEIGHTS_DIR, f"layer_{L}")
        os.makedirs(layer_dir, exist_ok=True)

        # qkv: ln_1 + c_attn
        qkv = {
            "ln_1.weight": sd[f"transformer.h.{L}.ln_1.weight"],
            "ln_1.bias": sd[f"transformer.h.{L}.ln_1.bias"],
            "c_attn.weight": sd[f"transformer.h.{L}.attn.c_attn.weight"],
            "c_attn.bias": sd[f"transformer.h.{L}.attn.c_attn.bias"],
        }
        torch.save(qkv, os.path.join(layer_dir, "qkv.pt"))

        # attn_merge: c_proj + ln_2
        attn_merge = {
            "c_proj.weight": sd[f"transformer.h.{L}.attn.c_proj.weight"],
            "c_proj.bias": sd[f"transformer.h.{L}.attn.c_proj.bias"],
            "ln_2.weight": sd[f"transformer.h.{L}.ln_2.weight"],
            "ln_2.bias": sd[f"transformer.h.{L}.ln_2.bias"],
        }
        torch.save(attn_merge, os.path.join(layer_dir, "attn_merge.pt"))

        # mlp c_fc: full weight [nx, nf] = [768, 3072], bias [3072]
        c_fc_w = sd[f"transformer.h.{L}.mlp.c_fc.weight"]
        c_fc_b = sd[f"transformer.h.{L}.mlp.c_fc.bias"]
        nf = c_fc_w.shape[1]
        chunk = nf // SHARD_COUNT

        for S in range(SHARD_COUNT):
            start, end = S * chunk, (S + 1) * chunk
            mlp_shard = {
                "weight": c_fc_w[:, start:end].clone(),
                "bias": c_fc_b[start:end].clone(),
            }
            torch.save(mlp_shard, os.path.join(layer_dir, f"mlp_shard_{S}.pt"))

        # mlp_merge: c_proj
        mlp_merge = {
            "c_proj.weight": sd[f"transformer.h.{L}.mlp.c_proj.weight"],
            "c_proj.bias": sd[f"transformer.h.{L}.mlp.c_proj.bias"],
        }
        torch.save(mlp_merge, os.path.join(layer_dir, "mlp_merge.pt"))

    print(f"[extract_weights] Saved layer_0..{NUM_LAYERS-1} (qkv, attn_merge, mlp_shard_0..{SHARD_COUNT-1}, mlp_merge)")
    print("[extract_weights] Done. Remove /app/model_cache to avoid duplicating weights in image.")


if __name__ == "__main__":
    main()
