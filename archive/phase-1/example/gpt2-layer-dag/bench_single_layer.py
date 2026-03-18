#!/usr/bin/env python3
"""
Baseline benchmark: run the same single GPT-2 layer (non-dagized) that the
27-node DAG executes, and measure wall-clock time.

Uses identical config, seed, and input so results are directly comparable.
"""

import time

t_start = time.time()

import torch
from transformers import GPT2Config, GPT2LMHeadModel

t_imports = time.time()

SEED = 42
BATCH, SEQLEN = 1, 8

CONFIG = GPT2Config(
    n_layer=1,
    n_embd=768,
    n_head=12,
    n_inner=3072,
    vocab_size=50257,
    n_positions=1024,
)

torch.manual_seed(SEED)
model = GPT2LMHeadModel(CONFIG)
model.eval()
t_model = time.time()

torch.manual_seed(SEED + 1)
input_ids = torch.randint(0, CONFIG.vocab_size, (BATCH, SEQLEN))

with torch.no_grad():
    t_compute_start = time.time()
    outputs = model(input_ids)
    t_compute_end = time.time()

hidden = outputs.logits
t_end = time.time()

print(f"Output shape: {hidden.shape}")
print()
print(f"PROFILE single-layer-baseline |"
      f" import={t_imports - t_start:.3f}s"
      f" model_load={t_model - t_imports:.3f}s"
      f" compute={t_compute_end - t_compute_start:.3f}s"
      f" total={t_end - t_start:.3f}s")
