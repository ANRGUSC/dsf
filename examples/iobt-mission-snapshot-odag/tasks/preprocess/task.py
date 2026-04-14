#!/usr/bin/env python3
"""
IoBT Mission Snapshot — preprocess task (preprocess-1 .. preprocess-4).

Receives raw image bytes from capture-i, computes a sha256 digest over
64 KB chunks (simulating feature extraction), and produces a smaller
feature blob of DSF_DATA_SIZE bytes.
"""

import hashlib
import os
import time

from dsf_sdk import DSFTask

task = DSFTask()

DATA_SIZE = task.expected_data_size or 20_000_000  # default 20 MB
RUNTIME = task.expected_runtime or 5.0

print(f"[{task.name}] node={task.node}  data_size={DATA_SIZE}  runtime={RUNTIME}s", flush=True)

# --- receive raw from capture-i ---
t0 = time.perf_counter()
raw = task.recv_raw()
elapsed_recv = time.perf_counter() - t0

print(f"[{task.name}] recv_raw() -> {len(raw)} bytes in {elapsed_recv:.3f}s", flush=True)

# --- feature extraction: hash every 64 KB chunk ---
t1 = time.perf_counter()
CHUNK = 65536
hashes = []
for off in range(0, len(raw), CHUNK):
    h = hashlib.sha256(raw[off:off + CHUNK]).digest()  # 32 bytes each
    hashes.append(h)

digest_blob = b"".join(hashes)
print(f"[{task.name}] computed {len(hashes)} chunk hashes ({len(digest_blob)} bytes digest)", flush=True)

# --- sleep remaining runtime budget ---
elapsed_so_far = time.perf_counter() - t1
remaining = max(0, RUNTIME - elapsed_so_far)
if remaining > 0:
    time.sleep(remaining)

# --- produce feature blob ---
# Header: digest bytes, then pad with repeating digest to reach DATA_SIZE
if len(digest_blob) >= DATA_SIZE:
    features = digest_blob[:DATA_SIZE]
else:
    reps = DATA_SIZE // len(digest_blob) + 1
    features = (digest_blob * reps)[:DATA_SIZE]

elapsed_proc = time.perf_counter() - t1
print(f"[{task.name}] feature extraction done in {elapsed_proc:.3f}s  output={len(features)} bytes", flush=True)

# --- send to infer-i ---
t2 = time.perf_counter()
task.send_raw(features)
elapsed_send = time.perf_counter() - t2

print(f"[{task.name}] send_raw() completed in {elapsed_send:.3f}s", flush=True)
print(f"[{task.name}] done", flush=True)
task.close()
