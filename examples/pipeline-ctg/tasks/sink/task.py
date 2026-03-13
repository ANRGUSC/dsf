#!/usr/bin/env python3
"""
CTG pipeline — sink task.

Subscribes to the 'processor' task and logs each result.
Tracks throughput and running statistics.

Runs indefinitely.
"""

import time
from dsf_sdk import DSFTask

task = DSFTask()

print(f"[{task.name}] starting sink loop", flush=True)

received = 0
running_sum = 0.0
report_every = 50
window_start = time.time()
window_count = 0

while True:
    result = task.recv("processor")
    received += 1
    window_count += 1
    running_sum += result["output_value"]

    if received % report_every == 0:
        elapsed = time.time() - window_start
        rate = window_count / elapsed if elapsed > 0 else 0
        avg = running_sum / received
        print(
            f"[{task.name}] received {received} results | "
            f"rate: {rate:.1f} msg/s | "
            f"running avg output: {avg:.4f} | "
            f"last: msg#{result['id']} {result['input_value']:.4f} -> {result['output_value']:.4f}",
            flush=True,
        )
        window_start = time.time()
        window_count = 0
