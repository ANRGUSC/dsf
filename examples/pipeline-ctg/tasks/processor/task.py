#!/usr/bin/env python3
"""
CTG pipeline — processor task.

Subscribes to the 'producer' task using the iterator API, applies a
transformation (squares the value), and publishes results downstream.

Runs indefinitely.
"""

import time
from dsf_sdk import DSFTask

task = DSFTask()

print(f"[{task.name}] starting processor loop (subscribe iterator)", flush=True)

processed = 0
report_every = 50

for msg in task.subscribe("producer"):
    processed += 1

    result = {
        "id": msg["id"],
        "source": task.name,
        "original_source": msg["source"],
        "timestamp": time.time(),
        "input_value": msg["value"],
        "output_value": round(msg["value"] ** 2, 4),
        "operation": "square",
    }

    task.publish(result)

    if processed % report_every == 0:
        print(
            f"[{task.name}] processed {processed} messages "
            f"(msg #{msg['id']}: {msg['value']:.4f} -> {result['output_value']:.4f})",
            flush=True,
        )
