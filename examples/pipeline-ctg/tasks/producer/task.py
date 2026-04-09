#!/usr/bin/env python3
"""
CTG pipeline — producer task.

Continuously generates numbered messages and publishes them to all
downstream subscribers via the DSF publish/subscribe API.

Runs indefinitely; the cdag-controller restarts it if it crashes.
"""

import time
import random
from dsf_sdk import DSFTask

task = DSFTask()

print(f"[{task.name}] starting producer loop (publish/subscribe)", flush=True)

counter = 0
report_every = 50

for counter in range(1, 2**63):
    msg = {
        "id": counter,
        "source": task.name,
        "timestamp": time.time(),
        "value": round(random.uniform(0, 100), 4),
    }

    task.publish(msg)

    if counter % report_every == 0:
        print(f"[{task.name}] published {counter} messages (last value: {msg['value']})", flush=True)

    time.sleep(0.1)  # ~10 messages/second
