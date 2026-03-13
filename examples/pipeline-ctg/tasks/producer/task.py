#!/usr/bin/env python3
"""
CTG pipeline — producer task.

Continuously generates numbered messages and publishes them to the
'processor' task via ZMQ PUB (DSF SDK pubsub transport).

Runs indefinitely; the ctg-controller restarts it if it crashes.
"""

import time
import random
from dsf_sdk import DSFTask

task = DSFTask()

print(f"[{task.name}] starting producer loop", flush=True)

counter = 0
report_every = 50

while True:
    counter += 1
    msg = {
        "id": counter,
        "source": task.name,
        "timestamp": time.time(),
        "value": round(random.uniform(0, 100), 4),
    }

    task.send("processor", msg)

    if counter % report_every == 0:
        print(f"[{task.name}] published {counter} messages (last value: {msg['value']})", flush=True)

    time.sleep(0.1)  # ~10 messages/second
