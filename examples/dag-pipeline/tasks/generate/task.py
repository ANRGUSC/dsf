#!/usr/bin/env python3
"""
DAG pipeline — generate task.

Simulates 10 seconds of computation, then sends a dataset to the
'transform' task via ZMQ PUSH (DSF SDK send/recv over pushpull transport).

All pods in the pipeline start simultaneously. Downstream tasks block
on their PULL socket until this task pushes data.
"""

import time
import random
from dsf_sdk import DSFTask

task = DSFTask()

print(f"[{task.name}] starting — simulating 10s of work", flush=True)
time.sleep(10)

# Generate a dataset: list of 100 random numbers.
dataset = {
    "source": task.name,
    "timestamp": time.time(),
    "values": [round(random.uniform(0, 100), 4) for _ in range(100)],
    "count": 100,
}

print(f"[{task.name}] generated dataset with {dataset['count']} values", flush=True)
print(f"[{task.name}] sample: {dataset['values'][:5]}", flush=True)

print(f"[{task.name}] sending dataset to 'transform'", flush=True)
task.send("transform", dataset)

print(f"[{task.name}] done", flush=True)
task.close()
