#!/usr/bin/env python3
"""
DAG pipeline — transform task.

Waits for data from 'generate' (blocking PULL), doubles every value
to simulate 10 seconds of work, then sends the result to 'output'.
"""

import time
from dsf_sdk import DSFTask

task = DSFTask()

print(f"[{task.name}] waiting for data from 'generate'", flush=True)
dataset = task.recv("generate")
print(f"[{task.name}] received {dataset['count']} values from '{dataset['source']}'", flush=True)

print(f"[{task.name}] transforming — simulating 10s of work", flush=True)
time.sleep(10)

transformed = {
    "source": task.name,
    "original_source": dataset["source"],
    "timestamp": time.time(),
    "values": [round(v * 2, 4) for v in dataset["values"]],
    "count": dataset["count"],
    "operation": "multiply_by_2",
}

print(f"[{task.name}] transformed {transformed['count']} values", flush=True)
print(f"[{task.name}] sample: {transformed['values'][:5]}", flush=True)

print(f"[{task.name}] sending result to 'output'", flush=True)
task.send("output", transformed)

print(f"[{task.name}] done", flush=True)
task.close()
