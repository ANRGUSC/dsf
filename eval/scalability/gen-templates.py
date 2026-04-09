#!/usr/bin/env python3
"""
Generate CDAGTemplate YAML files for scalability evaluation.

Creates templates with varying fan-out widths (N workers) for both
ZMQ (DSF native P2P) and MQTT (centralized broker) transports.

Topology:  source → worker-1..N → sink

Usage: python3 eval/scalability/gen-templates.py
"""

import os
import yaml

NAMESPACE = "dsf-system"
IMAGE = "192.168.1.163:5000/scalability-eval:latest"
NODES = ["anrg-3", "anrg-4", "anrg-5", "anrg-6"]
WORKER_COUNTS = [2, 4, 6, 8]
MSG_SIZES = [102400]  # 100KB default; can add more
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "templates")


def gen_template(n_workers: int, transport: str, msg_size: int) -> dict:
    """Generate a CDAGTemplate dict for n_workers with given transport."""
    name = f"scale-{transport}-w{n_workers}"
    if msg_size != 102400:
        name += f"-{msg_size // 1024}kb"

    transport_pattern = "pubsub" if transport == "zmq" else "mqtt"

    tasks = []

    # Source task.
    tasks.append({
        "name": "source",
        "image": IMAGE,
        "command": ["python", "task.py"],
        "replicas": 1,
        "dependencies": [],
        "dataRate": f"{msg_size * 5}B/s",  # MSG_RATE=5/s
        "env": [
            {"name": "DSF_EVAL_MSG_SIZE", "value": str(msg_size)},
            {"name": "DSF_EVAL_MSG_RATE", "value": "5"},
            {"name": "DSF_TRANSPORT_PATTERN", "value": transport_pattern},
        ],
        "resources": {"cpu": "200m", "memory": "128Mi"},
        "constraints": {"nodeNames": [NODES[0]]},
    })

    # Worker tasks (spread across nodes round-robin).
    worker_deps = []
    for i in range(1, n_workers + 1):
        worker_name = f"worker-{i}"
        worker_deps.append(worker_name)
        node = NODES[i % len(NODES)]
        tasks.append({
            "name": worker_name,
            "image": IMAGE,
            "command": ["python", "task.py"],
            "replicas": 1,
            "dependencies": ["source"],
            "dataRate": f"{msg_size * 5}B/s",
            "env": [
                {"name": "DSF_EVAL_MSG_SIZE", "value": str(msg_size)},
                {"name": "DSF_TRANSPORT_PATTERN", "value": transport_pattern},
            ],
            "resources": {"cpu": "200m", "memory": "128Mi"},
            "constraints": {"nodeNames": NODES},
        })

    # Sink task.
    tasks.append({
        "name": "sink",
        "image": IMAGE,
        "command": ["python", "task.py"],
        "replicas": 1,
        "dependencies": worker_deps,
        "env": [
            {"name": "DSF_TRANSPORT_PATTERN", "value": transport_pattern},
        ],
        "resources": {"cpu": "300m", "memory": "256Mi"},
        "constraints": {"nodeNames": NODES},
    })

    return {
        "apiVersion": "dsf.io/v1",
        "kind": "CDAGTemplate",
        "metadata": {"name": name, "namespace": NAMESPACE},
        "spec": {
            "description": f"Scalability eval: {transport.upper()} transport, {n_workers} workers, {msg_size // 1024}KB msgs",
            "scheduler": "random",
            "restartPolicy": "Always",
            "retention": {"maxInstances": 5},
            "tasks": tasks,
        },
    }


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    for transport in ["zmq", "mqtt"]:
        for n_workers in WORKER_COUNTS:
            for msg_size in MSG_SIZES:
                tmpl = gen_template(n_workers, transport, msg_size)
                name = tmpl["metadata"]["name"]
                path = os.path.join(OUTPUT_DIR, f"{name}.yml")

                with open(path, "w") as f:
                    yaml.dump(tmpl, f, default_flow_style=False, sort_keys=False)

                print(f"  Generated: {path}")

    print(f"\n  Total: {len(WORKER_COUNTS) * 2 * len(MSG_SIZES)} templates")
    print(f"  Apply all: kubectl apply -f {OUTPUT_DIR}/")


if __name__ == "__main__":
    print("=== Generating scalability evaluation templates ===")
    main()
