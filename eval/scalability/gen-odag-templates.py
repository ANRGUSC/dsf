#!/usr/bin/env python3
"""
Generate ODAGTemplate YAML files for scalability evaluation.

Creates templates with varying fan-out widths (N workers) for both
P2P (file transport / data-agent) and NFS (shared_volume transport).

Topology:  source → worker-1..N → sink

Usage: python3 eval/scalability/gen-odag-templates.py
"""

import os
import yaml

NAMESPACE = "dsf-system"
IMAGE = "192.168.1.163:5000/scalability-eval:latest"
NODES = ["anrg-3", "anrg-4", "anrg-5", "anrg-6"]
WORKER_COUNTS = [2, 4, 6, 8]
DATA_SIZE = 50_000_000  # 50MB
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "odag-templates")


def gen_template(n_workers: int, transport: str) -> dict:
    """Generate an ODAGTemplate for n_workers with given transport."""
    name = f"scale-odag-{transport}-w{n_workers}"

    # For NFS, override transport pattern via task env vars.
    nfs_env = []
    if transport == "nfs":
        nfs_env = [
            {"name": "DSF_TRANSPORT_PATTERN", "value": "shared_volume"},
            {"name": "DSF_SHARED_DIR", "value": "/shared/dsf-outputs"},
        ]

    tasks = []

    # Source task — pinned to anrg-3.
    tasks.append({
        "name": "source",
        "image": IMAGE,
        "command": ["python", "odag-task.py"],
        "dependencies": [],
        "dataSize": f"{DATA_SIZE}",
        "runtime": 2,
        "env": [{"name": "DSF_EVAL_DATA_SIZE", "value": str(DATA_SIZE)}] + nfs_env,
        "resources": {"cpu": "300m", "memory": "256Mi"},
        "constraints": {"nodeNames": [NODES[0]]},
    })

    # Worker tasks — spread across nodes round-robin.
    worker_names = []
    for i in range(1, n_workers + 1):
        wname = f"worker-{i}"
        worker_names.append(wname)
        node = NODES[i % len(NODES)]
        tasks.append({
            "name": wname,
            "image": IMAGE,
            "command": ["python", "odag-task.py"],
            "dependencies": ["source"],
            "dataSize": f"{DATA_SIZE}",
            "runtime": 2,
            "env": [{"name": "DSF_EVAL_DATA_SIZE", "value": str(DATA_SIZE)}] + nfs_env,
            "resources": {"cpu": "300m", "memory": "256Mi"},
            "constraints": {"nodeNames": NODES},
        })

    # Sink task.
    tasks.append({
        "name": "sink",
        "image": IMAGE,
        "command": ["python", "odag-task.py"],
        "dependencies": worker_names,
        "dataSize": "0",
        "runtime": 1,
        "env": nfs_env,
        "resources": {"cpu": "300m", "memory": "512Mi"},
        "constraints": {"nodeNames": NODES},
    })

    return {
        "apiVersion": "dsf.io/v1",
        "kind": "ODAGTemplate",
        "metadata": {"name": name, "namespace": NAMESPACE},
        "spec": {
            "description": f"Scalability eval: {transport.upper()} transport, {n_workers} workers, {DATA_SIZE // 1_000_000}MB per task",
            "scheduler": "random",
            "profiling": {"enabled": False},
            "defaults": {"runtime": 2, "dataSize": f"{DATA_SIZE}"},
            "retention": {"maxRuns": 10},
            "tasks": tasks,
        },
    }


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    for transport in ["p2p", "nfs"]:
        for n_workers in WORKER_COUNTS:
            tmpl = gen_template(n_workers, transport)
            name = tmpl["metadata"]["name"]
            path = os.path.join(OUTPUT_DIR, f"{name}.yml")

            with open(path, "w") as f:
                yaml.dump(tmpl, f, default_flow_style=False, sort_keys=False)

            print(f"  Generated: {path}")

    print(f"\n  Total: {len(WORKER_COUNTS) * 2} templates")
    print(f"  Apply all: kubectl apply -f {OUTPUT_DIR}/")


if __name__ == "__main__":
    print("=== Generating ODAG scalability templates ===")
    main()
