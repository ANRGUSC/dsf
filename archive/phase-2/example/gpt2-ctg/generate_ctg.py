#!/usr/bin/env python3
"""Generate ctg.yml for the GPT-2 CTG DAG (configurable layer count)."""

import yaml

NUM_LAYERS = 6  # 6 = half of 12; set to 12 for full model
SHARD_COUNT = 12
GRAPH = "gpt2-ctg"
REPO = "mohammadalikh/gpt2-ctg"
VERSION = "v1"
IMAGE = f"{REPO}:{VERSION}"


def feeder_task():
    last = NUM_LAYERS - 1
    # Use tail so you can exec in and run: python feeder.py --warmup 30 --auto 2
    return {
        "name": "feeder",
        "image": IMAGE,
        "command": ["tail", "-f", "/dev/null"],
        "dependencies": [],
        "resources": {"requests": {"cpu": "100m", "memory": "256Mi"}},
        "replicas": 1,
        "zeromq": {
            "publishTopics": [f"{GRAPH}/feeder/input"],
            "subscribeTopics": [],
            "pattern": "PUB",
            "port": 5555,
            "transport": "tcp",
            "bind": True,
        },
    }


def qkv_task(L):
    name = f"l{L}-qkv"
    dep = "feeder" if L == 0 else f"l{L-1}-mlp-merge"
    sub_topic = (f"{GRAPH}/feeder/input" if L == 0
                 else f"{GRAPH}/l{L-1}-mlp-merge/output")
    return {
        "name": name,
        "image": IMAGE,
        "command": ["python", "layer_node_ctg.py"],
        "args": ["--role", "qkv", "--layer", str(L)],
        "dependencies": [dep],
        "resources": {"requests": {"cpu": "200m", "memory": "128Mi"}},
        "replicas": 1,
        "zeromq": {
            "publishTopics": [f"{GRAPH}/{name}/output"],
            "subscribeTopics": [sub_topic],
            "pattern": "PUB",
            "port": 5555,
            "transport": "tcp",
            "bind": True,
        },
    }


def attn_shard_task(L, S):
    name = f"l{L}-attn-shard-{S}"
    return {
        "name": name,
        "image": IMAGE,
        "command": ["python", "layer_node_ctg.py"],
        "args": ["--role", "attn-shard", "--layer", str(L), "--shard-idx", str(S)],
        "dependencies": [f"l{L}-qkv"],
        "resources": {"requests": {"cpu": "100m", "memory": "128Mi"}},
        "replicas": 1,
        "zeromq": {
            "publishTopics": [f"{GRAPH}/{name}/output"],
            "subscribeTopics": [f"{GRAPH}/l{L}-qkv/output"],
            "pattern": "PUB",
            "port": 5555,
            "transport": "tcp",
            "bind": True,
        },
    }


def attn_merge_task(L):
    name = f"l{L}-attn-merge"
    deps = [f"l{L}-qkv"] + [f"l{L}-attn-shard-{i}" for i in range(SHARD_COUNT)]
    subs = ([f"{GRAPH}/l{L}-qkv/output"]
            + [f"{GRAPH}/l{L}-attn-shard-{i}/output" for i in range(SHARD_COUNT)])
    return {
        "name": name,
        "image": IMAGE,
        "command": ["python", "layer_node_ctg.py"],
        "args": ["--role", "attn-merge", "--layer", str(L)],
        "dependencies": deps,
        "resources": {"requests": {"cpu": "200m", "memory": "128Mi"}},
        "replicas": 1,
        "zeromq": {
            "publishTopics": [f"{GRAPH}/{name}/output"],
            "subscribeTopics": subs,
            "pattern": "PUB",
            "port": 5555,
            "transport": "tcp",
            "bind": True,
        },
    }


def mlp_shard_task(L, S):
    name = f"l{L}-mlp-shard-{S}"
    return {
        "name": name,
        "image": IMAGE,
        "command": ["python", "layer_node_ctg.py"],
        "args": ["--role", "mlp-shard", "--layer", str(L), "--shard-idx", str(S)],
        "dependencies": [f"l{L}-attn-merge"],
        "resources": {"requests": {"cpu": "100m", "memory": "128Mi"}},
        "replicas": 1,
        "zeromq": {
            "publishTopics": [f"{GRAPH}/{name}/output"],
            "subscribeTopics": [f"{GRAPH}/l{L}-attn-merge/output"],
            "pattern": "PUB",
            "port": 5555,
            "transport": "tcp",
            "bind": True,
        },
    }


def mlp_merge_task(L):
    name = f"l{L}-mlp-merge"
    deps = [f"l{L}-attn-merge"] + [f"l{L}-mlp-shard-{i}" for i in range(SHARD_COUNT)]
    subs = ([f"{GRAPH}/l{L}-attn-merge/output"]
            + [f"{GRAPH}/l{L}-mlp-shard-{i}/output" for i in range(SHARD_COUNT)])
    return {
        "name": name,
        "image": IMAGE,
        "command": ["python", "layer_node_ctg.py"],
        "args": ["--role", "mlp-merge", "--layer", str(L)],
        "dependencies": deps,
        "resources": {"requests": {"cpu": "200m", "memory": "128Mi"}},
        "replicas": 1,
        "zeromq": {
            "publishTopics": [f"{GRAPH}/{name}/output"],
            "subscribeTopics": subs,
            "pattern": "PUB",
            "port": 5555,
            "transport": "tcp",
            "bind": True,
        },
    }


def generate():
    tasks = [feeder_task()]

    for L in range(NUM_LAYERS):
        tasks.append(qkv_task(L))
        for S in range(SHARD_COUNT):
            tasks.append(attn_shard_task(L, S))
        tasks.append(attn_merge_task(L))
        for S in range(SHARD_COUNT):
            tasks.append(mlp_shard_task(L, S))
        tasks.append(mlp_merge_task(L))

    doc = {
        "apiVersion": "dsf.example.com/v1",
        "kind": "ContinuousTaskGraph",
        "metadata": {"name": GRAPH, "namespace": "default"},
        "spec": {
            "zeromq": {
                "discovery": {
                    "method": "kubernetes",
                    "namespace": "default",
                    "serviceNamePattern": "{task-name}-service",
                },
                "topicPrefix": GRAPH,
                "defaultPort": 5555,
                "defaultTransport": "tcp",
                "connectionTimeout": 5000,
                "reconnectInterval": 1000,
                "maxReconnectAttempts": 10,
            },
            "scheduler": {"name": "ctg-scheduler", "optimizeFor": "latency"},
            "migration": {"enabled": False},
            "namespace": "default",
            "tasks": tasks,
        },
    }
    return doc


if __name__ == "__main__":
    import pathlib, sys

    out = pathlib.Path(__file__).parent / "ctg.yml"
    doc = generate()
    with open(out, "w") as f:
        yaml.dump(doc, f, default_flow_style=False, sort_keys=False, width=200)

    task_count = len(doc["spec"]["tasks"])
    print(f"Generated {out} with {task_count} tasks")
    print(f"  1 feeder + {NUM_LAYERS} layers × "
          f"(1 qkv + {SHARD_COUNT} attn-shards + 1 attn-merge + "
          f"{SHARD_COUNT} mlp-shards + 1 mlp-merge)")
