#!/usr/bin/env python3
"""
Plot Experiment 2: Scalability — P2P (ZMQ) vs Centralized (MQTT).

Generates:
1. Throughput vs fan-out width (line chart, ZMQ vs MQTT)
2. Latency vs fan-out width (line chart, ZMQ vs MQTT)

Usage: python3 eval/scalability/plot-scalability.py
"""

import os
import csv
import sys
import numpy as np
from collections import defaultdict

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError:
    print("matplotlib not installed. Install with: pip install matplotlib")
    sys.exit(1)

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
FIGURES_DIR = os.path.join(os.path.dirname(__file__), "figures")
os.makedirs(FIGURES_DIR, exist_ok=True)

plt.rcParams.update({
    "font.size": 12,
    "font.family": "serif",
    "axes.grid": True,
    "grid.alpha": 0.3,
})


def load_data():
    csv_path = os.path.join(RESULTS_DIR, "scalability.csv")
    if not os.path.exists(csv_path):
        print(f"  CSV not found: {csv_path}")
        return None

    # Group by (transport, workers) → lists of metrics.
    data = defaultdict(lambda: {"throughput": [], "avg_latency": [], "p50": [], "p95": [], "p99": []})
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (row["transport"], int(row["workers"]))
            if float(row["throughput"]) > 0:
                data[key]["throughput"].append(float(row["throughput"]))
                data[key]["avg_latency"].append(float(row["avg_latency"]))
                data[key]["p50"].append(float(row["p50_latency"]))
                data[key]["p95"].append(float(row["p95_latency"]))
                data[key]["p99"].append(float(row["p99_latency"]))
    return data


def plot_throughput(data):
    """Throughput vs fan-out width."""
    fig, ax = plt.subplots(figsize=(6, 4))

    for transport, color, marker, label in [
        ("zmq", "#3b82f6", "o", "DSF (ZMQ P2P)"),
        ("mqtt", "#ef4444", "s", "MQTT Broker"),
    ]:
        workers = sorted(set(w for t, w in data.keys() if t == transport))
        means = [np.mean(data[(transport, w)]["throughput"]) for w in workers]
        stds = [np.std(data[(transport, w)]["throughput"]) for w in workers]

        ax.errorbar(workers, means, yerr=stds, marker=marker, color=color,
                     linewidth=2, markersize=8, capsize=5, label=label)

    ax.set_xlabel("Number of Workers (fan-out width)")
    ax.set_ylabel("Throughput at Sink (msg/s)")
    ax.set_title("Scalability: Throughput vs Fan-Out Width")
    ax.legend()
    ax.set_xticks(sorted(set(w for _, w in data.keys())))

    plt.tight_layout()
    out = os.path.join(FIGURES_DIR, "scalability-throughput.png")
    plt.savefig(out, dpi=200)
    print(f"  Saved: {out}")
    plt.close()


def plot_latency(data):
    """Latency (p50, p95) vs fan-out width."""
    fig, ax = plt.subplots(figsize=(6, 4))

    for transport, color, marker, label in [
        ("zmq", "#3b82f6", "o", "DSF (ZMQ P2P)"),
        ("mqtt", "#ef4444", "s", "MQTT Broker"),
    ]:
        workers = sorted(set(w for t, w in data.keys() if t == transport))

        p50_means = [np.mean(data[(transport, w)]["p50"]) * 1000 for w in workers]
        p95_means = [np.mean(data[(transport, w)]["p95"]) * 1000 for w in workers]

        ax.plot(workers, p50_means, marker=marker, color=color, linewidth=2,
                markersize=8, label=f"{label} (p50)", linestyle="-")
        ax.plot(workers, p95_means, marker=marker, color=color, linewidth=1.5,
                markersize=6, label=f"{label} (p95)", linestyle="--", alpha=0.7)

    ax.set_xlabel("Number of Workers (fan-out width)")
    ax.set_ylabel("End-to-End Latency (ms)")
    ax.set_title("Scalability: Latency vs Fan-Out Width")
    ax.legend(fontsize=9)
    ax.set_xticks(sorted(set(w for _, w in data.keys())))

    plt.tight_layout()
    out = os.path.join(FIGURES_DIR, "scalability-latency.png")
    plt.savefig(out, dpi=200)
    print(f"  Saved: {out}")
    plt.close()


def plot_combined(data):
    """Side-by-side throughput + latency."""
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.5))

    for transport, color, marker, label in [
        ("zmq", "#3b82f6", "o", "DSF (ZMQ P2P)"),
        ("mqtt", "#ef4444", "s", "MQTT Broker"),
    ]:
        workers = sorted(set(w for t, w in data.keys() if t == transport))

        # Throughput
        tp_means = [np.mean(data[(transport, w)]["throughput"]) for w in workers]
        tp_stds = [np.std(data[(transport, w)]["throughput"]) for w in workers]
        axes[0].errorbar(workers, tp_means, yerr=tp_stds, marker=marker, color=color,
                          linewidth=2, markersize=8, capsize=5, label=label)

        # Latency (p50)
        lat_means = [np.mean(data[(transport, w)]["p50"]) * 1000 for w in workers]
        lat_stds = [np.std(data[(transport, w)]["p50"]) * 1000 for w in workers]
        axes[1].errorbar(workers, lat_means, yerr=lat_stds, marker=marker, color=color,
                          linewidth=2, markersize=8, capsize=5, label=label)

    xticks = sorted(set(w for _, w in data.keys()))
    axes[0].set_xlabel("Workers")
    axes[0].set_ylabel("Throughput (msg/s)")
    axes[0].set_title("(a) Throughput")
    axes[0].legend()
    axes[0].set_xticks(xticks)

    axes[1].set_xlabel("Workers")
    axes[1].set_ylabel("Latency p50 (ms)")
    axes[1].set_title("(b) Latency")
    axes[1].legend()
    axes[1].set_xticks(xticks)

    plt.tight_layout()
    out = os.path.join(FIGURES_DIR, "scalability-combined.png")
    plt.savefig(out, dpi=200)
    print(f"  Saved: {out}")
    plt.close()


if __name__ == "__main__":
    print("=== Experiment 2: Scalability Plots ===")
    data = load_data()
    if data:
        plot_throughput(data)
        plot_latency(data)
        plot_combined(data)
    print("Done.")
