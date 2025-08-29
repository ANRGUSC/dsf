#!/usr/bin/env python3
import time
import json
import subprocess
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def measure_latency(dst_ip):
    """Ping dst_ip 3 times and return average RTT in ms."""
    logger.debug(f"Measuring latency to {dst_ip}")
    try:
        out = subprocess.check_output(
            ["ping", "-c", "3", "-W", "1", dst_ip],
            stderr=subprocess.DEVNULL
        ).decode()
        stats = out.splitlines()[-1].split('/')[4]
        return float(stats)
    except Exception:
        logger.warning(f"Failed to measure latency to {dst_ip}")
        return None

def measure_bandwidth(dst_ip, duration=5):
    """Run iperf3 client in JSON mode and return Mbps throughput."""
    logger.debug(f"Measuring bandwidth to {dst_ip}")
    try:
        cmd = ["iperf3", "-c", dst_ip, "-t", str(duration), "-J"]
        raw = subprocess.check_output(cmd, stderr=subprocess.DEVNULL).decode()
        js = json.loads(raw)
        end = js.get("end", {})
        summary = end.get("sum_received") or end.get("sum_sent")
        if summary and "bits_per_second" in summary:
            return summary["bits_per_second"] / 1e6
        intervals = js.get("intervals", [])
        if intervals and "sum" in intervals[-1]:
            last = intervals[-1]["sum"]
            if "bits_per_second" in last:
                return last["bits_per_second"] / 1e6
    except Exception:
        logger.warning(f"Failed to measure bandwidth to {dst_ip}")
    return None

def collect_and_publish():
    """Collect metrics for the single configured IP."""
    logger.info("Starting metrics collection cycle")

    target_ip = os.getenv("TARGET_IP")
    if not target_ip:
        logger.error("TARGET_IP environment variable not set")
        raise RuntimeError("TARGET_IP not set")

    logger.info(f"Probing target IP: {target_ip}")
    current_time = time.time()

    lat = measure_latency(target_ip)
    bw = measure_bandwidth(target_ip)

    row = {
        target_ip: {
            "lat": {
                "value": lat,
                "unit": "ms",
                "last_updated": current_time,
                "last_inquired": current_time
            } if lat is not None else None,
            "bw": {
                "value": bw,
                "unit": "Mbps",
                "last_updated": current_time,
                "last_inquired": current_time
            } if bw is not None else None
        }
    }

    logger.info("Collected metrics data:")
    logger.info(json.dumps(row, indent=2))

if __name__ == "__main__":
    logger.info("Network metrics collection service starting")
    while True:
        collect_and_publish()
        logger.debug("Sleeping for 10 seconds")
        time.sleep(10)
