#!/usr/bin/env python3
import os
import sys
import time
import zmq
import json

# Use line buffering for real-time logging
if sys.stdout.isatty():
    sys.stdout.reconfigure(line_buffering=True)
if sys.stderr.isatty():
    sys.stderr.reconfigure(line_buffering=True)

# Get configuration from environment
graph_name = os.getenv("ZMQ_GRAPH_NAME", "test")
task_name = os.getenv("ZMQ_TASK_NAME", "data-source")
topic_prefix = os.getenv("ZMQ_TOPIC_PREFIX", graph_name)
port = int(os.getenv("ZMQ_PORT", "5555"))
transport = os.getenv("ZMQ_TRANSPORT", "tcp")
pattern = os.getenv("ZMQ_PATTERN", "PUB")
bind = os.getenv("ZMQ_BIND", "true").lower() == "true"
publish_topics = os.getenv("ZMQ_PUBLISH_TOPICS", "").split(",") if os.getenv("ZMQ_PUBLISH_TOPICS") else []

print(f"[{task_name}] Starting ZeroMQ {pattern} publisher")
print(f"[{task_name}] Port: {port}, Transport: {transport}, Bind: {bind}")
print(f"[{task_name}] Publish topics: {publish_topics}")

# Create ZeroMQ context and socket
context = zmq.Context()
socket = context.socket(getattr(zmq, pattern))

# Configure for high throughput - don't block on send
# Set high water mark to unlimited (0) or very high to prevent blocking
socket.setsockopt(zmq.SNDHWM, 0)  # 0 = unlimited send queue
socket.setsockopt(zmq.SNDBUF, 10 * 1024 * 1024)  # 10MB send buffer
if pattern == "PUB":
    socket.setsockopt(zmq.XPUB_VERBOSE, 1)  # Enable verbose mode for PUB

# Build address
if transport == "tcp":
    if bind:
        address = f"tcp://*:{port}"
    else:
        # For connect, we need service discovery
        service_name = os.getenv("ZMQ_SERVICE_NAME", f"{graph_name}-{task_name}-service")
        namespace = os.getenv("ZMQ_NAMESPACE", "default")
        address = f"tcp://{service_name}.{namespace}.svc.cluster.local:{port}"
elif transport == "ipc":
    address = f"ipc:///tmp/{task_name}.ipc"
else:
    address = f"inproc://{task_name}"

print(f"[{task_name}] Connecting to: {address}")

if bind:
    socket.bind(address)
    print(f"[{task_name}] Bound to {address}")
else:
    socket.connect(address)
    print(f"[{task_name}] Connected to {address}")

# Wait a bit for subscribers to connect (PUB/SUB pattern)
if pattern == "PUB":
    time.sleep(1)

# Publish data continuously
counter = 0
rate = int(os.getenv("RATE", "100"))  # messages per second target
target_data_rate_mbps = float(os.getenv("TARGET_DATA_RATE_MBPS", "100"))  # Target data rate in MB/s
message_size_bytes = int(os.getenv("MESSAGE_SIZE_BYTES", str(int(target_data_rate_mbps * 1024 * 1024 / rate))))  # Calculate message size to achieve target rate

# Data rate measurement (data-source -> data-processor)
measure_interval = float(os.getenv("RATE_MEASURE_INTERVAL_SEC", "5.0"))
rate_window_start = time.time()
rate_window_bytes = 0
rate_window_msgs = 0
total_bytes_sent = 0

print(f"[{task_name}] Starting to publish data at ~{rate} msg/s")
print(f"[{task_name}] Target data rate: {target_data_rate_mbps} MB/s")
print(f"[{task_name}] Message size: {message_size_bytes} bytes (~{message_size_bytes/1024:.2f} KB)")
print(f"[{task_name}] Data rate measurement interval: {measure_interval}s")

start_time = time.time()

# Pre-generate padding data to reach target message size
def create_message_with_size(counter_val, target_size):
    """Create a message that is approximately target_size bytes"""
    base_message = {
        "task": task_name,
        "counter": counter_val,
        "timestamp": time.time(),
        "data": f"Sample data from {task_name} - message {counter_val}"
    }
    base_json = json.dumps(base_message)
    base_size = len(base_json.encode())
    
    # Add padding to reach target size
    if target_size > base_size:
        padding_size = target_size - base_size - 50  # Leave some margin for JSON overhead
        padding = "x" * max(0, padding_size)
        base_message["padding"] = padding
    
    return json.dumps(base_message)

try:
    while True:
        for topic in publish_topics:
            if topic.strip():
                message_json = create_message_with_size(counter, message_size_bytes)
                message_bytes = message_json.encode()
                frame_size = len(topic.encode()) + len(message_bytes)
                # PUB pattern: send topic first, then message
                socket.send_multipart([topic.encode(), message_bytes])
                counter += 1
                total_bytes_sent += frame_size
                rate_window_bytes += frame_size
                rate_window_msgs += 1
                # Periodic data rate report (data-source -> data-processor)
                now = time.time()
                if now - rate_window_start >= measure_interval:
                    elapsed = now - rate_window_start
                    mbps = (rate_window_bytes / (1024 * 1024)) / elapsed if elapsed > 0 else 0
                    msgps = rate_window_msgs / elapsed if elapsed > 0 else 0
                    print(f"[{task_name}] DATA RATE (send to data-processor): {mbps:.2f} MB/s, {msgps:.1f} msg/s (window {elapsed:.1f}s)")
                    rate_window_start = now
                    rate_window_bytes = 0
                    rate_window_msgs = 0
                if counter % 100 == 0:
                    print(f"[{task_name}] Published to {topic}: message {counter} ({len(message_bytes)} bytes)")
        
        # If no topics specified, use default
        if not publish_topics or not any(t.strip() for t in publish_topics):
            message_json = create_message_with_size(counter, message_size_bytes)
            message_bytes = message_json.encode()
            socket.send(message_bytes)
            counter += 1
            total_bytes_sent += len(message_bytes)
            rate_window_bytes += len(message_bytes)
            rate_window_msgs += 1
            now = time.time()
            if now - rate_window_start >= measure_interval:
                elapsed = now - rate_window_start
                mbps = (rate_window_bytes / (1024 * 1024)) / elapsed if elapsed > 0 else 0
                msgps = rate_window_msgs / elapsed if elapsed > 0 else 0
                print(f"[{task_name}] DATA RATE (send to data-processor): {mbps:.2f} MB/s, {msgps:.1f} msg/s (window {elapsed:.1f}s)")
                rate_window_start = now
                rate_window_bytes = 0
                rate_window_msgs = 0
            if counter % 100 == 0:
                print(f"[{task_name}] Published message {counter} ({len(message_bytes)} bytes)")
        
        # Rate limiting - for 100 MB/s target, we want 100 msg/s
        # With 1MB messages, that's 100 MB/s
        # Calculate sleep to achieve target rate, but allow faster if processing is slow
        if rate > 0:
            # Target: 1/rate seconds per message
            # But account for processing time - if we're behind, don't sleep
            target_interval = 1.0 / rate
            # Use very small sleep to allow maximum throughput
            # The HWM settings should prevent blocking
            sleep_time = max(0.0, target_interval - 0.0001)  # Minimal sleep
            if sleep_time > 0:
                time.sleep(sleep_time)
        else:
            time.sleep(0.0001)  # Minimal sleep
        
except KeyboardInterrupt:
    print(f"\n[{task_name}] Shutting down...")
finally:
    if counter > 0 and total_bytes_sent > 0:
        elapsed_total = time.time() - start_time
        avg_mbps = (total_bytes_sent / (1024 * 1024)) / elapsed_total if elapsed_total > 0 else 0
        avg_msgps = counter / elapsed_total if elapsed_total > 0 else 0
        print(f"[{task_name}] DATA RATE SUMMARY: {total_bytes_sent / (1024*1024):.2f} MB, {counter} msgs in {elapsed_total:.1f}s -> avg {avg_mbps:.2f} MB/s, {avg_msgps:.1f} msg/s")
    socket.close()
    context.term()
