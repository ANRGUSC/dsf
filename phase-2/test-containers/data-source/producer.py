#!/usr/bin/env python3
import os
import time
import zmq
import json

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

print(f"[{task_name}] Starting to publish data at ~{rate} msg/s")

try:
    while True:
        for topic in publish_topics:
            if topic.strip():
                message = {
                    "task": task_name,
                    "counter": counter,
                    "timestamp": time.time(),
                    "data": f"Sample data from {task_name} - message {counter}"
                }
                # PUB pattern: send topic first, then message
                socket.send_multipart([topic.encode(), json.dumps(message).encode()])
                counter += 1
                print(f"[{task_name}] Published to {topic}: message {counter}")
        
        # If no topics specified, use default
        if not publish_topics or not any(t.strip() for t in publish_topics):
            message = {
                "task": task_name,
                "counter": counter,
                "timestamp": time.time(),
                "data": f"Sample data from {task_name} - message {counter}"
            }
            socket.send(json.dumps(message).encode())
            counter += 1
            if counter % 10 == 0:
                print(f"[{task_name}] Published message {counter}")
        
        # Rate limiting
        time.sleep(1.0 / rate if rate > 0 else 1.0)
        
except KeyboardInterrupt:
    print(f"\n[{task_name}] Shutting down...")
finally:
    socket.close()
    context.term()
