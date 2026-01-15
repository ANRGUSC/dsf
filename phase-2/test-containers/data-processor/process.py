#!/usr/bin/env python3
import os
import time
import zmq
import json

# Get configuration from environment
graph_name = os.getenv("ZMQ_GRAPH_NAME", "test")
task_name = os.getenv("ZMQ_TASK_NAME", "data-processor")
topic_prefix = os.getenv("ZMQ_TOPIC_PREFIX", graph_name)
port = int(os.getenv("ZMQ_PORT", "5556"))
transport = os.getenv("ZMQ_TRANSPORT", "tcp")
pattern = os.getenv("ZMQ_PATTERN", "SUB")
bind = os.getenv("ZMQ_BIND", "false").lower() == "true"
subscribe_topics = os.getenv("ZMQ_SUBSCRIBE_TOPICS", "").split(",") if os.getenv("ZMQ_SUBSCRIBE_TOPICS") else []
publish_topics = os.getenv("ZMQ_PUBLISH_TOPICS", "").split(",") if os.getenv("ZMQ_PUBLISH_TOPICS") else []

print(f"[{task_name}] Starting ZeroMQ subscriber/processor")
print(f"[{task_name}] Port: {port}, Transport: {transport}, Bind: {bind}")
print(f"[{task_name}] Subscribe topics: {subscribe_topics}")
print(f"[{task_name}] Publish topics: {publish_topics}")

# Create ZeroMQ context
context = zmq.Context()

# Create subscriber socket
sub_socket = context.socket(zmq.SUB)

# Build subscribe address
if transport == "tcp":
    if bind:
        sub_address = f"tcp://*:{port}"
    else:
        # For connect, use service discovery to find publisher
        namespace = os.getenv("ZMQ_NAMESPACE", "default")
        # Connect to publisher service (data-source)
        # Get the first subscribe topic to determine which task to connect to
        if subscribe_topics:
            # Extract task name from topic (e.g., "test-task-graph/data-source/raw" -> "data-source")
            first_topic = subscribe_topics[0].strip()
            parts = first_topic.split("/")
            if len(parts) >= 2:
                publisher_task = parts[1]  # e.g., "data-source"
            else:
                publisher_task = "data-source"  # default
        else:
            publisher_task = "data-source"  # default
        
        publisher_service = f"{graph_name}-{publisher_task}-service"
        # Use default port 5555 for publisher (or get from env if available)
        publisher_port = 5555
        sub_address = f"tcp://{publisher_service}.{namespace}.svc.cluster.local:{publisher_port}"
elif transport == "ipc":
    sub_address = f"ipc:///tmp/data-source.ipc"
else:
    sub_address = f"inproc://data-source"

print(f"[{task_name}] Subscribing to: {sub_address}")

if bind:
    sub_socket.bind(sub_address)
else:
    sub_socket.connect(sub_address)

# Subscribe to topics
if subscribe_topics:
    for topic in subscribe_topics:
        if topic.strip():
            sub_socket.setsockopt_string(zmq.SUBSCRIBE, topic.strip())
            print(f"[{task_name}] Subscribed to topic: {topic.strip()}")
else:
    # Subscribe to all messages
    sub_socket.setsockopt_string(zmq.SUBSCRIBE, "")
    print(f"[{task_name}] Subscribed to all messages")

# Create publisher socket if needed
pub_socket = None
if publish_topics:
    pub_socket = context.socket(zmq.PUB)
    pub_port = port + 1000  # Use different port for publishing
    pub_address = f"tcp://*:{pub_port}" if transport == "tcp" else f"ipc:///tmp/{task_name}-pub.ipc"
    pub_socket.bind(pub_address)
    print(f"[{task_name}] Publishing to: {pub_address}")
    time.sleep(1)  # Wait for subscribers

print(f"[{task_name}] Ready to process messages...")

try:
    processed_count = 0
    last_log_time = time.time()
    bytes_received_since_log = 0
    start_time = time.time()
    total_bytes = 0
    
    while True:
        try:
            # Receive message (with topic if PUB/SUB pattern)
            if subscribe_topics:
                parts = sub_socket.recv_multipart(zmq.NOBLOCK)
                if len(parts) == 2:
                    topic = parts[0].decode()
                    message_data = parts[1].decode()
                    msg_bytes = len(parts[0]) + len(parts[1])
                else:
                    message_data = parts[0].decode()
                    topic = ""
                    msg_bytes = len(parts[0])
            else:
                message_data = sub_socket.recv_string(zmq.NOBLOCK)
                topic = ""
                msg_bytes = len(message_data.encode())
            
            total_bytes += msg_bytes
            bytes_received_since_log += msg_bytes
            
            # Parse message
            message = json.loads(message_data)
            processed_count += 1
            
            # Log less frequently for high throughput
            current_time = time.time()
            elapsed_since_log = current_time - last_log_time
            if elapsed_since_log >= 5.0 or processed_count % 1000 == 0:
                avg_msg_size_kb = (total_bytes / processed_count) / 1024 if processed_count > 0 else 0
                rate_mbps = (bytes_received_since_log / elapsed_since_log) / (1024 * 1024) if elapsed_since_log > 0 else 0
                print(f"[{task_name}] Received message {processed_count} from {message.get('task', 'unknown')} | "
                      f"Avg size: ~{avg_msg_size_kb:.2f} KB/msg | Current rate: ~{rate_mbps:.2f} MB/s")
                last_log_time = current_time
                bytes_received_since_log = 0
            
            # Process the message (simulate processing)
            processed_message = {
                "task": task_name,
                "original_task": message.get("task"),
                "original_counter": message.get("counter"),
                "processed_counter": processed_count,
                "timestamp": time.time(),
                "data": f"Processed: {message.get('data', '')[:100]}"  # Truncate for logging
            }
            
            # Publish processed message if publisher is configured
            if pub_socket and publish_topics:
                for pub_topic in publish_topics:
                    if pub_topic.strip():
                        pub_socket.send_multipart([pub_topic.encode(), json.dumps(processed_message).encode()])
                        if processed_count % 1000 == 0:
                        print(f"[{task_name}] Published processed message to {pub_topic}")
            
        except zmq.Again:
            # No message available, continue
            time.sleep(0.001)  # Reduced sleep for higher throughput
        except Exception as e:
            print(f"[{task_name}] Error processing message: {e}")
            time.sleep(0.1)
            
except KeyboardInterrupt:
    print(f"\n[{task_name}] Shutting down...")
finally:
    sub_socket.close()
    if pub_socket:
        pub_socket.close()
    context.term()
