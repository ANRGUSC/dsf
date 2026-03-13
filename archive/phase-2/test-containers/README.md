# Test Containers for Continuous Task Graph

Simple Python-based ZeroMQ containers for testing the ContinuousTaskGraph scheduler.

## Building Containers

```bash
# Build data-source container
cd data-source
docker build -t data-source:test .

# Build data-processor container
cd ../data-processor
docker build -t data-processor:test .
```

## Images

- `data-source:test` - Publishes messages via ZeroMQ PUB pattern
- `data-processor:test` - Subscribes to messages via ZeroMQ SUB pattern and processes them

## Environment Variables

The containers read ZeroMQ configuration from environment variables set by the scheduler:

- `ZMQ_GRAPH_NAME` - Name of the task graph
- `ZMQ_TASK_NAME` - Name of this task
- `ZMQ_TOPIC_PREFIX` - Prefix for all topics
- `ZMQ_PORT` - ZeroMQ port
- `ZMQ_TRANSPORT` - Transport protocol (tcp, ipc, inproc)
- `ZMQ_PATTERN` - ZeroMQ pattern (PUB, SUB, etc.)
- `ZMQ_BIND` - Whether to bind (true) or connect (false)
- `ZMQ_PUBLISH_TOPICS` - Comma-separated list of topics to publish
- `ZMQ_SUBSCRIBE_TOPICS` - Comma-separated list of topics to subscribe
- `ZMQ_SERVICE_NAME` - Kubernetes service name for discovery
- `ZMQ_NAMESPACE` - Kubernetes namespace
