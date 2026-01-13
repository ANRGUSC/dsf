# ContinuousTaskGraph Scheduler

A Kubernetes scheduler for ContinuousTaskGraph CRDs that deploys and schedules continuous task graphs with ZeroMQ communication.

## Features

- Watches for `ContinuousTaskGraph` CRD resources
- Automatically deploys all tasks as Kubernetes pods
- Supports task replication (replicas)
- Random node scheduling (for now - HEFT logic to be added)
- ZeroMQ service discovery via Kubernetes Services
- Configures ZeroMQ environment variables for tasks

## Architecture

```
┌─────────────────────┐
│ ContinuousTaskGraph│
│      (CRD)          │
└──────────┬──────────┘
           │ Detected
           ▼
┌─────────────────────┐
│   Scheduler         │
│  - Parse tasks      │
│  - Create pods      │
│  - Create services  │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Kubernetes Pods    │
│  - Task containers  │
│  - ZeroMQ config    │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Kubernetes Services│
│  - ZeroMQ discovery │
└─────────────────────┘
```

## How It Works

1. **CRD Detection**: Scheduler watches for `ContinuousTaskGraph` resources
2. **Task Deployment**: For each task in the graph:
   - Creates pods (respecting replicas count)
   - Sets ZeroMQ environment variables
   - Creates Kubernetes Service for ZeroMQ discovery
3. **Pod Scheduling**: When pods are created with `schedulerName: ctg-scheduler`:
   - Scheduler binds them to random nodes (for now)
   - Future: Will use HEFT algorithm for optimal placement

## ZeroMQ Configuration

The scheduler automatically configures ZeroMQ for each task via environment variables:

- `ZMQ_GRAPH_NAME` - Graph name
- `ZMQ_TASK_NAME` - Task name
- `ZMQ_TOPIC_PREFIX` - Topic prefix
- `ZMQ_PORT` - ZeroMQ port
- `ZMQ_TRANSPORT` - Transport (tcp, ipc, inproc)
- `ZMQ_PATTERN` - Pattern (PUB, SUB, etc.)
- `ZMQ_BIND` - Bind or connect
- `ZMQ_PUBLISH_TOPICS` - Comma-separated publish topics
- `ZMQ_SUBSCRIBE_TOPICS` - Comma-separated subscribe topics
- `ZMQ_SERVICE_NAME` - Kubernetes service name
- `ZMQ_NAMESPACE` - Kubernetes namespace

## Service Discovery

Tasks discover each other via Kubernetes Services:
- Service name pattern: `{graph-name}-{task-name}-service`
- Services expose ZeroMQ ports
- Tasks connect using: `tcp://{service-name}.{namespace}.svc.cluster.local:{port}`

## Status

🚧 **Current Implementation**: Random scheduling
- ✅ CRD watching
- ✅ Pod creation
- ✅ Service creation
- ✅ ZeroMQ configuration
- ⏳ HEFT scheduling (to be implemented)

## Future Enhancements- HEFT algorithm for optimal task placement
- Throughput-aware scheduling
- Resource-aware placement
- Migration support integration
