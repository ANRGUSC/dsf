# Phase 2: Continuous Task Graph Execution with Dynamic Migration

This phase extends the DAG scheduling framework to support continuous task graph execution with dynamic task migration on network partitions and node failures.

## Architecture

- **cmd/**: Main application entry points
- **pkg/**: Reusable libraries and components
- **deployments/**: Kubernetes deployment manifests
- **api/**: Custom Resource Definitions (CRDs)
- **charts/**: Helm charts for k3s plugin installation

## Components

### Scheduler (`cmd/scheduler/`)
Enhanced HEFT scheduler optimized for throughput instead of makespan, supporting continuous task execution.

### Migration Controller (`cmd/migration-controller/`)
Orchestrates dynamic task migration when network partitions or node failures are detected.

### Metrics Collector (`cmd/metrics-collector/`)
Collects throughput, latency, and resource utilization metrics for continuous task graphs.

### MQTT Library (`pkg/mqtt/`)
MQTT client library for task-to-task communication in edge/IoT environments.

### Migration Logic (`pkg/migration/`)
Core logic for detecting failures, planning migrations, and executing task relocations.

### Scheduler Library (`pkg/scheduler/`)
Throughput-aware scheduling algorithms and resource management.

### Metrics Library (`pkg/metrics/`)
Metrics collection, aggregation, and export functionality.

## Communication

- **MQTT**: Primary communication protocol for control messages and data transfer
- **Kubernetes API**: For pod management and cluster state
- **Prometheus**: For metrics export

## Status

🚧 Under Development

