# DAG Scheduling Framework for k3s - System Architecture

## Vision
A seamless k3s plugin that enables users to:
- Develop custom scheduling algorithms/policies
- Execute DAGs with automatic data communication
- Monitor network metrics and performance
- Manage workflows via APIs and UI

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         User Interface Layer                    │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐           │
│  │   Web UI     │  │  CLI Tool    │  │  REST API    │           │
│  └──────────────┘  └──────────────┘  └──────────────┘           │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      API Gateway / Controller                   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  DAG Management API                                      │   │
│  │  - Create/Update/Delete DAGs                             │   │
│  │  - Submit workflows                                      │   │
│  │  - Query status/results                                  │   │
│  └──────────────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Data Communication API                                  │   │
│  │  - Register data producers/consumers                     │   │
│  │  - Transfer data (TCP/UDP/MQTT/Kafka/etc)                │   │
│  │  - Query transfer status                                 │   │
│  └──────────────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Metrics API                                             │   │
│  │  - Network metrics (bandwidth, latency, etc)             │   │
│  │  - Task execution metrics                                │   │
│  │  - Historical data                                       │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│  Scheduler   │    │  Data        │    │  Network     │
│  Framework   │    │  Transfer    │    │  Metrics     │
│              │    │  Engine      │    │  Collector   │
└──────────────┘    └──────────────┘    └──────────────┘
        │                     │                     │
        ▼                     ▼                     ▼
┌─────────────────────────────────────────────────────────────┐
│                    Kubernetes Layer                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │  Scheduler   │  │  Controller  │  │  Metrics     │       │
│  │  Plugins     │  │  (DAG/Pod)   │  │  Server      │       │
│  └──────────────┘  └──────────────┘  └──────────────┘       │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │  k3s Cluster    │
                    │  (Pods/Nodes)   │
                    └─────────────────┘
```

---

## Core Components

### 1. **Scheduler Framework**
**Purpose**: Pluggable scheduling algorithm system

**Components**:
- **Scheduler Interface**: Abstract interface for custom schedulers
- **Scheduler Registry**: Dynamic registration of schedulers
- **Built-in Schedulers**: HEFT, Random, Round-Robin, etc.
- **Scheduler Runtime**: Executes scheduling decisions

**Key Features**:
- Plugin-based architecture (Go plugins or gRPC)
- Hot-reloadable schedulers
- Scheduler configuration per DAG
- Scheduler metrics and comparison

### 2. **Data Transfer Engine**
**Purpose**: Seamless data communication between tasks

**Components**:
- **Transfer Protocol Abstraction**: Interface for different protocols
- **Protocol Implementations**: TCP, UDP, MQTT, Kafka, gRPC, HTTP
- **Transfer Manager**: Orchestrates transfers, handles retries
- **Data Registry**: Tracks data producers/consumers
- **Transfer Optimizer**: Chooses optimal protocol/route

**Key Features**:
- Automatic protocol selection based on data size/type
- Fallback mechanisms
- Transfer progress tracking
- Bandwidth-aware scheduling

### 3. **Network Metrics System**
**Purpose**: Continuous network profiling

**Components**:
- **Metrics Collector**: Gathers network stats from nodes
- **Metrics Aggregator**: Processes and stores metrics
- **Metrics API**: Exposes metrics to schedulers/APIs
- **Network Profiler**: Active probing for latency/bandwidth

**Metrics Collected**:
- Inter-node bandwidth
- Inter-node latency
- Node-to-node topology
- Historical trends

### 4. **DAG Controller**
**Purpose**: Manages DAG lifecycle

**Components**:
- **DAG Watcher**: Monitors DAG CRDs
- **Pod Creator**: Creates pods for tasks
- **Dependency Manager**: Tracks task dependencies
- **Status Manager**: Updates DAG status
- **Cleanup Manager**: Garbage collection

### 5. **API Gateway**
**Purpose**: Unified API for all operations

**Endpoints**:
- `/api/v1/dags` - DAG CRUD
- `/api/v1/dags/{name}/submit` - Submit DAG
- `/api/v1/dags/{name}/status` - Get status
- `/api/v1/dags/{name}/logs` - Get logs
- `/api/v1/data/register` - Register data producer/consumer
- `/api/v1/data/transfer` - Initiate transfer
- `/api/v1/metrics/network` - Network metrics
- `/api/v1/metrics/tasks` - Task metrics
- `/api/v1/schedulers` - List available schedulers
- `/api/v1/schedulers/{name}/config` - Configure scheduler

---

## Project Structure

```
dag-scheduler-framework/
├── cmd/
│   ├── api-server/              # Main API server
│   │   └── main.go
│   ├── scheduler-framework/     # Scheduler framework daemon
│   │   └── main.go
│   ├── metrics-collector/       # Network metrics collector
│   │   └── main.go
│   └── cli/                     # CLI tool
│       └── main.go
│
├── pkg/
│   ├── api/                     # API handlers
│   │   ├── dag.go
│   │   ├── data.go
│   │   ├── metrics.go
│   │   └── scheduler.go
│   │
│   ├── scheduler/               # Scheduler framework
│   │   ├── interface.go         # Scheduler interface
│   │   ├── registry.go          # Scheduler registry
│   │   ├── runtime.go           # Scheduler execution
│   │   └── builtin/             # Built-in schedulers
│   │       ├── heft/
│   │       ├── random/
│   │       └── roundrobin/
│   │
│   ├── datatransfer/            # Data transfer engine
│   │   ├── interface.go         # Transfer protocol interface
│   │   ├── manager.go           # Transfer manager
│   │   ├── registry.go          # Data registry
│   │   └── protocols/           # Protocol implementations
│   │       ├── tcp/
│   │       ├── udp/
│   │       ├── mqtt/
│   │       ├── kafka/
│   │       ├── grpc/
│   │       └── http/
│   │
│   ├── metrics/                 # Metrics system
│   │   ├── collector.go         # Metrics collector
│   │   ├── aggregator.go        # Metrics aggregator
│   │   ├── profiler.go          # Network profiler
│   │   └── storage/             # Metrics storage
│   │       ├── memory.go
│   │       └── prometheus.go
│   │
│   ├── controller/              # DAG controller
│   │   ├── dag_watcher.go
│   │   ├── pod_creator.go
│   │   ├── dependency_manager.go
│   │   └── status_manager.go
│   │
│   ├── crd/                     # Custom resources
│   │   ├── dag/
│   │   ├── scheduler/
│   │   └── datatransfer/
│   │
│   └── utils/                   # Utilities
│       ├── kubernetes.go
│       ├── logging.go
│       └── config.go
│
├── api/                         # API definitions
│   ├── v1/
│   │   ├── dag.proto            # gRPC definitions
│   │   ├── data.proto
│   │   └── metrics.proto
│   └── openapi/                 # OpenAPI/Swagger specs
│       └── spec.yaml
│
├── ui/                          # Web UI (React/Vue)
│   ├── src/
│   │   ├── components/
│   │   ├── pages/
│   │   └── services/
│   └── public/
│
├── plugins/                     # Example scheduler plugins
│   ├── example-scheduler/
│   └── custom-policy/
│
├── deployments/                 # Kubernetes manifests
│   ├── api-server/
│   ├── scheduler-framework/
│   ├── metrics-collector/
│   └── crds/
│
├── docs/                        # Documentation
│   ├── architecture.md
│   ├── api.md
│   ├── scheduler-development.md
│   └── user-guide.md
│
├── examples/                    # Example DAGs and apps
│   ├── simple-workflow/
│   ├── ml-pipeline/
│   └── data-processing/
│
├── tests/                       # Integration tests
│   ├── e2e/
│   └── unit/
│
├── go.mod
├── go.sum
└── Makefile
```

---

## Implementation Phases

### Phase 1: Foundation (Weeks 1-2)
**Goal**: Core framework structure

1. **Project Setup**
   - Initialize Go modules
   - Set up project structure
   - Create base interfaces

2. **Scheduler Framework Core**
   - Define scheduler interface
   - Implement scheduler registry
   - Port existing HEFT/Random schedulers as plugins
   - Scheduler runtime engine

3. **Enhanced DAG Controller**
   - Refactor existing controller
   - Add status management
   - Improve error handling

**Deliverables**: Working scheduler framework with pluggable schedulers

---

### Phase 2: Data Transfer Engine (Weeks 3-4)
**Goal**: Multi-protocol data transfer

1. **Transfer Protocol Abstraction**
   - Define transfer interface
   - Implement protocol registry

2. **Protocol Implementations**
   - TCP (enhance existing)
   - UDP
   - HTTP/gRPC
   - MQTT (basic)
   - Kafka (basic)

3. **Transfer Manager**
   - Automatic protocol selection
   - Transfer orchestration
   - Retry logic
   - Progress tracking

4. **Data Registry**
   - Track data producers/consumers
   - Data location tracking

**Deliverables**: Multi-protocol data transfer working

---

### Phase 3: Network Metrics (Weeks 5-6)
**Goal**: Continuous network profiling

1. **Metrics Collector**
   - Node agent for metrics collection
   - Inter-node bandwidth measurement
   - Latency measurement
   - Topology discovery

2. **Metrics Storage**
   - In-memory storage (initial)
   - Prometheus integration (optional)
   - Time-series data

3. **Metrics API**
   - REST API for metrics
   - Real-time metrics
   - Historical queries

4. **Scheduler Integration**
   - Feed metrics to schedulers
   - Dynamic bandwidth updates

**Deliverables**: Network metrics system operational

---

### Phase 4: API Gateway (Weeks 7-8)
**Goal**: Unified REST/gRPC API

1. **API Server**
   - HTTP server setup
   - gRPC server setup
   - Authentication/Authorization

2. **DAG Management API**
   - CRUD operations
   - Submit/status/logs endpoints
   - WebSocket for real-time updates

3. **Data Transfer API**
   - Register producers/consumers
   - Initiate transfers
   - Query transfer status

4. **Metrics API**
   - Network metrics endpoints
   - Task metrics endpoints
   - Historical data queries

5. **Scheduler API**
   - List schedulers
   - Configure schedulers
   - Scheduler metrics

**Deliverables**: Complete API gateway

---

### Phase 5: Enhanced Features (Weeks 9-10)
**Goal**: Advanced features

1. **Shared Memory Optimization**
   - Fix and enhance shared memory
   - Automatic detection and usage

2. **Transfer Optimization**
   - Bandwidth-aware routing
   - Parallel transfers
   - Compression

3. **Scheduler Comparison**
   - Run multiple schedulers
   - Compare makespans
   - A/B testing

4. **Advanced Metrics**
   - Predictive metrics
   - Anomaly detection
   - Recommendations

**Deliverables**: Production-ready features

---

### Phase 6: UI Development (Weeks 11-12)
**Goal**: Web-based management interface

1. **UI Framework Setup**
   - React/Vue.js setup
   - API client library

2. **DAG Management UI**
   - DAG builder (drag-and-drop)
   - DAG visualization
   - Status monitoring
   - Logs viewer

3. **Metrics Dashboard**
   - Network metrics visualization
   - Task execution graphs
   - Scheduler comparison charts

4. **Scheduler Management**
   - Scheduler selection
   - Configuration UI
   - Plugin management

**Deliverables**: Complete web UI

---

## Key Design Decisions

### 1. **Scheduler Plugin Architecture**

**Option A: Go Plugins** (Recommended for simplicity)
```go
type Scheduler interface {
    Name() string
    Schedule(dag *DAG, nodes []Node, metrics *Metrics) (*Schedule, error)
    Configure(config map[string]interface{}) error
}
```

**Option B: gRPC Plugins** (Better isolation)
- Schedulers run as separate services
- Communicate via gRPC
- Hot-reloadable

**Recommendation**: Start with Go plugins, migrate to gRPC if needed

### 2. **Data Transfer Protocol Selection**

**Strategy**:
- Small data (<10MB): TCP/UDP
- Medium data (10MB-1GB): HTTP/gRPC
- Large data (>1GB): Kafka (streaming)
- Real-time: MQTT
- Same node: Shared memory (automatic)

**Implementation**: Protocol selector based on data size, latency requirements, node proximity

### 3. **Metrics Storage**

**Option A: In-Memory** (Fast, limited history)
- Redis-like structure
- Last N hours of data

**Option B: Prometheus** (Scalable, persistent)
- Time-series database
- Long-term storage
- Query language

**Recommendation**: Start with in-memory, add Prometheus as optional backend

### 4. **API Architecture**

**REST API**: Primary interface
- JSON over HTTP
- Standard REST patterns
- WebSocket for real-time

**gRPC API**: Internal/advanced
- High performance
- Streaming support
- Type safety

---

## Technical Stack

### Backend
- **Language**: Go 1.21+
- **Kubernetes Client**: client-go
- **API Framework**: Gin/Echo (REST), gRPC
- **Metrics**: Prometheus client
- **Storage**: In-memory + optional Prometheus/PostgreSQL

### Frontend
- **Framework**: React or Vue.js
- **Visualization**: D3.js, Cytoscape.js (for DAG graphs)
- **UI Library**: Material-UI or Ant Design
- **State Management**: Redux/Vuex

### Infrastructure
- **Container**: Docker
- **Orchestration**: Kubernetes/k3s
- **CI/CD**: GitHub Actions
- **Documentation**: MkDocs or Docusaurus

---

## Example: Custom Scheduler Plugin

```go
// plugins/my-scheduler/main.go
package main

import (
    "dag-scheduler-framework/pkg/scheduler"
    "dag-scheduler-framework/pkg/metrics"
)

type MyScheduler struct {
    config map[string]interface{}
}

func (s *MyScheduler) Name() string {
    return "my-custom-scheduler"
}

func (s *MyScheduler) Schedule(
    dag *scheduler.DAG,
    nodes []scheduler.Node,
    metrics *metrics.NetworkMetrics,
) (*scheduler.Schedule, error) {
    // Your custom scheduling logic
    // Access network metrics: metrics.Bandwidth(node1, node2)
    // Return schedule with task -> node assignments
}

func (s *MyScheduler) Configure(config map[string]interface{}) error {
    s.config = config
    return nil
}

// Export as plugin
func NewScheduler() scheduler.Scheduler {
    return &MyScheduler{}
}
```

---

## Example: Data Transfer API Usage

```go
// In your task application
import "dag-scheduler-framework/pkg/datatransfer"

// Register as data producer
producer := datatransfer.NewProducer("my-task-output")
producer.Publish(data, datatransfer.ProtocolAuto) // Auto-select protocol

// In dependent task
consumer := datatransfer.NewConsumer("my-task-output")
data, err := consumer.Consume(datatransfer.ProtocolAuto)
```

---

## Success Metrics

1. **Performance**
   - Scheduler overhead < 5%
   - Data transfer efficiency > 90%
   - API latency < 100ms (p95)

2. **Usability**
   - Custom scheduler in < 100 lines of code
   - DAG creation via UI in < 5 minutes
   - API documentation completeness

3. **Reliability**
   - 99.9% uptime
   - Automatic failover
   - Zero data loss

---

## Next Steps

1. **Review and Refine**: Get feedback on architecture
2. **Start Phase 1**: Begin with scheduler framework
3. **Iterate**: Build incrementally, test frequently
4. **Document**: Keep docs updated as you build

---

## Questions to Consider

1. **Scalability**: How many DAGs/tasks per cluster?
2. **Security**: Authentication/authorization requirements?
3. **Multi-tenancy**: Support multiple users/namespaces?
4. **Persistence**: Store DAG definitions and history?
5. **Integration**: Integrate with existing CI/CD?

---

This architecture provides a solid foundation for building a comprehensive DAG scheduling framework. Start with Phase 1 and iterate based on feedback and requirements.

