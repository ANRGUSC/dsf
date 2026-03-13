# Network Monitor - eBPF-based Network Metrics Collector

## Overview

This is a C-based eBPF network monitoring solution that collects:
- **Per-connection metrics**: TX/RX bytes, packet counts, and TCP RTT
- **Per-interface metrics**: TX/RX bytes and packet counts
- **Low overhead**: < 1% CPU and bandwidth impact

The monitor runs as a Kubernetes DaemonSet (one pod per node) and exports metrics in Prometheus format.

## Architecture

1. **eBPF Kernel Programs** (`network_monitor.bpf.c`):
   - `tcp_sendmsg` kprobe: Tracks outgoing TCP data
   - `tcp_cleanup_rbuf` kprobe: Tracks incoming TCP data and measures RTT
   - XDP program: Counts incoming interface traffic
   - TC egress hook: Counts outgoing interface traffic

2. **User-space Collector** (`monitor.c`):
   - Loads eBPF programs into the kernel
   - Reads data from eBPF maps
   - Exports metrics to Prometheus format
   - Prints human-readable stats to console

3. **Kubernetes Deployment**:
   - DaemonSet ensures one pod per node
   - Privileged mode for eBPF operations
   - Host network access for accurate monitoring

## Metrics Exported

### Connection-level Metrics
- `network_connection_tx_bytes{src_ip, src_port, dst_ip, dst_port}`: Total bytes sent
- `network_connection_rx_bytes{src_ip, src_port, dst_ip, dst_port}`: Total bytes received
- `network_connection_rtt_microseconds{src_ip, src_port, dst_ip, dst_port}`: TCP RTT

### Interface-level Metrics
- `network_interface_tx_bytes{interface}`: Total bytes transmitted
- `network_interface_rx_bytes{interface}`: Total bytes received

## Building

```bash
# Build Docker image
docker build -t mohammadalikh/network-monitor:latest .

# Push to Docker Hub
docker push mohammadalikh/network-monitor:latest
```

## Deployment

```bash
# Apply RBAC
kubectl apply -f rbac.yml

# Deploy DaemonSet
kubectl apply -f daemonset.yml

# Check status
kubectl get pods -n kube-system -l app=network-monitor

# View logs
kubectl logs -n kube-system -l app=network-monitor --tail=50
```

## Viewing Metrics

Metrics are exported to `/var/run/network_metrics.prom` on each node. You can:

1. **View console output**:
   ```bash
   kubectl logs -n kube-system -l app=network-monitor -f
   ```

2. **Access metrics file** (on the node):
   ```bash
   cat /var/run/network_metrics.prom
   ```

3. **Integrate with Prometheus** (future work):
   - Set up Prometheus to scrape the metrics file
   - Or expose metrics via HTTP endpoint

## Configuration

Edit `daemonset.yml` to change:
- Update interval (default: 5 seconds)
- Metrics file path (default: `/var/run/network_metrics.prom`)
- Resource limits

## Requirements

- Linux kernel 5.8+ (for BTF support)
- Kubernetes 1.20+
- eBPF support enabled in kernel
- Privileged pod security policy

## Troubleshooting

1. **Pod not starting**: Check if node has kernel headers installed
2. **Permission denied**: Ensure privileged security context
3. **BPF program load failed**: Check kernel version and eBPF support

## Integration with k3s Metrics

To integrate with k3s's embedded metrics:
1. Configure Prometheus to scrape `/var/run/network_metrics.prom`
2. Or modify `monitor.c` to expose HTTP endpoint for scraping
3. Add ServiceMonitor for automatic Prometheus discovery
