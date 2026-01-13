// SPDX-License-Identifier: GPL-2.0
// Simple eBPF program to monitor network traffic and TCP RTT (no BTF/CO-RE)

#include <linux/types.h>
#include <linux/bpf.h>
#include <bpf/bpf_helpers.h>

// Map to store per-connection statistics
struct conn_key {
    __u32 saddr;
    __u32 daddr;
    __u16 sport;
    __u16 dport;
};

struct conn_stats {
    __u64 tx_bytes;
    __u64 rx_bytes;
    __u64 rtt_us;
    __u64 last_update;
    __u32 packets_sent;
    __u32 packets_recv;
};

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 10240);
    __type(key, struct conn_key);
    __type(value, struct conn_stats);
} conn_stats_map SEC(".maps");

// Simplified kprobe that just counts events
// We'll use tracepoints instead which are more stable
SEC("tracepoint/sock/inet_sock_set_state")
int trace_inet_sock_set_state(void *ctx)
{
    // This is a placeholder - we'll aggregate data from /proc/net/tcp
    // and other system sources in user-space for better compatibility
    return 0;
}

char LICENSE[] SEC("license") = "GPL";
