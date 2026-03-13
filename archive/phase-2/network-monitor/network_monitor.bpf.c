// SPDX-License-Identifier: GPL-2.0
// eBPF program to monitor network traffic and TCP RTT

#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>
#include <bpf/bpf_core_read.h>
#include <bpf/bpf_tracing.h>

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
    __u64 rtt_us;        // RTT in microseconds
    __u64 last_update;   // timestamp
    __u32 packets_sent;
    __u32 packets_recv;
};

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 10240);
    __type(key, struct conn_key);
    __type(value, struct conn_stats);
} conn_stats_map SEC(".maps");

// Map to store per-interface statistics
struct iface_key {
    __u32 ifindex;
};

struct iface_stats {
    __u64 tx_bytes;
    __u64 rx_bytes;
    __u64 tx_packets;
    __u64 rx_packets;
};

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 256);
    __type(key, struct iface_key);
    __type(value, struct iface_stats);
} iface_stats_map SEC(".maps");

// Hook on TCP send
SEC("kprobe/tcp_sendmsg")
int BPF_KPROBE(tcp_sendmsg, struct sock *sk, struct msghdr *msg, size_t size)
{
    struct conn_key key = {};
    struct conn_stats *stats;
    
    // Extract connection info
    key.saddr = BPF_CORE_READ(sk, __sk_common.skc_rcv_saddr);
    key.daddr = BPF_CORE_READ(sk, __sk_common.skc_daddr);
    key.sport = bpf_ntohs(BPF_CORE_READ(sk, __sk_common.skc_num));
    key.dport = bpf_ntohs(BPF_CORE_READ(sk, __sk_common.skc_dport));
    
    // Update or create stats
    stats = bpf_map_lookup_elem(&conn_stats_map, &key);
    if (stats) {
        __sync_fetch_and_add(&stats->tx_bytes, size);
        __sync_fetch_and_add(&stats->packets_sent, 1);
        stats->last_update = bpf_ktime_get_ns();
    } else {
        struct conn_stats new_stats = {};
        new_stats.tx_bytes = size;
        new_stats.packets_sent = 1;
        new_stats.last_update = bpf_ktime_get_ns();
        bpf_map_update_elem(&conn_stats_map, &key, &new_stats, BPF_ANY);
    }
    
    return 0;
}

// Hook on TCP receive
SEC("kprobe/tcp_cleanup_rbuf")
int BPF_KPROBE(tcp_cleanup_rbuf, struct sock *sk, int copied)
{
    struct conn_key key = {};
    struct conn_stats *stats;
    
    if (copied <= 0)
        return 0;
    
    // Extract connection info
    key.saddr = BPF_CORE_READ(sk, __sk_common.skc_rcv_saddr);
    key.daddr = BPF_CORE_READ(sk, __sk_common.skc_daddr);
    key.sport = bpf_ntohs(BPF_CORE_READ(sk, __sk_common.skc_num));
    key.dport = bpf_ntohs(BPF_CORE_READ(sk, __sk_common.skc_dport));
    
    // Update or create stats
    stats = bpf_map_lookup_elem(&conn_stats_map, &key);
    if (stats) {
        __sync_fetch_and_add(&stats->rx_bytes, copied);
        __sync_fetch_and_add(&stats->packets_recv, 1);
        stats->last_update = bpf_ktime_get_ns();
        
        // Get RTT from TCP socket
        struct tcp_sock *tp = (struct tcp_sock *)sk;
        __u32 srtt = BPF_CORE_READ(tp, srtt_us);
        stats->rtt_us = srtt >> 3;  // Convert from 8x resolution
    } else {
        struct conn_stats new_stats = {};
        new_stats.rx_bytes = copied;
        new_stats.packets_recv = 1;
        new_stats.last_update = bpf_ktime_get_ns();
        
        struct tcp_sock *tp = (struct tcp_sock *)sk;
        __u32 srtt = BPF_CORE_READ(tp, srtt_us);
        new_stats.rtt_us = srtt >> 3;
        
        bpf_map_update_elem(&conn_stats_map, &key, &new_stats, BPF_ANY);
    }
    
    return 0;
}

// Note: XDP and TC programs removed for compatibility with older kernels
// Interface-level statistics will be aggregated from connection-level data

char LICENSE[] SEC("license") = "GPL";
