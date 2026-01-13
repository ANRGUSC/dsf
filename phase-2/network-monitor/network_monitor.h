// SPDX-License-Identifier: GPL-2.0
#ifndef __NETWORK_MONITOR_H
#define __NETWORK_MONITOR_H

#include <linux/types.h>

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

struct iface_key {
    __u32 ifindex;
};

struct iface_stats {
    __u64 tx_bytes;
    __u64 rx_bytes;
    __u64 tx_packets;
    __u64 rx_packets;
};

#endif /* __NETWORK_MONITOR_H */
