// SPDX-License-Identifier: GPL-2.0
// User-space program to load eBPF and collect network metrics

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <signal.h>
#include <arpa/inet.h>
#include <net/if.h>
#include <time.h>
#include <sys/resource.h>
#include <bpf/libbpf.h>
#include <bpf/bpf.h>
#include "network_monitor.h"

static volatile int exiting = 0;

static void sig_handler(int sig)
{
    exiting = 1;
}

static int libbpf_print_fn(enum libbpf_print_level level, const char *format, va_list args)
{
    return vfprintf(stderr, format, args);
}

// Convert IP address to string
static void ip_to_str(__u32 ip, char *buf, size_t len)
{
    struct in_addr addr;
    addr.s_addr = ip;
    inet_ntop(AF_INET, &addr, buf, len);
}

// Print connection statistics
static void print_conn_stats(int map_fd)
{
    struct conn_key key, next_key;
    struct conn_stats stats;
    char saddr_str[INET_ADDRSTRLEN];
    char daddr_str[INET_ADDRSTRLEN];
    int err;
    
    printf("\n=== Connection Statistics ===\n");
    printf("%-16s %-6s -> %-16s %-6s | TX Bytes | RX Bytes | RTT (us) | TX Pkts | RX Pkts\n",
           "Source IP", "Port", "Dest IP", "Port");
    printf("-----------------------------------------------------------------------------------\n");
    
    memset(&key, 0, sizeof(key));
    while (bpf_map_get_next_key(map_fd, &key, &next_key) == 0) {
        err = bpf_map_lookup_elem(map_fd, &next_key, &stats);
        if (err < 0) {
            key = next_key;
            continue;
        }
        
        ip_to_str(next_key.saddr, saddr_str, sizeof(saddr_str));
        ip_to_str(next_key.daddr, daddr_str, sizeof(daddr_str));
        
        printf("%-16s %-6d -> %-16s %-6d | %8llu | %8llu | %8llu | %7u | %7u\n",
               saddr_str, next_key.sport,
               daddr_str, next_key.dport,
               stats.tx_bytes, stats.rx_bytes, stats.rtt_us,
               stats.packets_sent, stats.packets_recv);
        
        key = next_key;
    }
}

// Print interface statistics
static void print_iface_stats(int map_fd)
{
    struct iface_key key, next_key;
    struct iface_stats stats;
    char ifname[IF_NAMESIZE];
    int err;
    
    printf("\n=== Interface Statistics ===\n");
    printf("%-16s | TX Bytes | RX Bytes | TX Pkts | RX Pkts | TX Mbps | RX Mbps\n", "Interface");
    printf("-------------------------------------------------------------------------\n");
    
    memset(&key, 0, sizeof(key));
    while (bpf_map_get_next_key(map_fd, &key, &next_key) == 0) {
        err = bpf_map_lookup_elem(map_fd, &next_key, &stats);
        if (err < 0) {
            key = next_key;
            continue;
        }
        
        if_indextoname(next_key.ifindex, ifname);
        
        // Calculate approximate throughput (bytes * 8 / 1000000 for Mbps)
        // This is cumulative, not instantaneous rate
        double tx_mbps = (stats.tx_bytes * 8.0) / 1000000.0;
        double rx_mbps = (stats.rx_bytes * 8.0) / 1000000.0;
        
        printf("%-16s | %8llu | %8llu | %7llu | %7llu | %7.2f | %7.2f\n",
               ifname, stats.tx_bytes, stats.rx_bytes,
               stats.tx_packets, stats.rx_packets,
               tx_mbps, rx_mbps);
        
        key = next_key;
    }
}

// Export metrics in Prometheus format
static void export_prometheus_metrics(int conn_map_fd, int iface_map_fd, const char *output_file)
{
    FILE *fp = fopen(output_file, "w");
    if (!fp) {
        fprintf(stderr, "Failed to open output file: %s\n", strerror(errno));
        return;
    }
    
    struct conn_key conn_key, conn_next;
    struct conn_stats conn_stats;
    char saddr_str[INET_ADDRSTRLEN];
    char daddr_str[INET_ADDRSTRLEN];
    
    fprintf(fp, "# HELP network_connection_tx_bytes Total bytes transmitted on connection\n");
    fprintf(fp, "# TYPE network_connection_tx_bytes counter\n");
    
    memset(&conn_key, 0, sizeof(conn_key));
    while (bpf_map_get_next_key(conn_map_fd, &conn_key, &conn_next) == 0) {
        if (bpf_map_lookup_elem(conn_map_fd, &conn_next, &conn_stats) == 0) {
            ip_to_str(conn_next.saddr, saddr_str, sizeof(saddr_str));
            ip_to_str(conn_next.daddr, daddr_str, sizeof(daddr_str));
            
            fprintf(fp, "network_connection_tx_bytes{src_ip=\"%s\",src_port=\"%d\",dst_ip=\"%s\",dst_port=\"%d\"} %llu\n",
                    saddr_str, conn_next.sport, daddr_str, conn_next.dport, conn_stats.tx_bytes);
        }
        conn_key = conn_next;
    }
    
    fprintf(fp, "\n# HELP network_connection_rx_bytes Total bytes received on connection\n");
    fprintf(fp, "# TYPE network_connection_rx_bytes counter\n");
    
    memset(&conn_key, 0, sizeof(conn_key));
    while (bpf_map_get_next_key(conn_map_fd, &conn_key, &conn_next) == 0) {
        if (bpf_map_lookup_elem(conn_map_fd, &conn_next, &conn_stats) == 0) {
            ip_to_str(conn_next.saddr, saddr_str, sizeof(saddr_str));
            ip_to_str(conn_next.daddr, daddr_str, sizeof(daddr_str));
            
            fprintf(fp, "network_connection_rx_bytes{src_ip=\"%s\",src_port=\"%d\",dst_ip=\"%s\",dst_port=\"%d\"} %llu\n",
                    saddr_str, conn_next.sport, daddr_str, conn_next.dport, conn_stats.rx_bytes);
        }
        conn_key = conn_next;
    }
    
    fprintf(fp, "\n# HELP network_connection_rtt_microseconds TCP round-trip time in microseconds\n");
    fprintf(fp, "# TYPE network_connection_rtt_microseconds gauge\n");
    
    memset(&conn_key, 0, sizeof(conn_key));
    while (bpf_map_get_next_key(conn_map_fd, &conn_key, &conn_next) == 0) {
        if (bpf_map_lookup_elem(conn_map_fd, &conn_next, &conn_stats) == 0) {
            ip_to_str(conn_next.saddr, saddr_str, sizeof(saddr_str));
            ip_to_str(conn_next.daddr, daddr_str, sizeof(daddr_str));
            
            fprintf(fp, "network_connection_rtt_microseconds{src_ip=\"%s\",src_port=\"%d\",dst_ip=\"%s\",dst_port=\"%d\"} %llu\n",
                    saddr_str, conn_next.sport, daddr_str, conn_next.dport, conn_stats.rtt_us);
        }
        conn_key = conn_next;
    }
    
    // Interface metrics
    struct iface_key iface_key, iface_next;
    struct iface_stats iface_stats;
    char ifname[IF_NAMESIZE];
    
    fprintf(fp, "\n# HELP network_interface_tx_bytes Total bytes transmitted on interface\n");
    fprintf(fp, "# TYPE network_interface_tx_bytes counter\n");
    
    memset(&iface_key, 0, sizeof(iface_key));
    while (bpf_map_get_next_key(iface_map_fd, &iface_key, &iface_next) == 0) {
        if (bpf_map_lookup_elem(iface_map_fd, &iface_next, &iface_stats) == 0) {
            if_indextoname(iface_next.ifindex, ifname);
            fprintf(fp, "network_interface_tx_bytes{interface=\"%s\"} %llu\n", ifname, iface_stats.tx_bytes);
        }
        iface_key = iface_next;
    }
    
    fprintf(fp, "\n# HELP network_interface_rx_bytes Total bytes received on interface\n");
    fprintf(fp, "# TYPE network_interface_rx_bytes counter\n");
    
    memset(&iface_key, 0, sizeof(iface_key));
    while (bpf_map_get_next_key(iface_map_fd, &iface_key, &iface_next) == 0) {
        if (bpf_map_lookup_elem(iface_map_fd, &iface_next, &iface_stats) == 0) {
            if_indextoname(iface_next.ifindex, ifname);
            fprintf(fp, "network_interface_rx_bytes{interface=\"%s\"} %llu\n", ifname, iface_stats.rx_bytes);
        }
        iface_key = iface_next;
    }
    
    fclose(fp);
}

int main(int argc, char **argv)
{
    struct bpf_object *obj;
    struct bpf_program *prog;
    struct bpf_link *link_tcp_send = NULL, *link_tcp_recv = NULL;
    int conn_map_fd, iface_map_fd;
    int err;
    const char *metrics_file = "/var/run/network_metrics.prom";
    int interval = 5;  // Update interval in seconds
    
    // Parse arguments
    if (argc > 1) {
        interval = atoi(argv[1]);
        if (interval <= 0) interval = 5;
    }
    if (argc > 2) {
        metrics_file = argv[2];
    }
    
    // Set up libbpf logging
    libbpf_set_print(libbpf_print_fn);
    
    // Increase RLIMIT_MEMLOCK
    struct rlimit r = {RLIM_INFINITY, RLIM_INFINITY};
    if (setrlimit(RLIMIT_MEMLOCK, &r)) {
        fprintf(stderr, "Failed to increase RLIMIT_MEMLOCK: %s\n", strerror(errno));
        return 1;
    }
    
    // Load BPF object
    obj = bpf_object__open_file("network_monitor.bpf.o", NULL);
    if (libbpf_get_error(obj)) {
        fprintf(stderr, "Failed to open BPF object\n");
        return 1;
    }
    
    // Load BPF programs
    err = bpf_object__load(obj);
    if (err) {
        fprintf(stderr, "Failed to load BPF object: %d\n", err);
        goto cleanup;
    }
    
    // Get map file descriptors
    conn_map_fd = bpf_object__find_map_fd_by_name(obj, "conn_stats_map");
    iface_map_fd = bpf_object__find_map_fd_by_name(obj, "iface_stats_map");
    
    if (conn_map_fd < 0 || iface_map_fd < 0) {
        fprintf(stderr, "Failed to find BPF maps\n");
        goto cleanup;
    }
    
    // Attach kprobes
    prog = bpf_object__find_program_by_name(obj, "tcp_sendmsg");
    if (prog) {
        link_tcp_send = bpf_program__attach(prog);
        if (libbpf_get_error(link_tcp_send)) {
            fprintf(stderr, "Failed to attach tcp_sendmsg kprobe\n");
            link_tcp_send = NULL;
        } else {
            printf("Successfully attached tcp_sendmsg kprobe\n");
        }
    }
    
    prog = bpf_object__find_program_by_name(obj, "tcp_cleanup_rbuf");
    if (prog) {
        link_tcp_recv = bpf_program__attach(prog);
        if (libbpf_get_error(link_tcp_recv)) {
            fprintf(stderr, "Failed to attach tcp_cleanup_rbuf kprobe\n");
            link_tcp_recv = NULL;
        } else {
            printf("Successfully attached tcp_cleanup_rbuf kprobe\n");
        }
    }
    
    // Set up signal handler
    signal(SIGINT, sig_handler);
    signal(SIGTERM, sig_handler);
    
    printf("Network monitoring started. Press Ctrl+C to exit.\n");
    printf("Metrics will be exported to: %s\n", metrics_file);
    printf("Update interval: %d seconds\n", interval);
    
    // Main loop
    while (!exiting) {
        sleep(interval);
        
        // Print stats to console
        print_iface_stats(iface_map_fd);
        print_conn_stats(conn_map_fd);
        
        // Export metrics to file for Prometheus
        export_prometheus_metrics(conn_map_fd, iface_map_fd, metrics_file);
        
        printf("\n--- Waiting %d seconds for next update ---\n", interval);
    }
    
    printf("\nCleaning up...\n");

cleanup:
    bpf_link__destroy(link_tcp_send);
    bpf_link__destroy(link_tcp_recv);
    bpf_object__close(obj);
    
    return err != 0;
}
