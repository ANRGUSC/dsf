/*
 * Multithreaded Pathload Receiver Wrapper
 * Spawns threads to connect to multiple senders concurrently
 * Port assignment matches sender's port allocation
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/stat.h>

#define MAX_NODES 64
#define BASE_UDP_PORT 55001
#define BASE_TCP_PORT 55002
#define LOCAL_UDP_BASE 56001  // Base for local UDP binding (different range)

typedef struct {
    int thread_id;
    char sender_ip[256];
    int local_udp_port;   // Local UDP port to bind (must be unique per thread)
    int remote_tcp_port;  // Sender's TCP port to connect to
} receiver_thread_t;

int compare_strings(const void *a, const void *b) {
    return strcmp(*(const char **)a, *(const char **)b);
}

void* receiver_thread_func(void* arg) {
    receiver_thread_t *data = (receiver_thread_t*)arg;
    
    printf("[Thread %d] Connecting to %s (local UDP:%d, remote TCP:%d)\n",
           data->thread_id, data->sender_ip, data->local_udp_port, data->remote_tcp_port);
    fflush(stdout);
    
    // Create unique log file for this thread to avoid race conditions
    char logfile[256];
    sprintf(logfile, "/tmp/pathload_%d_%s.log", data->thread_id, data->sender_ip);
    
    // Create unique working directory for this thread
    char workdir[256];
    sprintf(workdir, "/tmp/pathload_thread_%d", data->thread_id);
    mkdir(workdir, 0755);
    
    // UDPRCV_PORT: local port to bind for receiving UDP
    // TCPSND_PORT: sender's port to connect to
    // Use -o to specify unique output file
    char cmd[1024];
    sprintf(cmd, "cd %s && UDPRCV_PORT=%d TCPSND_PORT=%d pathload_rcv -q -o %s -s %s 2>&1",
            workdir, data->local_udp_port, data->remote_tcp_port, logfile, data->sender_ip);
    
    int ret = system(cmd);
    
    // Read and display results from log file
    FILE *fp = fopen(logfile, "r");
    if (fp) {
        char line[512];
        while (fgets(line, sizeof(line), fp)) {
            if (strstr(line, "Avail") || strstr(line, "bw") || strstr(line, "Mbps")) {
                printf("[Thread %d] %s -> %s", data->thread_id, data->sender_ip, line);
            }
        }
        fclose(fp);
    }
    
    printf("[Thread %d] Measurement from %s completed (exit=%d)\n", 
           data->thread_id, data->sender_ip, ret);
    fflush(stdout);
    return NULL;
}

int main() {
    printf("=== Multithreaded Pathload Receiver Starting ===\n");
    printf("Waiting 30 seconds for senders to start...\n");
    fflush(stdout);
    sleep(30);  // Wait for senders to be ready
    printf("Starting receiver connections...\n");
    fflush(stdout);
    
    // Get all node IPs using kubectl
    FILE *fp = popen("KUBECONFIG=/var/lib/rancher/k3s/k3s.yaml kubectl get nodes -o jsonpath='{.items[*].status.addresses[?(@.type==\"InternalIP\")].address}'", "r");
    if (!fp) {
        fprintf(stderr, "Failed to run kubectl\n");
        return 1;
    }
    
    char buffer[4096] = {0};
    if (fgets(buffer, sizeof(buffer), fp) == NULL) {
        fprintf(stderr, "Failed to read kubectl output\n");
        pclose(fp);
        return 1;
    }
    pclose(fp);
    buffer[strcspn(buffer, "\n\r")] = 0;
    
    printf("DEBUG: kubectl output: [%s]\n", buffer);
    fflush(stdout);
    
    // Get current node's IP
    const char *node_name = getenv("NODE_NAME");
    char current_ip[256] = {0};
    
    if (node_name && strlen(node_name) > 0) {
        char cmd[512];
        sprintf(cmd, "KUBECONFIG=/var/lib/rancher/k3s/k3s.yaml kubectl get node %s -o jsonpath='{.status.addresses[?(@.type==\"InternalIP\")].address}'", node_name);
        FILE *ip_fp = popen(cmd, "r");
        if (ip_fp) {
            if (fgets(current_ip, sizeof(current_ip), ip_fp) != NULL) {
                current_ip[strcspn(current_ip, "\n\r")] = 0;
            }
            pclose(ip_fp);
        }
    }
    
    printf("DEBUG: NODE_NAME=%s, current IP=%s\n", node_name ? node_name : "(null)", current_ip);
    fflush(stdout);
    
    // Parse all node IPs
    char *all_ips[MAX_NODES];
    int total_nodes = 0;
    
    char *saveptr;
    char *token = strtok_r(buffer, " \t\n", &saveptr);
    while (token && total_nodes < MAX_NODES) {
        if (strlen(token) > 0) {
            all_ips[total_nodes] = strdup(token);
            total_nodes++;
        }
        token = strtok_r(NULL, " \t\n", &saveptr);
    }
    
    // Sort IPs for consistent port assignment (must match sender's sorting)
    qsort(all_ips, total_nodes, sizeof(char*), compare_strings);
    
    // Find this receiver's index in the sorted list
    int my_index = -1;
    for (int i = 0; i < total_nodes; i++) {
        if (strcmp(all_ips[i], current_ip) == 0) {
            my_index = i;
            break;
        }
    }
    
    printf("=== Configuration ===\n");
    printf("Node: %s, IP: %s, Index: %d\n", node_name ? node_name : "(unknown)", current_ip, my_index);
    printf("Total nodes: %d\n", total_nodes);
    printf("Sorted node IPs:\n");
    for (int i = 0; i < total_nodes; i++) {
        printf("  [%d] %s %s\n", i, all_ips[i], (i == my_index) ? "(me)" : "");
    }
    fflush(stdout);
    
    if (my_index < 0) {
        fprintf(stderr, "ERROR: Could not find current node in IP list!\n");
        return 1;
    }
    
    // Create threads to connect to each sender
    pthread_t threads[MAX_NODES];
    receiver_thread_t thread_data[MAX_NODES];
    int thread_count = 0;
    
    for (int i = 0; i < total_nodes; i++) {
        // Skip current node
        if (i == my_index) {
            continue;
        }
        
        // Sender at index i listens on port based on MY index (the receiver)
        // Sender's port: BASE_TCP_PORT + 2 * my_index
        int remote_tcp_port = BASE_TCP_PORT + 2 * my_index;
        
        // Local UDP port: unique per thread to avoid bind conflicts
        int local_udp_port = LOCAL_UDP_BASE + thread_count;
        
        thread_data[thread_count].thread_id = thread_count;
        memset(thread_data[thread_count].sender_ip, 0, sizeof(thread_data[thread_count].sender_ip));
        strncpy(thread_data[thread_count].sender_ip, all_ips[i], sizeof(thread_data[thread_count].sender_ip) - 1);
        thread_data[thread_count].local_udp_port = local_udp_port;
        thread_data[thread_count].remote_tcp_port = remote_tcp_port;
        
        printf("Thread %d: %s -> local UDP:%d, remote TCP:%d\n", 
               thread_count, all_ips[i], local_udp_port, remote_tcp_port);
        fflush(stdout);
        
        if (pthread_create(&threads[thread_count], NULL, receiver_thread_func, &thread_data[thread_count]) != 0) {
            fprintf(stderr, "Failed to create thread for %s\n", all_ips[i]);
        }
        thread_count++;
    }
    
    printf("=== Started %d receiver threads ===\n", thread_count);
    fflush(stdout);
    
    // Wait for all threads
    for (int i = 0; i < thread_count; i++) {
        pthread_join(threads[i], NULL);
    }
    
    // Cleanup
    for (int i = 0; i < total_nodes; i++) {
        free(all_ips[i]);
    }
    
    printf("=== All receiver threads completed ===\n");
    return 0;
}
