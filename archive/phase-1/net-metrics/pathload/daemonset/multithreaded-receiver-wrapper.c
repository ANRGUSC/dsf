/*
 * Multithreaded Pathload Receiver Wrapper
 * Runs one receiver per node that connects to all other nodes concurrently
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <strings.h>

#define MAX_NODES 64
#define BASE_UDP_PORT 55001
#define BASE_TCP_PORT 55002

typedef struct {
    int thread_id;
    char sender_ip[256];
    int udp_port;
    int tcp_port;
} receiver_thread_t;

pthread_mutex_t port_mutex = PTHREAD_MUTEX_INITIALIZER;
int next_port = 0;

void get_next_ports(int *udp, int *tcp) {
    pthread_mutex_lock(&port_mutex);
    int offset = next_port++;
    if (next_port >= 1000) next_port = 0;
    pthread_mutex_unlock(&port_mutex);
    *udp = BASE_UDP_PORT + offset;
    *tcp = BASE_TCP_PORT + offset;
}

void* receiver_thread_func(void* arg) {
    receiver_thread_t *data = (receiver_thread_t*)arg;
    
    char udp_env[32], tcp_env[32];
    sprintf(udp_env, "UDPRCV_PORT=%d", data->udp_port);
    sprintf(tcp_env, "TCPSND_PORT=%d", data->tcp_port);
    putenv(udp_env);
    putenv(tcp_env);
    
    printf("[Receiver Thread %d] Connecting to sender %s (ports UDP:%d TCP:%d)\n",
           data->thread_id, data->sender_ip, data->udp_port, data->tcp_port);
    
    // Validate IP before connecting
    if (strlen(data->sender_ip) < 7 || strlen(data->sender_ip) > 15) {
        printf("[Receiver Thread %d] ERROR: Invalid sender IP: %s\n", data->thread_id, data->sender_ip);
        return NULL;
    }
    
    // Run pathload_rcv with retry
    char cmd[512];
    sprintf(cmd, "for i in $(seq 1 10); do pathload_rcv -s %s && break || sleep 2; done",
            data->sender_ip);
    int ret = system(cmd);
    
    printf("[Receiver Thread %d] Measurement from %s completed\n", data->thread_id, data->sender_ip);
    return NULL;
}

int main() {
    // Get all node IPs - use kubectl with mounted kubeconfig
    FILE *fp = popen("KUBECONFIG=/var/lib/rancher/k3s/k3s.yaml kubectl get nodes -o jsonpath='{.items[*].status.addresses[?(@.type==\"InternalIP\")].address}' 2>/dev/null", "r");
    if (!fp) {
        fprintf(stderr, "Failed to get nodes\n");
        return 1;
    }
    
    char buffer[4096] = {0};
    size_t len = 0;
    char *line = NULL;
    while (fgets(buffer + len, sizeof(buffer) - len, fp) != NULL) {
        len = strlen(buffer);
        if (len >= sizeof(buffer) - 1) break;
    }
    pclose(fp);
    
    // Remove trailing newline
    buffer[strcspn(buffer, "\n\r")] = 0;
    
    // Get current node name and IP from downward API or hostname
    char current_node_name[256] = {0};
    const char *node_name_env = getenv("NODE_NAME");
    if (node_name_env) {
        strncpy(current_node_name, node_name_env, sizeof(current_node_name) - 1);
    }
    
    // Get current node IP from kubectl
    char current_ip[256] = {0};
    char node_cmd[512];
    if (strlen(current_node_name) > 0) {
        sprintf(node_cmd, "KUBECONFIG=/var/lib/rancher/k3s/k3s.yaml kubectl get node %s -o jsonpath='{.status.addresses[?(@.type==\"InternalIP\")].address}' 2>/dev/null", current_node_name);
    } else {
        // Fallback: get first IP from hostname that matches pattern
        strcpy(node_cmd, "hostname -I | awk '{for(i=1;i<=NF;i++) if($i ~ /^192\\.168\\./) {print $i; exit}}'");
    }
    FILE *ip_fp = popen(node_cmd, "r");
    if (ip_fp) {
        fgets(current_ip, sizeof(current_ip), ip_fp);
        current_ip[strcspn(current_ip, "\n\r")] = 0;
        pclose(ip_fp);
    }
    
    printf("Current node: %s, IP: %s\n", current_node_name, current_ip);
    printf("Raw kubectl output: [%s]\n", buffer);
    
    // Parse node IPs - split by space
    char *node_ips[MAX_NODES];
    int node_count = 0;
    char *saveptr;
    char *token = strtok_r(buffer, " \t\n", &saveptr);
    printf("DEBUG: Starting tokenization. Buffer length: %zu\n", strlen(buffer));
    int token_num = 0;
    while (token && node_count < MAX_NODES) {
        token_num++;
        printf("DEBUG: Token %d: [%s] (len=%zu)\n", token_num, token, strlen(token));
        // Skip empty tokens and current node IP
        if (strlen(token) > 0 && strcmp(token, current_ip) != 0) {
            // Validate it's an IPv4 address (starts with numbers)
            if (token[0] >= '0' && token[0] <= '9') {
                node_ips[node_count] = (char*)malloc(strlen(token) + 1);
                if (node_ips[node_count]) {
                    strcpy(node_ips[node_count], token);
                    printf("DEBUG: Added target node %d: [%s]\n", node_count, node_ips[node_count]);
                    node_count++;
                }
            } else {
                printf("DEBUG: Skipping token (doesn't start with digit): [%s]\n", token);
            }
        } else {
            printf("DEBUG: Skipping token (empty or current IP): [%s]\n", token ? token : "(null)");
        }
        token = strtok_r(NULL, " \t\n", &saveptr);
    }
    printf("DEBUG: Total tokens processed: %d, Valid target nodes: %d\n", token_num, node_count);
    
    fflush(stdout);
    printf("=== Multithreaded Pathload Receiver ===\n");
    printf("Current node name: [%s], IP: [%s]\n", current_node_name, current_ip);
    printf("Found %d sender nodes\n", node_count);
    fflush(stdout);
    
    if (node_count == 0) {
        printf("ERROR: No target nodes found. Exiting.\n");
        return 1;
    }
    
    // Create threads for each sender node
    pthread_t threads[MAX_NODES];
    receiver_thread_t thread_data[MAX_NODES];
    
    for (int i = 0; i < node_count; i++) {
        thread_data[i].thread_id = i;
        // Ensure null termination
        memset(thread_data[i].sender_ip, 0, sizeof(thread_data[i].sender_ip));
        strncpy(thread_data[i].sender_ip, node_ips[i], sizeof(thread_data[i].sender_ip) - 1);
        thread_data[i].sender_ip[sizeof(thread_data[i].sender_ip) - 1] = '\0';
        get_next_ports(&thread_data[i].udp_port, &thread_data[i].tcp_port);
        
        printf("Creating thread %d for sender IP: [%s] (len=%zu)\n", i, thread_data[i].sender_ip, strlen(thread_data[i].sender_ip));
        
        if (pthread_create(&threads[i], NULL, receiver_thread_func, &thread_data[i]) != 0) {
            fprintf(stderr, "Failed to create thread for %s\n", node_ips[i]);
        }
    }
    
    // Wait for all threads
    for (int i = 0; i < node_count; i++) {
        pthread_join(threads[i], NULL);
        free(node_ips[i]);
    }
    
    printf("All receiver threads completed\n");
    return 0;
}
