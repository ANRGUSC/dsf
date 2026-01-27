/*
 * Multithreaded Pathload Sender Wrapper
 * Runs one sender per node that accepts connections from all other nodes concurrently
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define MAX_NODES 64
#define BASE_UDP_PORT 55001
#define BASE_TCP_PORT 55002

typedef struct {
    int thread_id;
    char receiver_ip[256];
    int udp_port;
    int tcp_port;
} sender_thread_t;

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

void* sender_thread_func(void* arg) {
    sender_thread_t *data = (sender_thread_t*)arg;
    
    char udp_env[32], tcp_env[32];
    sprintf(udp_env, "UDPRCV_PORT=%d", data->udp_port);
    sprintf(tcp_env, "TCPSND_PORT=%d", data->tcp_port);
    putenv(udp_env);
    putenv(tcp_env);
    
    printf("[Sender] Ports allocated for potential receiver %s: UDP:%d TCP:%d\n",
           data->receiver_ip, data->udp_port, data->tcp_port);
    
    // Note: pathload_snd accepts connections in a loop, so we don't need threads here
    // This is just for port allocation tracking
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
    while (fgets(buffer + len, sizeof(buffer) - len, fp) != NULL) {
        len = strlen(buffer);
        if (len >= sizeof(buffer) - 1) break;
    }
    pclose(fp);
    
    // Remove trailing newline
    buffer[strcspn(buffer, "\n\r")] = 0;
    
    // Get current node name and IP
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
    while (token && node_count < MAX_NODES) {
        // Skip empty tokens and current node IP
        if (strlen(token) > 0 && strcmp(token, current_ip) != 0) {
            // Validate it's an IPv4 address
            if (token[0] >= '0' && token[0] <= '9') {
                node_ips[node_count] = strdup(token);
                printf("Found target node: %s\n", node_ips[node_count]);
                node_count++;
            }
        }
        token = strtok_r(NULL, " \t\n", &saveptr);
    }
    
    printf("Pathload Sender (multithreaded - accepts multiple connections)\n");
    printf("Current node IP: %s\n", current_ip);
    printf("Will accept connections from %d receiver nodes\n", node_count);
    
    // pathload_snd already handles multiple connections in a loop
    // Just run it - it will accept connections from all receivers
    printf("Starting pathload_snd (will accept connections from all receivers)...\n");
    
    // Use default ports for the main sender (it accepts connections)
    char udp_env[32], tcp_env[32];
    sprintf(udp_env, "UDPRCV_PORT=%d", BASE_UDP_PORT);
    sprintf(tcp_env, "TCPSND_PORT=%d", BASE_TCP_PORT);
    putenv(udp_env);
    putenv(tcp_env);
    
    // Run pathload_snd - it loops accepting connections
    system("pathload_snd");
    
    for (int i = 0; i < node_count; i++) {
        free(node_ips[i]);
    }
    
    return 0;
}
