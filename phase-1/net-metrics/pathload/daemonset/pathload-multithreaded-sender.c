/*
 * Multithreaded Pathload Sender
 * Handles multiple concurrent measurements to different receivers
 */

#define LOCAL
#include "pathload_gbls.h"
#include "pathload_snd.h"
#include <pthread.h>

// Thread data structure
typedef struct {
    char receiver_ip[256];
    int udp_port;
    int tcp_port;
    int thread_id;
} thread_data_t;

// Global port allocator
pthread_mutex_t port_mutex = PTHREAD_MUTEX_INITIALIZER;
int next_port_offset = 0;

int get_next_port_pair(int *udp_port, int *tcp_port) {
    pthread_mutex_lock(&port_mutex);
    int offset = next_port_offset++;
    if (next_port_offset >= 1000) {
        next_port_offset = 0; // Wrap around
    }
    pthread_mutex_unlock(&port_mutex);
    
    *udp_port = 55001 + offset;
    *tcp_port = 55002 + offset;
    return offset;
}

// Thread function to run a single measurement
void* measurement_thread(void* arg) {
    thread_data_t *data = (thread_data_t*)arg;
    
    // Set environment variables for this thread's ports
    char udp_env[32], tcp_env[32];
    sprintf(udp_env, "UDPRCV_PORT=%d", data->udp_port);
    sprintf(tcp_env, "TCPSND_PORT=%d", data->tcp_port);
    putenv(udp_env);
    putenv(tcp_env);
    
    // Run pathload_snd with the receiver IP
    char cmd[512];
    sprintf(cmd, "pathload_snd %s", data->receiver_ip);
    
    printf("[Thread %d] Starting measurement to %s (UDP:%d, TCP:%d)\n", 
           data->thread_id, data->receiver_ip, data->udp_port, data->tcp_port);
    
    int ret = system(cmd);
    
    printf("[Thread %d] Measurement to %s completed with exit code %d\n", 
           data->thread_id, data->receiver_ip, ret);
    
    return NULL;
}

int main(int argc, char* argv[]) {
    // Get list of nodes from k3s
    FILE *fp = popen("k3s kubectl get nodes -o jsonpath='{.items[*].status.addresses[?(@.type==\"InternalIP\")].address}'", "r");
    if (!fp) {
        fprintf(stderr, "Failed to get nodes\n");
        return 1;
    }
    
    char nodes_output[4096];
    fgets(nodes_output, sizeof(nodes_output), fp);
    pclose(fp);
    
    // Get current node IP
    char current_ip[256];
    FILE *hostname_fp = popen("hostname -I | awk '{print $1}'", "r");
    if (hostname_fp) {
        fgets(current_ip, sizeof(current_ip), hostname_fp);
        current_ip[strcspn(current_ip, "\n")] = 0;
        pclose(hostname_fp);
    }
    
    // Parse node IPs
    char *node_ips[64];
    int node_count = 0;
    char *token = strtok(nodes_output, " ");
    while (token && node_count < 64) {
        // Skip current node
        if (strcmp(token, current_ip) != 0) {
            node_ips[node_count] = strdup(token);
            node_count++;
        }
        token = strtok(NULL, " ");
    }
    
    printf("Found %d target nodes. Starting concurrent measurements...\n", node_count);
    
    // Create threads for each measurement
    pthread_t threads[64];
    thread_data_t thread_data[64];
    
    for (int i = 0; i < node_count; i++) {
        thread_data[i].thread_id = i;
        strcpy(thread_data[i].receiver_ip, node_ips[i]);
        get_next_port_pair(&thread_data[i].udp_port, &thread_data[i].tcp_port);
        
        if (pthread_create(&threads[i], NULL, measurement_thread, &thread_data[i]) != 0) {
            fprintf(stderr, "Failed to create thread for %s\n", node_ips[i]);
        }
    }
    
    // Wait for all threads to complete
    for (int i = 0; i < node_count; i++) {
        pthread_join(threads[i], NULL);
        free(node_ips[i]);
    }
    
    printf("All measurements completed.\n");
    return 0;
}
