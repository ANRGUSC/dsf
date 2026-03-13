/*
 * Multithreaded Pathload Sender Wrapper
 * Spawns multiple pathload_snd processes - one for each potential receiver
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>

#define MAX_NODES 64
#define BASE_UDP_PORT 55001
#define BASE_TCP_PORT 55002

int compare_strings(const void *a, const void *b) {
    return strcmp(*(const char **)a, *(const char **)b);
}

int main() {
    printf("=== Multithreaded Pathload Sender Starting ===\n");
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
    
    // Parse all node IPs (including current)
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
    
    // Sort IPs for consistent port assignment across all nodes
    qsort(all_ips, total_nodes, sizeof(char*), compare_strings);
    
    printf("=== Configuration ===\n");
    printf("Node: %s, IP: %s\n", node_name ? node_name : "(unknown)", current_ip);
    printf("Total nodes in cluster: %d\n", total_nodes);
    printf("Sorted node IPs:\n");
    for (int i = 0; i < total_nodes; i++) {
        printf("  [%d] %s\n", i, all_ips[i]);
    }
    fflush(stdout);
    
    // Spawn one pathload_snd for each other node (potential receiver)
    // Port assignment: receiver at index i connects to UDP port BASE_UDP_PORT + 2*i
    pid_t pids[MAX_NODES];
    int sender_count = 0;
    
    for (int i = 0; i < total_nodes; i++) {
        // Skip if this is current node
        if (strcmp(all_ips[i], current_ip) == 0) {
            continue;
        }
        
        int udp_port = BASE_UDP_PORT + 2 * i;
        int tcp_port = BASE_TCP_PORT + 2 * i;
        
        printf("Starting pathload_snd for receiver %s (UDP:%d, TCP:%d)\n", 
               all_ips[i], udp_port, tcp_port);
        fflush(stdout);
        
        pid_t pid = fork();
        if (pid == 0) {
            // Child process - run pathload_snd
            char udp_env[32], tcp_env[32];
            sprintf(udp_env, "UDPRCV_PORT=%d", udp_port);
            sprintf(tcp_env, "TCPSND_PORT=%d", tcp_port);
            putenv(udp_env);
            putenv(tcp_env);
            
            // Redirect output to include port info
            printf("[Sender %s:%d] Starting pathload_snd\n", current_ip, tcp_port);
            fflush(stdout);
            
            execlp("pathload_snd", "pathload_snd", NULL);
            perror("execlp failed");
            exit(1);
        } else if (pid > 0) {
            pids[sender_count++] = pid;
        } else {
            perror("fork failed");
        }
    }
    
    printf("=== Started %d pathload_snd processes ===\n", sender_count);
    fflush(stdout);
    
    // Wait for all children
    for (int i = 0; i < sender_count; i++) {
        int status;
        waitpid(pids[i], &status, 0);
        printf("pathload_snd process %d exited with status %d\n", i, WEXITSTATUS(status));
    }
    
    // Cleanup
    for (int i = 0; i < total_nodes; i++) {
        free(all_ips[i]);
    }
    
    printf("=== All sender processes completed ===\n");
    return 0;
}
