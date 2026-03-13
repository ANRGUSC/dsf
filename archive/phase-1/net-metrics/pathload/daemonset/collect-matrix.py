#!/usr/bin/env python3

import subprocess
import re
import sys
from collections import defaultdict

def run_cmd(cmd):
    """Run a shell command and return output"""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        return ""

def get_all_nodes():
    """Get all node names from cluster"""
    output = run_cmd("sudo k3s kubectl get nodes -o jsonpath='{.items[*].metadata.name}'")
    nodes = sorted(output.split())
    return nodes

def get_orchestrator_pods():
    """Get all orchestrator pod names"""
    output = run_cmd("sudo k3s kubectl get pods -l app=pathload-orchestrator -o jsonpath='{.items[*].metadata.name}'")
    if not output:
        return []
    return output.split()

def get_pod_node(pod_name):
    """Get the node name for a pod"""
    output = run_cmd(f"sudo k3s kubectl get pod {pod_name} -o jsonpath='{{.spec.nodeName}}'")
    return output.strip()

def parse_results(logs):
    """Parse measurement results from logs"""
    results = {}
    latencies = {}
    
    # Find the Results section
    lines = logs.split('\n')
    in_results = False
    
    for line in lines:
        if "=== Results ===" in line:
            in_results = True
            continue
        
        if in_results and "->" in line:
            # Parse: anrg-X -> anrg-Y: bandwidth (latency: time)
            match = re.match(r'(\S+)\s+->\s+(\S+):\s+(.+?)\s+\(latency:\s+([0-9.]+|N/A)\)', line)
            if match:
                from_node = match.group(1)
                to_node = match.group(2)
                bandwidth = match.group(3).strip()
                latency = match.group(4)
                
                # Clean up bandwidth (remove "Mbps" and handle ranges)
                bandwidth = bandwidth.replace('(Mbps)', '').strip()
                if bandwidth == "N/A":
                    bandwidth = "N/A"
                else:
                    # Handle ranges like "100.00 - 0.00" -> "100.00"
                    parts = bandwidth.split(' - ')
                    if len(parts) == 2 and parts[1] == "0.00":
                        bandwidth = parts[0]
                    elif len(parts) == 2:
                        bandwidth = f"{parts[0]}-{parts[1]}"
                
                results[f"{from_node}->{to_node}"] = bandwidth
                latencies[f"{from_node}->{to_node}"] = latency
    
    return results, latencies

def main():
    print("=" * 60)
    print("Pathload Full Measurement Matrix")
    print("=" * 60)
    print()
    
    # Get all nodes
    all_nodes = get_all_nodes()
    if not all_nodes:
        print("No nodes found!")
        sys.exit(1)
    
    # Get orchestrator pods
    pods = get_orchestrator_pods()
    if not pods:
        print("No orchestrator pods found!")
        sys.exit(1)
    
    # Collect results from all pods
    all_results = {}
    all_latencies = {}
    
    print(f"Collecting results from {len(pods)} orchestrator pods...")
    for pod in pods:
        node = get_pod_node(pod)
        if not node:
            continue
        
        print(f"  Processing {pod} on {node}...")
        logs = run_cmd(f"sudo k3s kubectl logs {pod} 2>&1")
        results, latencies = parse_results(logs)
        all_results.update(results)
        all_latencies.update(latencies)
    
    print()
    print("=" * 60)
    print("Full Measurement Matrix (Bandwidth in Mbps)")
    print("=" * 60)
    print()
    
    # Print header
    print(f"{'From\\To':<12}", end="")
    for node in all_nodes:
        print(f"{node:<15}", end="")
    print()
    
    # Print rows
    for from_node in all_nodes:
        print(f"{from_node:<12}", end="")
        for to_node in all_nodes:
            if from_node == to_node:
                print(f"{'-':<15}", end="")
            else:
                key = f"{from_node}->{to_node}"
                bandwidth = all_results.get(key, "N/A")
                print(f"{bandwidth:<15}", end="")
        print()
    
    print()
    print("=" * 60)
    print("Latency Matrix (seconds)")
    print("=" * 60)
    print()
    
    # Print latency header
    print(f"{'From\\To':<12}", end="")
    for node in all_nodes:
        print(f"{node:<15}", end="")
    print()
    
    # Print latency rows
    for from_node in all_nodes:
        print(f"{from_node:<12}", end="")
        for to_node in all_nodes:
            if from_node == to_node:
                print(f"{'-':<15}", end="")
            else:
                key = f"{from_node}->{to_node}"
                latency = all_latencies.get(key, "N/A")
                print(f"{latency:<15}", end="")
        print()
    
    print()
    print("=" * 60)
    print("Summary Statistics")
    print("=" * 60)
    print()
    
    total_measurements = len(all_nodes) * (len(all_nodes) - 1)
    successful = sum(1 for v in all_results.values() if v != "N/A")
    failed = total_measurements - successful
    
    print(f"Total possible measurements: {total_measurements}")
    print(f"Successful: {successful}")
    print(f"Failed/Missing: {failed}")
    print(f"Success rate: {successful/total_measurements*100:.1f}%")
    
    print()
    print("=" * 60)
    print("Detailed Results by Node")
    print("=" * 60)
    print()
    
    for pod in pods:
        node = get_pod_node(pod)
        if not node:
            continue
        print(f"--- {node} ({pod}) ---")
        logs = run_cmd(f"sudo k3s kubectl logs {pod} 2>&1")
        # Extract results section
        lines = logs.split('\n')
        in_results = False
        for line in lines:
            if "=== Results ===" in line:
                in_results = True
            if in_results:
                print(line)
                if line.strip() == "" and in_results:
                    break
        print()

if __name__ == "__main__":
    main()
