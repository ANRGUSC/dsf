#!/usr/bin/env bash
set -e
echo "Host IP: $NODE_IP"
echo "Node Name: $NODE_NAME"

if [[ "$1" == "-s" ]]; then
    while true; do
        mkdir -p /pathload_results
        ./pathload_snd >> /pathload_results/snd.txt &
        wait
        echo "=== Results from $NODE_NAME ($NODE_IP) ==="
        cat /pathload_results/snd.txt
    done
elif [[ "$1" == "-r" ]]; then
    mkdir -p /pathload_results
    IFS=',' read -ra IP_ARRAY <<< "$ALL_IPS"
    while true; do
        for ip in "${IP_ARRAY[@]}"; do
            if [[ "$ip" != "$NODE_IP" ]]; then
                ./pathload_rcv -s $ip | tail -3 >> /pathload_results/rcv_$ip.txt &
                wait
                cat /pathload_results/rcv_$ip.txt
                sleep 10
            fi
        done
    done
else
    echo "Usage: $0 -s|-r [options]"
    exit 1
fi
