#!/bin/bash
# Deploy sender on node 3 (anrg-3)
# Usage: ./deploy-sender.sh <receiver_ip>
# Example: ./deploy-sender.sh 192.168.1.156

RECEIVER_IP=${1:-192.168.1.156}

cd /home/anrg/dsf/phase-1/net-metrics/pathload/sender

# Build the image
docker build -f Dockerfile -t pathload-sender:latest .

# Stop and remove existing container
docker stop pathload-sender 2>/dev/null
docker rm pathload-sender 2>/dev/null

# Run sender container
docker run -d --name pathload-sender --network host pathload-sender:latest pathload_snd $RECEIVER_IP

echo "Sender deployed. Check logs with: docker logs pathload-sender"
