#!/bin/bash
# Deploy receiver on node 4 (anrg-4)
# Run this script on node 4

cd /home/anrg/dsf/phase-1/net-metrics/pathload/receiver

# Build the image
docker build -f Dockerfile -t pathload-receiver:latest .

# Stop and remove existing container
docker stop pathload-receiver 2>/dev/null
docker rm pathload-receiver 2>/dev/null

# Run receiver container
docker run -d --name pathload-receiver --network host pathload-receiver:latest

echo "Receiver deployed. Check logs with: docker logs pathload-receiver"
