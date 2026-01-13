#!/bin/bash
# Script to push ctg-scheduler to Docker Hub

# Replace YOUR_DOCKERHUB_USERNAME with your actual Docker Hub username
DOCKERHUB_USERNAME="YOUR_DOCKERHUB_USERNAME"

echo "Setting up Docker Hub push for ctg-scheduler..."
echo ""
echo "Repository will be: ${DOCKERHUB_USERNAME}/ctg-scheduler"
echo ""

# Check if logged in
if ! docker info | grep -q "Username"; then
    echo "Please login to Docker Hub first:"
    echo "  docker login"
    echo ""
    read -p "Press Enter after you've logged in..."
fi

# Tag the image
echo "Tagging image..."
docker tag ctg-scheduler:latest ${DOCKERHUB_USERNAME}/ctg-scheduler:latest

# Push the image
echo "Pushing to Docker Hub..."
docker push ${DOCKERHUB_USERNAME}/ctg-scheduler:latest

echo ""
echo "Done! Repository created automatically at:"
echo "  https://hub.docker.com/r/${DOCKERHUB_USERNAME}/ctg-scheduler"
echo ""
echo "Now update deployment.yml with:"
echo "  image: ${DOCKERHUB_USERNAME}/ctg-scheduler:latest"
echo "  imagePullPolicy: Always"
