# Pushing Scheduler Image to Registry

## Option 1: Docker Hub (Simplest)

1. **Create Docker Hub account** (if you don't have one):
   - Go to https://hub.docker.com
   - Sign up for free account

2. **Login to Docker Hub**:
   ```bash
   docker login
   # Enter your Docker Hub username and password
   ```

3. **Tag and push the image**:
   ```bash
   cd /home/anrg/dsf/phase-2/cmd/scheduler
   
   # Tag with your Docker Hub username
   docker tag ctg-scheduler:latest <your-dockerhub-username>/ctg-scheduler:latest
   
   # Push to Docker Hub
   docker push <your-dockerhub-username>/ctg-scheduler:latest
   ```

4. **Update deployment.yml**:
   ```yaml
   image: <your-dockerhub-username>/ctg-scheduler:latest
   imagePullPolicy: Always  # or IfNotPresent
   ```

5. **Apply the updated deployment**:
   ```bash
   kubectl apply -f deployment.yml
   ```

## Option 2: GitHub Container Registry

1. **Create GitHub Personal Access Token**:
   - Go to GitHub Settings → Developer settings → Personal access tokens
   - Create token with `write:packages` permission

2. **Login and push**:
   ```bash
   echo $GITHUB_TOKEN | docker login ghcr.io -u <your-github-username> --password-stdin
   
   docker tag ctg-scheduler:latest ghcr.io/<your-github-username>/ctg-scheduler:latest
   docker push ghcr.io/<your-github-username>/ctg-scheduler:latest
   ```

3. **Update deployment.yml**:
   ```yaml
   image: ghcr.io/<your-github-username>/ctg-scheduler:latest
   imagePullPolicy: Always
   ```

## Option 3: Local Registry (No Online Account)

See `LOCAL_REGISTRY.md` for setting up a local registry.
