docker buildx build \
  --platform linux/arm64 \
  -t mohammadalikh/random-scheduler:latest \
  --push .

go mod tidy

docker buildx build -t mohammadalikh/random-scheduler:latest --push .

kubectl apply -f deployment.yml
kubectl delete -f deployment.yml
kubectl rollout restart deployment/random-scheduler


kubectl apply -f test-pod.yml
kubectl delete -f test-pod.yml

kubectl -n kube-system logs -l app=random-scheduler -f

kubectl get pod rand-test-1 -o wide


# See node resource usage
kubectl top nodes

# See pod resource usage  
kubectl top pods

# See pod resource usage in specific namespace
kubectl top pods -n kube-system