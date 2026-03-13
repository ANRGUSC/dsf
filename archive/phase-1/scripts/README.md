docker buildx build \
  --platform linux/arm64 \
  -t mohammadalikh/random-controller:latest \
  --push .

go mod tidy

docker buildx build -t mohammadalikh/random-controller:latest --push .

kubectl apply -f deployment.yml
kubectl delete -f deployment.yml
kubectl rollout restart deployment/random-controller -n kube-system


kubectl apply -f test-pod.yml
kubectl delete -f test-pod.yml



# to log the random scheduler
kubectl -n kube-system logs -l app=random-controller -f

kubectl get pod rand-test-1 -o wide


# to delete all pods manually
kubectl delete pod sample-workflow-step1 sample-workflow-step2 sample-workflow-step3 sample-workflow-step4


# See node resource usage
kubectl top nodes

# See pod resource usage  
kubectl top pods

# See pod resource usage in specific namespace
kubectl top pods -n kube-system

# Test dag deployment is in the crd folder -> test-dag.yml

# deploy the test DAG workflow using this command:
kubectl apply -f test-dag.yml

# after deploymnet, you can watch them being placed continuesly using:
kubectl get pods -o wide --watch


# to delete a dag after completion
kubectl delete dag sample-workflow




# full redeployment commands
cd /home/anrg/dsf/phase-1/Schedulers/src/random-controller
docker buildx build -t mohammadalikh/random-controller:latest --push .
kubectl rollout restart deployment/random-controller -n kube-system
kubectl delete dag sample-workflow
kubectl apply -f ../crd/test-dag.yml

# run to limit bandwidth on each node
sudo tc qdisc del dev enp2s0 root 2>/dev/null
sudo tc qdisc add dev enp2s0 root tbf rate 100mbit burst 32kbit latency 400ms
sudo tc qdisc show dev enp2s0











========================================
    5X DATA SIZE TEST RESULTS
========================================

TEST CONFIGURATION:
  - Network Bandwidth: 100 Mbps (TC limits)
  - Same-Node Bandwidth: 160 Mbps 
  - Data Sizes: 250MB, 375MB, 300MB, 500MB (5x larger!)
  - Task Runtime: 10 seconds each

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

HEFT SCHEDULER ⭐

  Actual Makespan:     41 seconds
  Predicted Makespan:  40 seconds
  Prediction Error:    2.5% (EXCELLENT!)
  
  Pod Placement:
    ALL 4 tasks on anrg-2 (same node!)
  
  Strategy:
    ✓ Single-node placement to maximize 160 Mbps same-node bandwidth
    ✓ Avoided 100 Mbps inter-node penalty
    ✓ Sequential execution but with fast local transfers
  
  Why faster with 5x data?
    - With larger data sizes (250-500MB), communication costs dominate
    - HEFT optimized by keeping everything local (160 Mbps)
    - Avoided slow inter-node transfers (100 Mbps)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

RANDOM SCHEDULER

  Status: Pods not created (scheduler issue)
  Cannot compare this run.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

COMPARISON WITH SMALLER DATA:

  Small Data (50-100MB):
    - HEFT: 45s, split across 2 nodes
    - Random: 55s, scattered across 3 nodes
    - HEFT 18% faster
  
  Large Data (250-500MB):
    - HEFT: 41s, ALL on 1 node
    - Prediction accuracy: 2.5%!
    - Single-node strategy optimal with larger transfers

KEY INSIGHT:
  As data sizes increase, HEFT shifts strategy from 
  multi-node parallelism to single-node locality to 
  avoid slow inter-node transfers!

======================================== 
