## DSF — Distributed Scheduling Framework
## All commands are run from the repo root.
##
## Registry: 192.168.1.163:5000 (local k3s registry on master node)
## Cluster:  k3s, master = anrg-2, workers = anrg-{1,3..9}

REGISTRY   ?= 192.168.1.163:5000
NAMESPACE  ?= default
GOFLAGS    ?=

.PHONY: all build ui-build test \
        image-odag-controller image-cdag-controller image-ui-server \
        image-examples push-all \
        install deploy rollout \
        example-odag example-cdag clean-examples clean-deploy \
        help

# ─── default ──────────────────────────────────────────────────────────────────

all: build ui-build

# ─── Go build ─────────────────────────────────────────────────────────────────

## Build all Go binaries into bin/
build:
	mkdir -p bin
	go build $(GOFLAGS) -o bin/odag-controller ./cmd/odag-controller
	go build $(GOFLAGS) -o bin/cdag-controller ./cmd/cdag-controller
	go build $(GOFLAGS) -o bin/ui-server      ./cmd/ui-server
	go build $(GOFLAGS) -o bin/dsf            ./cmd/cli

## Build the React UI into ui/dist/
ui-build:
	cd ui && npm ci && npm run build

## Run Go unit tests
test:
	go test $(GOFLAGS) ./...

## Run Go tests with verbose output
test-v:
	go test -v $(GOFLAGS) ./...

# ─── Docker images ────────────────────────────────────────────────────────────

## Build the odag-controller image
image-odag-controller:
	docker build -f cmd/odag-controller/Dockerfile \
		-t $(REGISTRY)/odag-controller:latest .

## Build the cdag-controller image
image-cdag-controller:
	docker build -f cmd/cdag-controller/Dockerfile \
		-t $(REGISTRY)/cdag-controller:latest .

## Build the ui-server image (includes the compiled React frontend)
image-ui-server:
	docker build -f cmd/ui-server/Dockerfile \
		-t $(REGISTRY)/ui-server:latest .

## Build all example task images
image-examples:
	docker build -f examples/dag-pipeline/tasks/generate/Dockerfile \
		-t $(REGISTRY)/dag-pipeline-generate:latest .
	docker build -f examples/dag-pipeline/tasks/transform/Dockerfile \
		-t $(REGISTRY)/dag-pipeline-transform:latest .
	docker build -f examples/dag-pipeline/tasks/output/Dockerfile \
		-t $(REGISTRY)/dag-pipeline-output:latest .
	docker build -f examples/pipeline-ctg/tasks/producer/Dockerfile \
		-t $(REGISTRY)/pipeline-ctg-producer:latest .
	docker build -f examples/pipeline-ctg/tasks/processor/Dockerfile \
		-t $(REGISTRY)/pipeline-ctg-processor:latest .
	docker build -f examples/pipeline-ctg/tasks/sink/Dockerfile \
		-t $(REGISTRY)/pipeline-ctg-sink:latest .

## Build and push all images (controllers + UI + examples) to the local registry
push-all: image-odag-controller image-cdag-controller image-ui-server image-examples
	docker push $(REGISTRY)/odag-controller:latest
	docker push $(REGISTRY)/cdag-controller:latest
	docker push $(REGISTRY)/ui-server:latest
	docker push $(REGISTRY)/dag-pipeline-generate:latest
	docker push $(REGISTRY)/dag-pipeline-transform:latest
	docker push $(REGISTRY)/dag-pipeline-output:latest
	docker push $(REGISTRY)/pipeline-ctg-producer:latest
	docker push $(REGISTRY)/pipeline-ctg-processor:latest
	docker push $(REGISTRY)/pipeline-ctg-sink:latest

## Build and push only the control-plane images (no examples)
push-controllers: image-odag-controller image-cdag-controller image-ui-server
	docker push $(REGISTRY)/odag-controller:latest
	docker push $(REGISTRY)/cdag-controller:latest
	docker push $(REGISTRY)/ui-server:latest

# ─── Cluster install / deploy ─────────────────────────────────────────────────

## Install CRDs, namespace, and RBAC into the cluster
install:
	kubectl apply -f api/v1/odag-crd.yml
	kubectl apply -f api/v1/cdag-crd.yml
	kubectl apply -f deployments/namespace.yml
	kubectl apply -f deployments/odag-controller/rbac.yml
	kubectl apply -f deployments/cdag-controller/rbac.yml
	kubectl apply -f deployments/ui-server/rbac.yml

## Deploy the control plane (odag-controller, cdag-controller, ui-server)
deploy:
	kubectl apply -f deployments/odag-controller/deployment.yml
	kubectl apply -f deployments/cdag-controller/deployment.yml
	kubectl apply -f deployments/ui-server/deployment.yml
	kubectl apply -f deployments/ui-server/service.yml

## Force-restart all control-plane deployments (picks up new :latest images)
rollout:
	kubectl rollout restart deployment/odag-controller  -n dsf-system
	kubectl rollout restart deployment/cdag-controller  -n dsf-system
	kubectl rollout restart deployment/ui-server        -n dsf-system
	kubectl rollout status  deployment/odag-controller  -n dsf-system
	kubectl rollout status  deployment/cdag-controller  -n dsf-system
	kubectl rollout status  deployment/ui-server        -n dsf-system

# ─── Examples ─────────────────────────────────────────────────────────────────

## Submit the one-shot dag-pipeline example
example-odag:
	kubectl apply -f examples/dag-pipeline/dag.yml

## Submit the continuous pipeline-ctg example
example-cdag:
	kubectl apply -f examples/pipeline-ctg/ctg.yml

## Delete both examples from the cluster
clean-examples:
	-kubectl delete -f examples/dag-pipeline/dag.yml  --ignore-not-found
	-kubectl delete -f examples/pipeline-ctg/ctg.yml  --ignore-not-found

# ─── Cleanup ──────────────────────────────────────────────────────────────────

## Delete all DSF control-plane resources (keeps CRDs and namespace)
clean-deploy:
	-kubectl delete -f deployments/odag-controller/deployment.yml --ignore-not-found
	-kubectl delete -f deployments/cdag-controller/deployment.yml --ignore-not-found
	-kubectl delete -f deployments/ui-server/deployment.yml      --ignore-not-found
	-kubectl delete -f deployments/ui-server/service.yml         --ignore-not-found

## Delete everything including CRDs and namespace (destructive!)
clean-all: clean-examples clean-deploy
	-kubectl delete -f api/v1/odag-crd.yml  --ignore-not-found
	-kubectl delete -f api/v1/cdag-crd.yml  --ignore-not-found
	-kubectl delete -f deployments/namespace.yml --ignore-not-found

# ─── Help ─────────────────────────────────────────────────────────────────────

## Show this help message
help:
	@echo "DSF Makefile targets:"
	@echo ""
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/^## /  /'
	@echo ""
	@echo "Variables:"
	@echo "  REGISTRY=$(REGISTRY)   (override with REGISTRY=... make push-all)"
	@echo "  NAMESPACE=$(NAMESPACE)"
