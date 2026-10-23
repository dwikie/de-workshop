.PHONY: up down status logs-seaweed logs-flink logs-hms logs-spark

PWD := $(shell pwd)
MANIFESTS_DIR := $(PWD)/k8s
NAMESPACE := data-platform

up:
	@echo "Creating namespace..."
	kubectl apply -f $(MANIFESTS_DIR)/namespace.yaml
	@echo "Deploying SeaweedFS..."
	kubectl apply -f $(MANIFESTS_DIR)/seaweedfs.yaml
	@echo "Deploying PostgreSQL..."
	kubectl apply -f $(MANIFESTS_DIR)/postgres.yaml
	@echo "Deploying Hive Metastore..."
	kubectl apply -f $(MANIFESTS_DIR)/hms.yaml
	@echo "Deploying Apache Flink..."
	kubectl apply -f $(MANIFESTS_DIR)/flink.yaml
	@echo "\nWaiting for workloads to become ready..."
	kubectl rollout status deployment -n $(NAMESPACE) --timeout=180s || true
	kubectl wait --for=condition=ready pod --all -n $(NAMESPACE) --timeout=180s
	@echo "\nAll services are up and running!"

down:
	@echo "Tearing down infrastructure..."
	kubectl delete -f $(MANIFESTS_DIR)/flink.yaml --ignore-not-found
	kubectl delete -f $(MANIFESTS_DIR)/postgres.yaml --ignore-not-found
	kubectl delete -f $(MANIFESTS_DIR)/hms.yaml --ignore-not-found
	kubectl delete -f $(MANIFESTS_DIR)/seaweedfs.yaml --ignore-not-found
	kubectl delete -f $(MANIFESTS_DIR)/namespace.yaml --ignore-not-found

copy-jobs:
	@JOB_MANAGER_POD=$$(kubectl get pods -n $(NAMESPACE) -l app=flink,component=jobmanager -o jsonpath='{.items[0].metadata.name}'); \
	kubectl cp ./jobs $$JOB_MANAGER_POD:/opt/flink -n $(NAMESPACE)

submit-py-job:
	@if [ -z "$(JOB_NAME)" ]; then \
		echo "Error: JOB_NAME is required."; \
		echo "Usage: make submit-py-job JOB_NAME=<your_script.py>"; \
		exit 1; \
	fi; \
	JOB_MANAGER_POD=$$(kubectl get pods -n $(NAMESPACE) -l app=flink,component=jobmanager -o jsonpath='{.items[0].metadata.name}'); \
	kubectl exec -n $(NAMESPACE) $$JOB_MANAGER_POD -- /opt/flink/bin/flink run -py /opt/flink/jobs/$(JOB_NAME)