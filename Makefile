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

status:
	kubectl get pods,svc -n $(NAMESPACE)

logs-seaweed:
	kubectl logs -n $(NAMESPACE) -l app=seaweedfs --tail=100 -f

logs-flink:
	kubectl logs -n $(NAMESPACE) -l app=flink --tail=100 -f

logs-hms:
	kubectl logs -n $(NAMESPACE) -l app=hive-metastore --tail=100 -f

logs-spark:
	kubectl logs -n $(NAMESPACE) -l app=spark --tail=100 -f

copy-batch:
	@echo "Copying batch job to Flink JobManager pod..."
	@JOB_MANAGER_POD=$$(kubectl get pods -n $(NAMESPACE) -l app=flink,component=jobmanager -o jsonpath='{.items[0].metadata.name}'); \
	kubectl cp ./etl_batch.py $$JOB_MANAGER_POD:/opt/flink/jobs/workshop/etl_batch.py -n $(NAMESPACE)
	@echo "Batch job copied successfully."