.PHONY: up down status logs-seaweed logs-flink logs-hms logs-spark

PWD := $(shell pwd)
MANIFESTS_DIR := $(PWD)/k8s
HELM_DIR := $(PWD)/helm
NAMESPACE := data-platform
JOBS_DIR := $(PWD)/jobs
AIRFLOW_FLINK_DAG_CONFIGMAP := airflow-dags

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
	@echo "Deploying Apache Kafka..."
	kubectl apply -f $(MANIFESTS_DIR)/kafka.yaml
	kubectl apply -f $(MANIFESTS_DIR)/kafka-ui.yaml
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
	kubectl delete -f $(MANIFESTS_DIR)/kafka.yaml --ignore-not-found
	kubectl delete -f $(MANIFESTS_DIR)/kafka-ui.yaml --ignore-not-found

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

airflow-start:
	@echo "Starting Airflow webserver and scheduler..."
	helm upgrade \
		--install airflow apache-airflow/airflow\
		--namespace $(NAMESPACE)\
		--create-namespace\
		--values $(HELM_DIR)/airflow-values.yaml
	@echo "Airflow webserver and scheduler started successfully."

airflow-stop:
	@echo "Stopping Airflow webserver and scheduler..."
	helm uninstall airflow --namespace $(NAMESPACE)
	@echo "Airflow webserver and scheduler stopped successfully."

spark-start:
	@echo "Starting Spark..."
	helm upgrade\
		--install spark-operator spark-operator/spark-operator\
		--namespace $(NAMESPACE)\
		--create-namespace\
		--values $(HELM_DIR)/spark-operator-values.yaml

spark-stop:
	@echo "Stopping Spark..."
	helm uninstall spark-operator --namespace $(NAMESPACE)
	@echo "Spark stopped successfully."

mount-jobs:
	@echo "Mounting DAGs to Airflow..."
	minikube mount $(JOBS_DIR):/mnt/jobs