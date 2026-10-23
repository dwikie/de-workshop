## Prerequisites

Before starting the hands-on session, ensure you have the following tools installed on your machine:

- **Container Runtime & Orchestration:** [Docker](https://www.docker.com/) and a local Kubernetes cluster ([KIND](https://kind.sigs.k8s.io/), [Minikube](https://minikube.sigs.k8s.io/), [MicroK8s](https://microk8s.io/), or [k3s](https://k3s.io/))
- **Build Tool:** [Make](https://www.gnu.org/software/make/)

> **System Requirements:** Allocate at least 4 vCPUs and 8 GB of RAM to your local Kubernetes container runtime to accommodate the services (SeaweedFS, Flink, Hive Metastore, and Spark).

> **Disclaimer**  
> This session focuses strictly on practical **data engineering workflows** (ingestion, processing, and analytics) rather than Kubernetes platform administration or cluster setup.

---

## Architecture Overview

1. **Storage Layer:** Local [SeaweedFS](https://github.com/seaweedfs/seaweedfs) cluster exposing an S3-compatible API (`s3://source_data`).
2. **Ingestion & Processing:** [Apache Flink](https://flink.apache.org/) to process and land incoming data into the storage layer.
3. **Catalog & Metadata:** [Hive Metastore](https://hive.apache.org/) managing table definitions and schemas.
4. **Analytics Layer:** [PySpark](https://spark.apache.org/docs/latest/api/python/) executing batch queries and feature extraction over the data warehouse.

---

## Hands-on Exercises

### Step 1: Build new Flink Image
Re-build Flink image and install python + apache-flink package:
```bash
minikube image build -f Flink.Dockerfile  -t flink-pyflink:2.2.1 .
```

### Step 2: Spin Up Infrastructure
Deploy the local environment by running the following command in your terminal:

```bash
make up
```

### Step 3: Setting up Kafka
Create new Kafka topic from Kafka UI, open `http://localhost:8084/ui/clusters/local-kafka/all-topics` then click `New Topic` on top-right of the interface, then fill the form with:

- Topic Name: transactions
- Number of Partitions: 3
- Replication Factor: 1

Leave the other options default and then click `Create Topic`

### Step 4: Copy jobs to Flink
Run `make copy-jobs` to copy files from local which is inside `jobs` folder to Flink in order to submit the job later into Flink.

### Step 5: Submit your first Flink Job
run `make submit-py-job JOB_NAME={script.py}` replace `script.py` to the actual file name that already been copied into the Job Manager pod from step 4.