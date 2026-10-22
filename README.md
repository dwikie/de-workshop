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
minikube image build -t flink-pyflink:2.3.0 . -f Flink.Dockerfile
```

### Step 2: Spin Up Infrastructure
Deploy the local environment by running the following command in your terminal:

```bash
make up
```

### Step 3: Data Ingestion (ETL)
Open `etl.ipynb` in your environment and follow the instructions to ingest raw data `from s3://source_data` and stream/dump it into the warehouse using Flink.

### Step 4: Data Processing and Analytics
Open `analytics.ipynb` and follow the guided tasks to query the warehouse via PySpark, extract business metrics, and derive actionable insights.# de-workshop
