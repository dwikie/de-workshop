from datetime import datetime,  timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s


NAMESPACE = "data-platform"
FLINK_IMAGE = "flink-pyflink:2.2.1"

JOBMANAGER = (
    "flink-jobmanager.data-platform.svc.cluster.local:8081"
)

# This path must exist inside the Minikube node.
NODE_JOBS_PATH = "/mnt/jobs"

# This file must exist at:
# /home/dwiki/project/workshop-de-sv-1/jobs/stream_from_kafka_to_parquet.py
JOB_SCRIPT = "/opt/flink/jobs/stream_from_kafka_to_parquet.py"

flink_volumes = [
    k8s.V1Volume(
        name="jobs",
        host_path=k8s.V1HostPathVolumeSource(
            path=NODE_JOBS_PATH,
            type="Directory",
        ),
    ),
    k8s.V1Volume(
        name="flink-config",
        config_map=k8s.V1ConfigMapVolumeSource(
            name="flink-config",
        ),
    )
]

flink_volume_mounts = [
    k8s.V1VolumeMount(
        name="jobs",
        mount_path="/opt/flink/jobs",
        read_only=True,
    ),
    k8s.V1VolumeMount(
        name="flink-config",
        mount_path="/opt/flink/conf/config.yaml",
        sub_path="config.yaml",
        read_only=True,
    ),
]


with DAG(
    dag_id="stream_from_kafka_to_parquet",
    description="Submit the Kafka-to-Parquet streaming job to Flink",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["flink", "kafka", "seaweedfs"],
) as dag:
    submit_stream_kafka_to_parquet = KubernetesPodOperator(
        task_id="submit_stream_kafka_to_parquet",
        name="submit-stream-kafka-to-parquet",
        namespace=NAMESPACE,
        image=FLINK_IMAGE,
        image_pull_policy="Never",

        # Submit the job to the existing Flink cluster.
        cmds=["flink"],
        arguments=[
            "run",
            "--target", "remote",
            "--jobmanager", JOBMANAGER,
            "--python", JOB_SCRIPT,
            "--detached",
        ],

        volumes=[
            *flink_volumes,
        ],

        volume_mounts=[
            *flink_volume_mounts
        ],
        on_finish_action="delete_pod",
    )

    wait_for_parquet_files = KubernetesPodOperator(
        task_id="wait_for_parquet_files",
        name="wait-for-parquet-files",
        namespace=NAMESPACE,
        image="public.ecr.aws/aws-cli/aws-cli:latest",
        image_pull_policy="IfNotPresent",
        cmds=["/bin/sh", "-c"],
        arguments=[
            """
            set -eu

            BUCKET="sink"
            PREFIX="transactions/"
            ENDPOINT="http://seaweedfs.data-platform.svc.cluster.local:8333"
            TIMEOUT=300
            INTERVAL=10
            ELAPSED=0

            echo "Waiting for committed Parquet files..."

            while [ "$ELAPSED" -lt "$TIMEOUT" ]; do
                if COUNT=$(aws --endpoint-url "$ENDPOINT" \
                    s3api list-objects-v2 \
                    --bucket "$BUCKET" \
                    --prefix "$PREFIX" \
                    --query "length(Contents[?ends_with(Key, '.parquet')])" \
                    --output text 2>/dev/null); then

                    case "$COUNT" in
                        ''|None|null) COUNT=0 ;;
                    esac

                    if [ "$COUNT" -gt 0 ] 2>/dev/null; then
                        echo "Found ${COUNT} Parquet file(s)."
                        exit 0
                    fi
                else
                    echo "Could not list S3 objects; checking again..."
                fi

                echo "No Parquet files yet; elapsed=${ELAPSED}s"
                sleep "$INTERVAL"
                ELAPSED=$((ELAPSED + INTERVAL))
            done

            echo "Timed out waiting for Parquet files."
            exit 1
            """
        ],
        env_vars={
            "AWS_DEFAULT_REGION": "us-east-1",
            "AWS_ACCESS_KEY_ID": "dev_key",
            "AWS_SECRET_ACCESS_KEY": "dev_secret",
        },
        execution_timeout=timedelta(minutes=6),
        on_finish_action="delete_pod",
    )

    process_transactions = KubernetesPodOperator(
        task_id="process_transactions",
        name="process-transactions",
        namespace=NAMESPACE,
        image=FLINK_IMAGE,
        image_pull_policy="Never",

        cmds=["flink"],
        arguments=[
            "run",
            "--target", "remote",
            "--jobmanager", JOBMANAGER,
            "--python", "/opt/flink/jobs/process_stream_from_kafka.py",
        ],

        volumes=[
            *flink_volumes,
        ],

        volume_mounts=[
            *flink_volume_mounts
        ],

        execution_timeout=timedelta(minutes=30),
        on_finish_action="delete_pod",
    )



submit_stream_kafka_to_parquet >> wait_for_parquet_files
wait_for_parquet_files >> process_transactions