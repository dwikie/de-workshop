
from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

flink_config_volume = k8s.V1Volume(
    name="flink-config",
    config_map=k8s.V1ConfigMapVolumeSource(
        name="flink-config",
        items=[
            k8s.V1KeyToPath(
                key="config.yaml",
                path="config.yaml",
            )
        ],
    ),
)

flink_config_mount = k8s.V1VolumeMount(
    name="flink-config",
    mount_path="/opt/flink/conf/config.yaml",
    sub_path="config.yaml",
    read_only=True,
)

with DAG(
    dag_id="submit_pyflink_word_count",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["flink", "pyflink"],
) as dag:
    submit_word_count = KubernetesPodOperator(
        task_id="submit_word_count",
        name="submit-pyflink-word-count",
        namespace="data-platform",
        image="flink-pyflink:2.2.1",
        image_pull_policy="IfNotPresent",
        cmds=["flink"],
        arguments=[
            "run",
            "--target", "remote",
            "--jobmanager",
            "flink-jobmanager.data-platform.svc.cluster.local:8081",
            "--python",
            "/opt/flink/examples/python/datastream/streaming_word_count.py",
        ],
        volumes=[flink_config_volume],
        volume_mounts=[flink_config_mount],
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        do_xcom_push=False,
    )
