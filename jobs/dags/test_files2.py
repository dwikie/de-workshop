from airflow import DAG
from datetime import datetime

with DAG(
    dag_id="my_new_dag_v2",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    pass