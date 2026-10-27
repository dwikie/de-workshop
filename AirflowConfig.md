### Generate Airflow default values
```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update

helm show values apache-airflow/airflow --version 1.22.0 > helm/airflow-values.yaml
```
> You can change the config values of airflow that you desired from helm/airflow-values.yaml

### Validate the values of airflow helm manifest
```baah
helm template airflow apache-airflow/airflow \
  --namespace data-platform \
  --version 1.22.0 \
  -f helm/airflow-values.yaml > helm/airflow-rendered.yaml
```
> If rendering succeeds, go to next step to install Airflow

### Installing Apache Airflow
```bash
helm upgrade --install airflow apache-airflow/airflow \
  --namespace data-platform \
  --version 1.22.0 \
  -f airflow-values.yaml \
  --timeout 15m
```