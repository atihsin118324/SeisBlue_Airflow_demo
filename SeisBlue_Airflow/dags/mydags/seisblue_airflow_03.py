from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
import docker

from config import dockers

default_args = {
    "owner": "airflow",
    "start_date": days_ago(2),
    "schedule_interval": "@once",
    "catchup": False,
    "xcom_push": True,
}

torch_image = "seisblue/airflow_torch:20230723"

with DAG(dag_id="03_training", default_args=default_args, tags=["test"]) as dag:
    training = DockerOperator(
        task_id="training",
        image=torch_image,
        device_requests=[docker.types.DeviceRequest(count=-1, capabilities=[["gpu"]])],
        command=[
            "python3",
            "/tmp/training.py",
            "--config_filepath",
            Variable.get("model_config_filepath"),
        ],
        **dockers.dockerops_kwargs
    )
    training
