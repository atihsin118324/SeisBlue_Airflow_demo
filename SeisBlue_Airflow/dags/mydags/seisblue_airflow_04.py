from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

from config import dockers

default_args = {
    "owner": "airflow",
    "start_date": days_ago(2),
    "schedule_interval": "@once",
    "catchup": False,
    "xcom_push": True,
}

torch_image = "seisblue/airflow_torch:20230723"

with DAG(dag_id="04_testing", default_args=default_args, tags=["test"]) as dag:
    testing = DockerOperator(
        task_id="testing",
        image=torch_image,
        command=[
            "python3",
            "/tmp/testing.py",
            "--model_config_filepath",
            Variable.get("model_config_filepath"),
            "--data_config_filepath",
            Variable.get("data_config_filepath"),
        ],
        **dockers.dockerops_kwargs
    )
    testing
