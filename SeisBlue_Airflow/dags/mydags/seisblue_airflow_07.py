from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.models import Variable
from docker.types import Mount
from airflow.utils.dates import days_ago

from config import dockers


default_args = {
    "owner": "airflow",
    "start_date": days_ago(2),
    "schedule_interval": "@once",
    "catchup": False,
}

obspy_image = "seisblue/airflow_obspy:20230723"

with DAG(
    dag_id="07_estimate_magnitude", default_args=default_args, tags=["test"]
) as dag:
    estimate_magnitude = DockerOperator(
        task_id="estimate_magnitude",
        image=obspy_image,
        command=[
            "python",
            "/tmp/estimate_magnitude.py",
            "--data_config_filepath",
            Variable.get("data_config_filepath"),
        ],
        **dockers.dockerops_kwargs,
    )
    estimate_magnitude
