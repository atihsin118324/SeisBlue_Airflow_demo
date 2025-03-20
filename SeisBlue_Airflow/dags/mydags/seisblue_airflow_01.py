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
}

obspy_image = "seisblue/airflow_obspy:20230723"

with DAG(dag_id="01_process_metadata", default_args=default_args, tags=["test"]) as dag:
    add_inv = DockerOperator(
        task_id="add_inventory",
        image=obspy_image,
        command=[
            "python",
            "/tmp/process_inventory.py",
            "--data_config_filepath",
            Variable.get("data_config_filepath"),
        ],
        **dockers.dockerops_kwargs
    )
    add_evt_pick = DockerOperator(
        task_id="add_event_and_pick",
        image=obspy_image,
        command=[
            "python",
            "/tmp/process_event.py",
            "--data_config_filepath",
            Variable.get("data_config_filepath"),
        ],
        **dockers.dockerops_kwargs
    )

    [add_inv, add_evt_pick]
