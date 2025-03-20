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

obspy_image = "seisblue/airflow_obspy:20230723"

with DAG(dag_id="02_process_data", default_args=default_args, tags=["test"]) as dag:
    get_train_instance = DockerOperator(
        task_id="get_train_instance",
        image=obspy_image,
        command=[
            "python",
            "/tmp/process_waveform.py",
            "--data_config_filepath",
            Variable.get("data_config_filepath"),
            "--mode",
            "train",
        ],
        **dockers.dockerops_kwargs,
    )
    get_test_instance = DockerOperator(
        task_id="get_test_instance",
        image=obspy_image,
        command=[
            "python",
            "/tmp/process_waveform.py",
            "--data_config_filepath",
            Variable.get("data_config_filepath"),
            "--mode",
            "test",
        ],
        **dockers.dockerops_kwargs,
    )
    get_train_instance >> get_test_instance
