from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.models import Variable
from docker.types import Mount
from airflow.utils.dates import days_ago
from airflow.sensors.sql_sensor import SqlSensor

from config import dockers

default_args = {
    "owner": "airflow",
    "start_date": days_ago(2),
    "schedule_interval": "@once",
    "catchup": False,
}

obspy_image = "seisblue/airflow_obspy:20230723"

with DAG(dag_id="05_associator", default_args=default_args, tags=["test"]) as dag:
    # check_db_exists = SqlSensor(
    #     task_id='check_db_exists',
    #     conn_id='mysql_conn',
    #     sql='SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = "testdb"',
    # )
    add_assocpicks = DockerOperator(
        task_id="add_assocpicks",
        image=obspy_image,
        command=[
            "python",
            "/tmp/process_assocpicks.py",
            "--data_config_filepath",
            Variable.get("data_config_filepath"),
        ],
        **dockers.dockerops_kwargs,
    )
    get_candidate_events = DockerOperator(
        task_id="get_candidate_events",
        image=obspy_image,
        command=[
            "python",
            "/tmp/process_candidate_events.py",
            "--data_config_filepath",
            Variable.get("data_config_filepath"),
        ],
        **dockers.dockerops_kwargs,
    )

    add_assocpicks >> get_candidate_events
