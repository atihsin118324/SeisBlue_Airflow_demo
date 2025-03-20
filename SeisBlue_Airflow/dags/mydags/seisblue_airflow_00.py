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
torch_image = "seisblue/airflow_torch:20230723"

with DAG(
    dag_id="00_pick_and_associate", default_args=default_args, tags=["test"]
) as dag:
    add_inv = DockerOperator(
        task_id="add_inventory",
        image=obspy_image,
        command=[
            "python",
            "/tmp/process_inventory.py",
            "--data_config_filepath",
            Variable.get("data_config_filepath"),
        ],
        **dockers.dockerops_kwargs,
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
        **dockers.dockerops_kwargs,
    )
    get_train_instance = DockerOperator(
        task_id=f"get_train_instance",
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
        task_id=f"get_test_instance",
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
    training = DockerOperator(
        task_id="training",
        image=torch_image,
        command=[
            "python3",
            "/tmp/training.py",
            "--config_filepath",
            Variable.get("model_config_filepath"),
        ],
        **dockers.dockerops_kwargs,
    )
    testing = DockerOperator(
        task_id="testing",
        image=torch_image,
        command=[
            "python3",
            "/tmp/testing.py",
            "--config_filepath",
            Variable.get("model_config_filepath"),
        ],
        **dockers.dockerops_kwargs,
    )
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

    (
        [add_inv, add_evt_pick]
        >> get_train_instance
        >> get_test_instance
        >> training
        >> testing
        >> add_assocpicks
        >> get_candidate_events
    )
