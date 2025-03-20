from airflow.models import Variable
from docker.types import Mount

from seisblue.tool import read_yaml


config = read_yaml(Variable.get('mnt_config_filepath'))

dockerops_kwargs = {
    'api_version': 'auto',
    'auto_remove': True,
    'docker_url': "unix://var/run/docker.sock",
    'mount_tmp_dir': False,
    'network_mode': "bridge",
    'xcom_all': False,
    'mounts': [
        Mount(
            source=config['dirpath']['waveforms'],
            target='/mnt/waveforms',
            read_only=True,
            type='bind',
        ),
        Mount(
            source=config['dirpath']['events'],
            target='/mnt/events',
            read_only=True,
            type='bind',
        ),
        Mount(
            source=Variable.get('workspace') + '/dags/mydags/nodes',
            target='/tmp',
            type='bind',
        ),
        Mount(
            source=Variable.get('workspace') + '/dags/seisblue',
            target='/tmp/seisblue',
            type='bind',
        ),
        Mount(
            source=Variable.get('workspace') + '/dags/dataset',
            target='/tmp/dataset',
            type='bind',
        ),
        Mount(
            source=Variable.get('workspace') + '/dags/config',
            target='/tmp/config',
            type='bind',
        ),
        Mount(
            source=Variable.get('workspace') + '/dags/misc',
            target='/tmp/misc',
            type='bind',
        ),
    ],
}