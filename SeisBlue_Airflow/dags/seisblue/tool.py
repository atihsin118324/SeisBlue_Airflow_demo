from seisblue import core

import h5py
import numpy as np
import json
from types import SimpleNamespace
from datetime import datetime
import dataclasses
import yaml


def remove_none_values(data):
    if isinstance(data, dict):
        return {key: remove_none_values(value) for key, value in data.items() if value is not None}
    elif isinstance(data, list):
        return [remove_none_values(item) for item in data if item is not None]
    else:
        return data


class EnhancedEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, np.integer):
            return int(obj)
        elif dataclasses.is_dataclass(obj):
            return dataclasses.asdict(obj)
        else:
            return super().default(obj)


class EnhancedDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(self, object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, dct):
        for key, value in dct.items():
            if isinstance(value, list) and len(value) > 0 and isinstance(value[0], (int, float)):
                dct[key] = np.array(value)
            elif isinstance(value, str):
                try:
                    dct[key] = datetime.fromisoformat(value)
                except ValueError:
                    pass
        return dct


def to_json(obj):
    return json.dumps(obj, cls=EnhancedEncoder)


def from_json(obj):
    return json.loads(obj, object_hook=lambda d: SimpleNamespace(**d))


def to_dict(obj):
    return {k: v for k, v in dataclasses.asdict(obj).items()}


def read_hdf5(instance):
    traces = []
    labels = []
    id = instance.attrs['id']
    timewindow = core.TimeWindow(
        starttime=datetime.fromisoformat(instance['timewindow'].attrs['starttime']),
        endtime=datetime.fromisoformat(instance['timewindow'].attrs['endtime']),
        npts=instance['timewindow'].attrs['npts'],
        delta=instance['timewindow'].attrs['delta'],
        samplingrate=instance['timewindow'].attrs[
            'samplingrate'],
    )
    inventory = core.Inventory(
        network=instance['inventory'].attrs['network'],
        station=instance['inventory'].attrs['station'],
    )

    for trace_h5 in instance['traces'].values():
        trace = core.Trace(
            channel=trace_h5.attrs['channel'],
            data=np.array(trace_h5['data']),
        )
        traces.append(trace)

    for label_h5 in instance['labels'].values():
        picks = []
        if 'pick' in label_h5.keys():
            for pick_h5 in label_h5['picks'].values():
                pick = core.Pick(
                    time=pick_h5.attrs['time'],
                    phase=pick_h5.attrs['phase'],
                    tag=pick_h5.attrs['tag']
                )
                picks.append(pick)
        label = core.Label(
            inventory=inventory,
            timewindow=timewindow,
            picks=picks,
            phase=label_h5.attrs['phase'],
            tag=label_h5.attrs['tag'],
            data=np.array(label_h5['data'])
        )
        labels.append(label)

    instance = core.Instance(
        inventory=inventory,
        timewindow=timewindow,
        traces=traces,
        labels=labels,
        id=id,
    )
    return instance


def write_hdf5_layer(group, dictionary):
    """
    Write HDF5 dynamically.
    :param h5py._hl.group.Group group: group
    :param dict[any, any] dictionary:
    """
    for key, value in dictionary.items():
        try:
            if value is None:
                continue
            elif isinstance(value, dict):
                sub_group = group.create_group(key)
                write_hdf5_layer(sub_group, value)
            elif isinstance(value, list):
                sub_group = group.create_group(key)
                for i, item in enumerate(value):
                    min_group = sub_group.create_group(f'{key}{i}')
                    write_hdf5_layer(min_group, item)
            elif isinstance(value, np.ndarray):
                group.create_dataset(key, data=value)
            elif isinstance(value, (int, float, str, np.integer)):
                if isinstance(value, np.integer):
                    value = value.item()
                group.attrs.create(key, value)
            else:
                group.attrs.create(key, value.isoformat())
        except Exception as e:
            print(f"key={key}, value={value}, error={e}")


def write_hdf5(instances, **kwargs):
    """
    Add instances into HDF5 file.
    :param core.Instance instances: List of instances.
    :param list filepath: hdf5
    """
    filepath = instances[0].datasetpath
    with h5py.File(filepath, 'w') as f:
        for i, instance in enumerate(instances):
            HDFdict = to_dict(instance)
            if HDFdict['id'] not in f.keys():
                instance_group = f.create_group(HDFdict['id'])
                write_hdf5_layer(instance_group, HDFdict)


def merge_hdf5_files(file_list, merged_file_name):
    merged_file = h5py.File(merged_file_name, 'w')

    for file_name in file_list:
        with h5py.File(file_name, 'r') as file:
            for dataset_name, dataset in file.items():
                if dataset_name not in merged_file:
                    dataset_shape = dataset.shape
                    dataset_dtype = dataset.dtype
                    dataset = merged_file.create_dataset(dataset_name, shape=dataset_shape, dtype=dataset_dtype)

                dataset[:] = file[dataset_name][:]

    merged_file.close()


def _print_attrs(name, obj):
    print(name)
    for key, val in obj.attrs.items():
        print("    %s: %s" % (key, val))


def inspect_hdf5(filepath, **kwargs):
    f = h5py.File(filepath, 'r')
    if 'id' in kwargs.keys():
        f['id'].visititems(_print_attrs)
    else:
        f.visititems(_print_attrs)


def read_yaml(filepath):
    with open(filepath, 'r') as file:
        return yaml.safe_load(file)



if __name__ == '__main__':
    pass
