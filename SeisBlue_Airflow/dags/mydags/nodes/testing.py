import argparse
import h5py
import torch
import scipy
import obspy
from tqdm import tqdm
from datetime import datetime

from seisblue import SQL, tool, model, plot, core, generator
from seisblue.model import PhaseNet


def get_waveform(database, **kwargs):
    client = SQL.Client(database)
    client.get_instance_filepaths()
    waveforms = client.get_instance_filepaths(**kwargs)

    filepaths = [waveform.datasetpath for waveform in waveforms]
    tool.merge_hdf5_file(filepaths, '/tmp/dataset/test.hdf5')
    instances = generator.get_instance('test.hdf5')
    return instances


def evaluate(model_path, dataloader, learning_rate):
    evaluate_results = []
    model = PhaseNet()
    optimizer = torch.optim.Adam(model.parameters(), lr=eval(learning_rate))
    checkpoint = torch.load(model_path)
    model.load_state_dict(checkpoint['model_state_dict'])
    optimizer.load_state_dict(checkpoint['optimizer_state_dict'])
    model.eval()

    with torch.no_grad():
        for (batch_X, batch_y) in tqdm(dataloader):
            batch_pred_y = model(batch_X)
            evaluate_results.append(batch_pred_y)
    evaluate_results = torch.cat(evaluate_results, dim=0).numpy()

    return evaluate_results


def _get_predict_picks(label, tag, threshold=0.5, distance=100, from_ms=0, to_ms=-1):
    """
    Extract pick from label and write into the database.
    :param float height: Height threshold, from 0 to 1, default is 0.5.
    :param int distance: Distance threshold in data point.
    """
    picks = []
    for i, phase in enumerate(label.phase[0:2]):
        peaks, properties = scipy.signal.find_peaks(
            label.data[from_ms:to_ms, i], height=threshold, distance=distance
        )

        for j, peak in enumerate(peaks):
            if peak:
                pick_time = (
                    obspy.UTCDateTime(label.timewindow.starttime)
                    + (peak + from_ms) * label.timewindow.delta
                )

                picks.append(
                    core.Pick(
                        time=datetime.utcfromtimestamp(pick_time.timestamp),
                        inventory=label.inventory,
                        phase=label.phase[i],
                        tag=tag,
                        confidence=round(float(properties["peak_heights"][j]), 2),
                    )
                )
    label.picks = picks
    return label


def get_instances_with_predict(data, instances, input_filepath, tag='predict', **kwargs):
    picks = []
    for i, instance in enumerate(instances):
        label = core.Label(inventory=instance.inventory,
                           timewindow=instance.timewindow,
                           phase=instance.labels[0].phase,
                           tag=tag,
                           data=data[i].T)
        label = _get_predict_picks(label, tag, **kwargs)
        picks.extend(label.picks)
        instance.labels.append(label)

        with h5py.File(input_filepath, "r+") as f:
            instance_h5 = f[instance.id]
            if 'labels1' in instance_h5['labels'].keys():
                del instance_h5['labels/labels1']
            tool.write_hdf5_layer(instance_h5['labels'], {'labels1': tool.to_dict(label)})
    print(f'Get {len(instances)} evaluated instances and {len(picks)} picks.')
    return instances, picks


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--model_config_filepath", type=str, required=True)
    parser.add_argument("--data_config_filepath", type=str, required=True)
    args = parser.parse_args()
    model_config = tool.read_yaml(args.model_config_filepath)
    data_config = tool.read_yaml(args.data_config_filepath)
    waveform_filter = data_config['process_waveform']['test']['pick_filter']

    database = model_config['global']['database']
    model_filepath = model_config['global']['model_filepath']
    c = model_config['test']
    tag = c['tag']
    loader_parameters = c['loader_parameters']
    evaluate_parameters = c['evaluate_parameters']
    threshold_parameters = c['threshold_parameters']

    print("Start testing.")
    instances = get_waveform(database, **waveform_filter)
    dataset = generator.PickDataset(instances)
    test_loader = generator.get_loaders('test', dataset, **loader_parameters)
    results = evaluate(model_filepath, test_loader, **evaluate_parameters)
    updated_instances, predict_picks = get_instances_with_predict(results, instances, '/tmp/dataset/test.hdf5', tag, **threshold_parameters)

    client = SQL.Client(database)
    client.add_picks(predict_picks)

    if c['plot']:
        plot.plot_dataset(updated_instances[0], save_dir='/tmp/dataset/')
