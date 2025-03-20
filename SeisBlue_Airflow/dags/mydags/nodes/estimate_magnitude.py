from obspy.signal.invsim import estimate_magnitude
import numpy as np
from scipy.signal import peak_prominences
import math
import argparse
import itertools
import h5py

from seisblue import SQL, tool, utils


def calculate_distance(location1, location2):
    lat1, lon1 = location1
    lat2, lon2 = location2
    radius = 6371

    dlat = math.radians(lat2-lat1)
    dlon = math.radians(lon2-lon1)
    a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) \
        * math.cos(math.radians(lat2)) * math.sin(dlon/2) * math.sin(dlon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = radius * c

    return d


def calculate_magnitude(event, paz=None):
    waveforms = client.get_waveform(time=event.time)
    waveforms = sorted(waveforms, key=lambda waveform: waveform.starttime)
    waveform_groupby = itertools.groupby(
        waveforms, key=lambda waveform: [waveform.station]
    )

    magnitude_by_sta = []
    for (key, waveforms) in waveform_groupby:
        inventory = client.get_inventory(station=key[0])
        distance = calculate_distance((inventory.latitude, inventory.longitude), (event.latitude, event.longitude))
        for waveform in waveforms:
            with h5py.File(waveform.datasetpath, "r") as f:
                for instance in f.values():
                    instance = tool.read_hdf5(instance)
                    npts = instance.timewindow.npts
                    for trace in instance.traces:
                        x = trace.data
                        peak_index = np.array([np.argmax(x)])
                        prominence, left_bases, right_bases = peak_prominences(x, peak_index)
                        timespan = (right_bases - peak_index)/npts
                        amplitude = prominence / 2
                        # print(amplitude[0], timespan[0], distance)
                    if not paz:
                        paz = {'poles': [-21.99 + 22.44j, -21.99 - 22.44j],
                               'zeros': [0 + 0j, 0 + 0j],
                               'gain': 1, 'sensitivity': 76.7}

                    mag = estimate_magnitude(paz, amplitude, timespan, distance)
                    magnitude_by_sta.append(mag)
    if len(magnitude_by_sta) > 0:
        mean_mag = sum(magnitude_by_sta) / len(magnitude_by_sta)
        print(event.magnitude, mean_mag)
    return event


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_config_filepath", type=str, required=True)
    args = parser.parse_args()
    config = tool.read_yaml(args.data_config_filepath)
    c = config['process_magnitude']
    database = config['global']['database']

    event_filter = c['event_filter']

    print("Start estimating magnitude.")
    client = SQL.Client(database)
    events = client.get_event(**event_filter)
    print(f"Get {len(events)} events.")
    for event in events:
        event = calculate_magnitude(event)

    events = utils.parallel(events, func=calculate_magnitude)
    [client.add_magnitude_into_event(event) for event in events]
    print(f'Update {len(events)} events with magnitude in database.')

 





