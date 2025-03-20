from seisblue import core, tool, SQL, utils

import itertools
import argparse
from obspy import UTCDateTime, Stream
from obspy.clients.filesystem import sds
import numpy as np
import collections
import operator
import scipy
from tqdm import tqdm


def get_picks(database, **kwargs):
    client = SQL.Client(database)
    results = client.get_pick(**kwargs)
    picks = [pick.to_dataclass() for pick in results]
    return picks


def _get_time_window(trace_length, anchor_time, shift=0):
    """
    Returns TimeWindow object from anchor time.
    :param int trace_length: Length of trace.
    :param anchor_time: Anchor of the time window.
    :param float or str shift: (Optional.) Shift in sec,
        if 'random' will shift randomly within the trace length.
    :rtype: core.TimeWindow
    :return: TimeWindow object.
    """
    if shift == "random":
        rng = np.random.default_rng()
        shift = rng.random() * (trace_length - 3)
        shift = shift + 3

    time_window = core.TimeWindow(
        starttime=UTCDateTime(anchor_time) - shift,
        endtime=UTCDateTime(anchor_time) - shift + trace_length
    )
    return time_window


def get_waveform_time_windows(picks, trace_length):
    """
    Returns TimeWindow objects of waveform by pick time.
    :param list[core.Pick] picks: List of pick.
    :param int trace_length: Length of trace.
    :rtype: list[core.TimeWindow]
    :return: List of TimeWindow object.
    """
    time_windows = []
    time_window = _get_time_window(trace_length=trace_length,
                                   anchor_time=UTCDateTime(0),
                                   shift="random")
    for pick in picks:
        starttime = time_window.starttime
        endtime = time_window.endtime
        if starttime < pick.time < endtime:
            continue
        elif endtime < pick.time < endtime + 30:
            time_window = _get_time_window(trace_length=trace_length,
                                           anchor_time=starttime + 30)
        else:
            time_window = _get_time_window(trace_length=trace_length,
                                           anchor_time=pick.time,
                                           shift="random")
        time_window.station = pick.inventory.station
        time_windows.append(time_window)
    return time_windows


def get_waveforms(time_windows, waveforms_dir, trim=True, channel="*",
                  fmtstr=None):
    """
    Returns list of stream by reading SDS database.
    :param list[core.TimeWindow] time_windows:
    :param str waveforms_dir:
    :param bool trim:
    :param str channel:
    :param fmtstr:
    :rtype: list[collections.defaultdict[Any, Stream]]
    :return: List of dictionary contains geophone type and streams.
    """
    streams = []
    for time_window in time_windows:
        try:
            station = time_window.station
            starttime = time_window.starttime
            endtime = time_window.endtime + 0.1
            client = sds.Client(sds_root=waveforms_dir)
            if fmtstr:
                client.FMTSTR = fmtstr

            stream = client.get_waveforms(network="*",
                                          station=station,
                                          location="*",
                                          channel=channel,
                                          starttime=starttime,
                                          endtime=endtime)
            if stream:
                if trim:
                    stream.trim(starttime,
                                endtime,
                                pad=True,
                                fill_value=int(np.average(stream[0].data)))

            stream.sort(keys=["channel"], reverse=True)
            stream_dict = collections.defaultdict(Stream)
            for traces in stream:
                geophone_type = traces.stats.channel[0:2]
                stream_dict[geophone_type].append(traces)
            streams.append(stream_dict)
        except Exception as e:
            print(f"station = {time_window.station}, start time = {time_window.starttime}, error = {e}")
    print(f"Get {len(streams)} stream.")
    return streams


def _trim_trace(stream, points=3001):
    """
    Return trimmed stream in a given length.
    :param Stream stream: Obspy Stream object.
    :param int points: Trace data length.
    :rtype: Stream
    :return: Trimmed stream.
    """
    trace = stream[0]
    start_time = trace.stats.starttime
    if trace.data.size > 1:
        dt = (trace.stats.endtime - trace.stats.starttime) / (
                trace.data.size - 1)
        end_time = start_time + 3 + dt * (points - 1)
    elif trace.data.size == 1:
        end_time = start_time
    else:
        print("No data points in trace")
        return
    stream.trim(
        start_time + 3, end_time, nearest_sample=True, pad=True, fill_value=0
    )
    return stream


def signal_preprocessing(streams):
    """
    Return signal processed streams.
    :param list[collections.defaultdict[Any, Stream]] streams:
        List of dictionary contains geophone type and streams.
    :rtype: list[Stream]
    :return: List of signal processed stream.
    """
    processed_streams = []
    for geophone_type_stream_dict in streams:
        for geophone_type, stream in geophone_type_stream_dict.items():
            stream.detrend("demean")
            stream.detrend("linear")
            stream.filter("bandpass", freqmin=1, freqmax=45)
            for trace in stream:
                trace.normalize()
            stream.resample(100)
            stream = _trim_trace(stream)
            processed_streams.append(stream)
    print(f'Get {len(processed_streams)} signal preprocessed stream.')
    return processed_streams


def get_instances(database, streams, phase, shape, half_width, tag, output_filepath):
    """
    Returns list of Instance objects.
    :param str database:
    :param list[Stream] streams: List of obspy Stream objects.
    :param list[core.Pick] picks: List of Pick objects.
    :param str phase: PSN for example.
    :param str shape:
    :param str half_width:
    :param str tag:
    :param str output_filepath:
    :rtype: list[core.Instance]
    :return: List of Instance objects.
    """
    counter = 0
    client = SQL.Client(database)
    instances = []
    for stream in tqdm(streams):
        timewindow = get_timewindow(stream)
        inventory = client.get_inventory(network=stream[0].stats.network,
                                         station=stream[0].stats.station,
                                         time=timewindow.starttime)
        inventory = inventory.to_dataclass()
        traces = get_traces(stream, timewindow)
        label = get_label(database, phase, timewindow, shape, half_width, tag, inventory)
        id = '.'.join([timewindow.starttime.isoformat(), stream[0].stats.network, stream[0].stats.station])
        filepath_by_date = output_filepath[:-5] + str(timewindow.starttime.date()) + output_filepath[-5:]
        instance = core.Instance(inventory=inventory,
                                 timewindow=timewindow,
                                 traces=traces,
                                 labels=[label],
                                 id=id,
                                 datasetpath=filepath_by_date,
                                 dataset=filepath_by_date.split('/')[-1])

        instances.append(instance)
        counter += 1
    print(f"Add {counter} instances in {output_filepath}")
    return instances


def get_timewindow(stream):
    return core.TimeWindow(starttime=stream[0].stats.starttime.datetime,
                           endtime=stream[0].stats.endtime.datetime,
                           npts=stream[0].stats.npts,
                           samplingrate=stream[0].stats.sampling_rate,
                           delta=stream[0].stats.delta)


def get_traces(stream, timewindow=None):
    traces = []
    for tr in stream:
        channel = tr.stats.channel
        trace = core.Trace(inventory=core.Inventory(station=stream[0].stats.station,
                                                    network=stream[0].stats.network),
                           timewindow=timewindow,
                           data=tr.data,
                           channel=channel)
        traces.append(trace)
    traces.sort(key=operator.attrgetter('channel'))
    return traces


def get_label(database, phase, timewindow, shape=None, half_width=None, tag=None, inventory=None):
    label = core.Label(inventory=inventory,
                       timewindow=timewindow,
                       phase=phase,
                       tag=tag,
                       data=None)
    label = generate_pick_uncertainty_label(database, label, phase, shape, half_width)
    return label


def generate_pick_uncertainty_label(database, label, phases, shape, half_width):
    ph_index = {}
    label.data = np.zeros([label.timewindow.npts, len(label.phase)])
    for i, phase in enumerate(phases):
        ph_index[phase] = i

        picks = get_picks(
            database=database,
            from_time=label.timewindow.starttime,
            to_time=label.timewindow.endtime,
            station=label.inventory.station,
            phase=phase,
            tag=label.tag)

        for pick in picks:
            pick_time = UTCDateTime(pick.time) - UTCDateTime(label.timewindow.starttime)
            pick_time_index = int(pick_time / label.timewindow.delta)

            label.data[pick_time_index, i] = 1
            label.picks.append(pick)
        picks_time = label.data.copy()
        wavelet = scipy.signal.windows.get_window(shape, 2 * int(half_width))
        label.data[:, i] = scipy.signal.convolve(label.data[:, i],
                                                 wavelet[1:], mode="same")

    if 'E' in label.phase:
        eq_time = (picks_time[:, ph_index["P"]] - picks_time[:, ph_index["S"]])
        eq_time = np.cumsum(eq_time)
        if np.any(eq_time < 0):
            eq_time += 1
        label.data[:, ph_index["E"]] = eq_time

    if 'N' in label.phase:
        # Make Noise window by 1 - P - S
        label.data[:, ph_index["N"]] = 1
        label.data[:, ph_index["N"]] -= label.data[:, ph_index["P"]]
        label.data[:, ph_index["N"]] -= label.data[:, ph_index["S"]]

    return label


def get_hdf5(instances):
    instances = sorted(instances, key=lambda instance: instance.timewindow.starttime)
    instance_groupby = itertools.groupby(
        instances, key=lambda instance: [instance.timewindow.starttime.date()]
    )
    group_instances = [[item for item in data] for (key, data) in instance_groupby]
    utils.parallel(group_instances, func=tool.write_hdf5)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_config_filepath", type=str, required=True)
    parser.add_argument("--mode", type=str, required=True)
    args = parser.parse_args()
    config = tool.read_yaml(args.data_config_filepath)
    c = config['process_waveform'][args.mode]

    database = config['global']['database']
    pick_filter = c['pick_filter']
    trace_length = c['trace_length']
    instance_parameters = c['instance_parameters']
    output_filepath = instance_parameters['output_filepath']

    print(f"Start processing waveform for {args.mode}.")

    picks = get_picks(database, **pick_filter)
    time_windows = get_waveform_time_windows(picks, trace_length)
    waveforms = get_waveforms(time_windows, '/mnt/waveforms')
    processed_waveforms = signal_preprocessing(waveforms)
    instances = get_instances(database, processed_waveforms, **instance_parameters)

    get_hdf5(instances)

    instances_sql = [core.WaveformSQL(instance) for instance in instances]
    client = SQL.Client(database)
    client.add_waveforms(instances_sql)
