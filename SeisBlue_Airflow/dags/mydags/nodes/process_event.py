from seisblue import core, tool, SQL

from typing import List
import glob
import itertools
import obspy.io.nordic.core
import os
import warnings
import argparse
from datetime import datetime


def _get_obspy_event(filepath, debug=False):
    """
    Returns obspy.event list from sfile.
    :param str filepath: Sfile file path.
    :param bool debug: If False, warning from reader will be ignore,
        default to False.
    :rtype: list[obspy.core.event.event.Event]
    :return: List of obspy events.
    """
    with warnings.catch_warnings():
        if not debug:
            warnings.simplefilter("ignore")
        try:
            catalog = obspy.io.nordic.core.read_nordic(filepath)
            return catalog.events

        except Exception as err:
            if debug:
                print(err)


def read_sfile(events_dir):
    """
    Returns obspy events from events directory.
    :param str events_dir: Directory contains SEISAN sfile.
    :rtype: list[obspy.core.event.event.Event]
    :return: List of obspy events.
    """
    events_path_list = glob.glob(os.path.join(events_dir, "*"))
    obspy_events = list(map(_get_obspy_event, events_path_list))
    flatten = itertools.chain.from_iterable
    while isinstance(obspy_events[0], list):
        obspy_events = flatten(obspy_events)
        obspy_events = [event for event in obspy_events if
                        event.origins[0].latitude]
    print(f"Read {len(obspy_events)} events from {events_dir}.")
    return obspy_events


def get_events(obspy_events):
    """
    Returns event objects from obspy events .
    :param list[obspy.core.event.event.Event] obspy_events: List of obspy events.
    :rtype: list[core.Event]
    :return: List of event objects.
    """
    events = []

    for obspy_event in obspy_events[:20]:
        origin_time = obspy_event.origins[0].time
        latitude = obspy_event.origins[0].latitude
        longitude = obspy_event.origins[0].longitude
        depth = obspy_event.origins[0].depth
        event = core.Event(time=datetime.utcfromtimestamp(origin_time.timestamp),
                           latitude=latitude,
                           longitude=longitude,
                           depth=depth)
        event_SQL = core.EventSQL(event)
        events.append(event_SQL)
    return events


def get_picks(obspy_events: list, tag: str) -> List[object]:
    """
    Returns list of picks dataclass from events list.
    :param list obspy_events: Obspy Event.
    :param str tag: Pick tag.
    :rtype: list
    :return: Dataclass Pick.
    """
    picks = []
    for obspy_event in obspy_events:
        for pick in obspy_event.picks:
            time = pick.time
            station = pick.waveform_id.station_code
            network = pick.waveform_id.network_code
            phase = pick.phase_hint
            pick = core.Pick(time=datetime.utcfromtimestamp(time.timestamp),
                             inventory=core.Inventory(station=station, network=network),
                             phase=phase,
                             tag=tag)
            pick_SQL = core.PickSQL(pick)
            picks.append(pick_SQL)
    print(f'Read {len(picks)} picks.')
    return picks


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_config_filepath", type=str, required=True)
    args = parser.parse_args()
    config = tool.read_yaml(args.data_config_filepath)

    tag = config['process_event']['tag']
    database = config['global']['database']
    build = config['process_event']['build_database']

    print("Processing events and picks.")
    obspy_events = read_sfile('/mnt/events')
    events = get_events(obspy_events)
    picks = get_picks(obspy_events, tag)
    client = SQL.Client(database, build=build)
    client.add_events(events)
    client.add_picks(picks)
