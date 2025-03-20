"""
Input / Output
"""

import io
import multiprocessing as mp
import os
import warnings

import obspy
import obspy.io.nordic.core
import pandas as pd
from obspy import Stream, UTCDateTime
from obspy.core import inventory
from obspy.core.event import (Event, EventDescription, Origin, Pick,
                              WaveformStreamID)
from obspy.io.nordic.core import _write_nordic

import seisblue
import seisblue.utils


def parse_dataset(dataset):
    dataset = dataset.map(
        seisblue.example_proto.sequence_example_parser,
        num_parallel_calls=mp.cpu_count(),
    )
    return dataset


def associate_to_txt(txt_path, assciate_list):
    txt_file = open(txt_path, "w")
    for assoc in assciate_list:
        for event in assoc:
            txt_file.write(
                f"{obspy.UTCDateTime(event.ot)} {event.longitude:7.3f}{event.latitude:7.3f} {event.depth:7.1f} {event.nsta}\n"
            )


def read_header(header):
    header_info = {}
    header_info["year"] = int(header[1:5])
    header_info["month"] = int(header[5:7])
    header_info["day"] = int(header[7:9])
    header_info["hour"] = int(header[9:11])
    header_info["minute"] = int(header[11:13])
    header_info["second"] = float(header[13:19])
    header_info["lat"] = float(header[19:21])
    header_info["lat_minute"] = float(header[21:26])
    header_info["lon"] = int(header[26:29])
    header_info["lon_minute"] = float(header[29:34])
    header_info["depth"] = float(header[34:40]) * 1000
    header_info["magnitude"] = float(header[40:44])
    header_info["nsta"] = header[44:46].replace(" ", "")
    header_info["Pfilename"] = header[46:58].replace(" ", "")
    header_info["newNoPick"] = header[60:63].replace(" ", "")

    return header_info


def read_lines(lines):
    trace = []
    f = open(lines, "r")
    event_for_each_station = pd.read_fwf(
        f,
        header=None,
        skiprows=1,
        colspecs=[
            (1, 5),
            (5, 11),
            (12, 15),
            (16, 19),
            (19, 20),
            (21, 23),
            (23, 29),
            (29, 34),
            (35, 39),
            (39, 45),
            (45, 50),
            (51, 55),
            (55, 60),
            (61, 65),
            (66, 70),
            (71, 75),
            (76, 77),
            (78, 83),
        ],
    )
    event_for_each_station.columns = [
        "station_name",
        "epicentral_distance",
        "azimuth",
        "take_off_angle",
        "polarity",
        "minute",
        "p_arrival_time",
        "p_residual",
        "p_weight",
        "s_arrival_time",
        "s_residual",
        "s_weight",
        "log_A_of_S13",
        "log_A_of_A900A",
        "ML_of_S13",
        "ML_of_A900A",
        "intensity",
        "PGA",
    ]
    for i in range(len(event_for_each_station)):
        trace.append(event_for_each_station.iloc[i].to_dict())

    return trace


def read_afile(afile_path):
    count = 0
    f = open(afile_path, "r")
    print(afile_path)
    header = f.readline()
    header_info = read_header(header)
    if int(header_info["nsta"]) < 3:
        return [], 0
    trace_info = read_lines(afile_path)
    ev = obspy.core.event.Event()
    ev.event_descriptions.append(obspy.core.event.EventDescription())
    event_time = obspy.UTCDateTime(
        header_info["year"],
        header_info["month"],
        header_info["day"],
        header_info["hour"],
        header_info["minute"],
        header_info["second"],
    )

    ev.origins.append(
        obspy.core.event.Origin(
            time=event_time,
            latitude=header_info["lat"] + header_info["lat_minute"] / 60,
            longitude=header_info["lon"] + header_info["lon_minute"] / 60,
            depth=header_info["depth"],
        )
    )
    for trace in trace_info:
        if len(trace["station_name"]) > 4:
            trace["station_name"] = change_station_code(trace["station_name"])
        _waveform_id_1 = obspy.core.event.WaveformStreamID(
            station_code=trace["station_name"], channel_code="", network_code=""
        )
        for phase in ["P", "S"]:
            try:
                if (
                        trace[f"{phase.lower()}_arrival_time"]
                        and trace[f"{phase.lower()}_residual"] == 0
                ):
                    continue
                pick_time = (
                        obspy.UTCDateTime(
                            header_info["year"],
                            header_info["month"],
                            header_info["day"],
                            header_info["hour"],
                            int(trace["minute"]),
                        )
                        + trace[f"{phase.lower()}_arrival_time"]
                )
                if float(trace[f"{phase.lower()}_arrival_time"]) != 0:
                    time_errors = obspy.core.event.base.QuantityError()
                    time_errors.confidence_level = trace[f"{phase.lower()}_weight"]
                    ev.picks.append(
                        obspy.core.event.Pick(
                            waveform_id=_waveform_id_1,
                            phase_hint=phase,
                            time=pick_time,
                            time_errors=time_errors,
                        )
                    )

                    count += 1
            except TypeError:
                print(afile_path, "--------------", "afile_path")
    ev.magnitudes = header_info["magnitude"]
    return ev, count


def change_station_code(station):
    if station[0:3] == "TAP":
        station = "A" + station[3:6]
    if station[0:3] == "TCU":
        station = "B" + station[3:6]
    if station[0:3] == "CHY":
        station = "C" + station[3:6]
    if station[0:3] == "KAU":
        station = "D" + station[3:6]
    if station[0:3] == "ILA":
        station = "E" + station[3:6]
    if station[0:3] == "HWA":
        station = "G" + station[3:6]
    if station[0:3] == "TTN":
        station = "H" + station[3:6]

    return station


def read_afile_directory(path_list):
    event_list = []
    trace_count = 0
    abs_path = seisblue.utils.get_dir_list(path_list)
    for path in abs_path:
        try:
            event, c = read_afile(path)
            if event:
                event_list.append(event)
                trace_count += c
        except ValueError:
            print("error")
            continue
    print("total_pick = ", trace_count)
    return event_list


def associate_to_sfile(associate, database, out_dir):
    picks = seisblue.sql.get_pick_assoc_id(database=database, assoc_id=associate.id)
    try:
        seisblue.io.output_sfile(
            associate.origin_time,
            associate.latitude,
            associate.longitude,
            associate.depth,
            picks,
            out_dir=out_dir,
        )
    except:
        print()


def output_sfile(
        ot, latitude, longitude, depth, picks, out_dir, channel_type="EH", filename=None
):
    ev = Event()
    ev.event_descriptions.append(EventDescription())
    ev.origins.append(
        Origin(
            time=UTCDateTime(ot), latitude=latitude, longitude=longitude, depth=depth
        )
    )

    station = []
    ARC = []
    duration_time = 30
    for pick in picks:
        channel = [f"{channel_type}E", f"{channel_type}N", f"{channel_type}Z"]
        network = ""
        location = ""
        if pick.sta not in station:
            station.append(pick.sta)
            for chan in channel:
                ARC.append(
                    f"ARC {pick.sta:<5} {chan:<3} {network:<2} {location:<2} {ot.year:<4} "
                    f"{ot.month:0>2}{ot.day:0>2} {ot.hour:0>2}"
                    f"{ot.minute:0>2} {ot.second:0>2} {duration_time}"
                )

        _waveform_id_1 = WaveformStreamID(
            station_code=pick.sta,
            channel_code=f"{channel_type}Z",
            network_code=pick.net,
        )
        # obspy issue #2848 pick.second = 0. bug
        time = UTCDateTime(pick.time)
        if time.second == 0 and time.microsecond == 0:
            time = time + 0.01

        ev.picks.append(
            Pick(
                waveform_id=_waveform_id_1,
                phase_hint=pick.phase,
                time=time,
                evaluation_mode="automatic",
            )
        )
        if not os.path.exists(out_dir):
            os.mkdir(out_dir)
        if not ARC == []:
            _write_nordic(ev, filename=filename, wavefiles=ARC, outdir=out_dir)


def convert_lat_lon(lat, lon):
    NS = 1
    if lat[-1] == "S":
        NS = -1

    EW = 1
    if lon[-1] == "W":
        EW = -1
    if "." in lat[0:4]:
        lat_degree = int(lat[0:1])
        lat_minute = float(lat[1:-1]) / 60
    else:
        lat_degree = int(lat[0:2])
        lat_minute = float(lat[2:-1]) / 60
    if "." not in lat:  # high accuracy lat-lon
        lat_minute /= 1000
    lat = (lat_degree + lat_minute) * NS
    lat = inventory.util.Latitude(lat)

    lon_degree = int(lon[0:3])
    lon_minute = float(lon[3:-1]) / 60
    if "." not in lon:  # high accuracy lat-lon
        lon_minute /= 1000
    lon = (lon_degree + lon_minute) * EW
    lon = inventory.util.Longitude(lon)
    return (
        lat,
        lon,
    )


def read_hypout(hypo_result):
    picks = []
    azm = []
    origin = obspy.core.event.origin.Origin()
    origin["quality"] = obspy.core.event.origin.OriginQuality()
    origin["origin_uncertainty"] = obspy.core.event.origin.OriginUncertainty()
    f = io.BytesIO(hypo_result.stdout)
    f = f.readlines()

    skip = False
    for i, l in enumerate(f):
        if l.decode("ascii")[2:6] == "date":
            skip = i
    if not skip:
        return origin, picks

    f = io.BytesIO(hypo_result.stdout)
    #  date hrmn   sec      lat      long depth   no m    rms  damp erln erlt erdp
    # 21 420 2358 51.29 2351.54N 121 32.6E   3.9    6 3   0.19 0.000  6.6 13.2574.0

    hyp_event = pd.read_fwf(
        f,
        skiprows=skip,
        nrows=1,
        colspecs=[
            (0, 6),
            (7, 9),
            (9, 11),
            (11, 17),
            (17, 26),
            (26, 36),
            (36, 42),
            (42, 47),
            (47, 49),
            (49, 56),
            (56, 62),
            (62, 67),
            (67, 72),
            (72, 77),
        ],
    )
    f = io.BytesIO(hypo_result.stdout)
    # stn   dist   azm  ain w phas    calcphs hrmn tsec  t-obs  t-cal    res   wt di
    # SF64     9 218.3 58.6 0 P    A  PN3     2358 54.0   2.75   2.47   0.28 1.00 11
    hyp_pick = pd.read_fwf(
        f,
        skiprows=skip + 2,
        skipfooter=4,
        colspecs=[
            (0, 6),
            (6, 11),
            (11, 17),
            (17, 22),
            (22, 24),
            (25, 29),
            (30, 31),
            (33, 40),
            (41, 45),
            (46, 50),
            (50, 57),
            (57, 64),
            (64, 71),
            (71, 76),
            (76, 79),
        ],
    )
    for index, row in hyp_event.iterrows():
        lat, lon = convert_lat_lon(row["lat"], row["long"])
        origin.depth = row["depth"] * 1000
        origin.depth_errors.uncertainty = row["erdp"]
        origin.latitude = lat
        origin.latitude_errors.uncertainty = row["erlt"]
        origin.longitude = lon
        origin.longitude_errors.uncertainty = row["erln"]
        origin.quality.used_station_count = row["no"]
        origin.quality.azimuthal_gap = 0
        origin.time_errors = row["rms"]
        if float(row["sec"]) == 60:
            row["mn"] = int(str(row["mn"]).strip()) + 1
            row["sec"] = 0
        if float(row["mn"]) == 60:
            row["hr"] = int(str(row["hr"]).strip()) + 1
            row["mn"] = 0

        origin.time = obspy.UTCDateTime(
            int("20" + row["date"][0:2].strip()),
            int(row["date"][2:4].strip()),
            int(row["date"][4:6].strip()),
            int(str(row["hr"]).strip()),
            int(str(row["mn"]).strip()),
            float(row["sec"]),
        )
    for index, row in hyp_pick.iterrows():
        try:
            pick = Pick(
                waveform_id=WaveformStreamID(station_code=row["stn"]),
                phase_hint=row["phas"],
                time=origin.time + float(row["t-obs"]),
                evaluation_mode="automatic",
                time_errors=row["res"],
            )

            arrival = obspy.core.event.origin.Arrival(
                pick_id=pick.resource_id, phase=row["phas"], time_residual=row["res"]
            )
        except Exception as err:

            continue
        picks.append(pick)
        origin.arrivals.append(arrival)
        azm.append(float(row["azm"]))
    azm.sort(reverse=True)
    try:
        for i in range(len(azm)):
            if i == 0:
                gap = int(abs(azm[i] - (azm[i - 1] + 360)))
            else:
                gap = int(abs(azm[i] - azm[i - 1]))
            if gap > origin.quality.azimuthal_gap:
                origin.quality.azimuthal_gap = gap
    except TypeError as err:
        print(err)
        print(azm[i], "   ", azm[i - 1])
    return origin, picks


if __name__ == "__main__":
    pass