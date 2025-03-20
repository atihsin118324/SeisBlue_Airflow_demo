import argparse
from obspy.clients.filesystem import sds
import pandas as pd

from seisblue import SQL, core, tool, utils


def get_picks(**kwargs):
    client = SQL.Client('testdb', build=False)
    results = client.get_pick(**kwargs)
    picks = [pick.to_dataclass() for pick in results]
    return picks


def get_nslc(sds_root):
    client = sds.Client(sds_root=sds_root)
    nslc = client.get_all_nslc()
    df_nslc = pd.DataFrame(nslc, columns=["net", "sta", "loc", "chan"])
    df_nslc = df_nslc.drop(columns="chan").drop_duplicates()
    df_nslc = df_nslc.set_index("sta")
    return df_nslc


def _pickassoc(pick, df_nslc):
    try:
        if isinstance(df_nslc.loc[pick.inventory.station]["net"], pd.Series):
            df_nslc_net = df_nslc.loc[pick.inventory.station]["net"][0]
        else:
            df_nslc_net = df_nslc.loc[pick.inventory.station]["net"]
    except:
        df_nslc_net = None
    try:
        if isinstance(df_nslc.loc[pick.inventory.station]["loc"], pd.Series):
            df_nslc_loc = df_nslc.loc[pick.inventory.station]["loc"][0]
        else:
            df_nslc_loc = df_nslc.loc[pick.inventory.station]["loc"]
    except:
        df_nslc_loc = None

    item = core.PickAssoc(
        sta=pick.inventory.station,
        net=df_nslc_net,
        loc=df_nslc_loc,
        time=pick.time,
        snr=pick.snr,
        trace_id=pick.traceid,
        phase=pick.phase,
    )
    item = core.PickAssocSQL(item)
    return item


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_config_filepath", type=str, required=True)
    args = parser.parse_args()
    config = tool.read_yaml(args.data_config_filepath)
    database = config['global']['database']
    c = config['associate']['assoc_picks']

    pick_filter = c['pick_filter']

    picks = get_picks(**pick_filter)
    df_nslc = get_nslc('/mnt/waveforms')
    print(len(picks))
    bulk_picks = utils.parallel(picks, func=_pickassoc, df_nslc=df_nslc)
    bulk_picks = [item for sublist in bulk_picks for item in sublist]

    client = SQL.Client(database)
    client.add_pickassoc(bulk_picks)
