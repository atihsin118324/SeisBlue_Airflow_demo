from seisblue import SQL, utils, io

import argparse


def associate_to_sfile(associate, database, out_dir):
    picks = SQL.get_pick_assoc_id(database=database, assoc_id=associate.id)
    try:
        io.output_sfile(
            associate.origin_time,
            associate.latitude,
            associate.longitude,
            associate.depth,
            picks,
            out_dir=out_dir,
        )
    except:
        print()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--associated_filter", type=str, required=True)
    parser.add_argument("--database", type=str, required=True)
    parser.add_argument("--output_filepath", type=str, required=True)
    args = parser.parse_args()

    client = SQL.Client(args.database)
    associates = client.get_associates(**eval(args.associated_filter))
    utils.parallel(
        associates,
        func=associate_to_sfile,
        database=args.database,
        out_dir=args.output_filepath,
        batch_size=16,
    )
