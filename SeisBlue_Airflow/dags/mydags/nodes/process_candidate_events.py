import argparse

from seisblue import assoc_core, tool

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_config_filepath", type=str, required=True)
    args = parser.parse_args()
    config = tool.read_yaml(args.data_config_filepath)

    database = config['global']['database']
    c = config['associate']['candidate_events']
    associator_parameters = c['associator_parameters']

    associator = assoc_core.LocalAssociator(database, **associator_parameters)

    # candidate events
    print("add candidate events")
    associator.add_candidate_events()
    print("associate events")
    associator.fast_associate(plot=c['plot'])
