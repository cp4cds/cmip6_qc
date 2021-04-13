#!/usr/bin/env Python

import os, sys
import argparse
import logging

c3s_sel_dir = '/group_workspaces/jasmin3/esgf_repl/synda/selections/cmip6-ceda-synda-selections/c3s-priority/missing_basin/'


def parseArgs():

    parser = argparse.ArgumentParser()
    parser.add_argument('--datasets_list', '-i', type=str, help="list of datasets to generate selection files for")
    args = parser.parse_args()
    if not args.datasets_list:
        sys.exit('Please supply a datasets list file')

    if not os.path.isfile(args.datasets_list):
        sys.exit(f'Please supply a valid file of dataset ids')

    return(args)


def _setup():
    logging.basicConfig(format='[%(levelname)s]:%(message)s', level=logging.INFO)


def main(datasets):

    logging.info(f'Reading {datasets}')
    with open(datasets) as r:
        dataset_ids = [line.strip() for line in r]

    logging.info(f'Generating selection files')

    for ds in dataset_ids:
        logging.info(ds)
        mipera, activity, inst, source, expt, member, table, var, grid, version = ds.split('.')
        with open(os.path.join(c3s_sel_dir, ds + '.txt'), 'w+') as r:
            r.writelines(f'project=CMIP6\n'
                         f'protocol=gridftp\n'
                         f'latest=true\n'
                         f'priority=5000\n'
                         f'data_node!=esgf-data3.ceda.ac.uk\n'
                         f'activity_id={activity}\n'
                         f'institution_id={inst}\n'
                         f'source_id={source}\n'
                         f'experiment_id={expt}\n'
                         f'member_id={member}\n'
                         f'variable[table_id={table}]={var}\n'
                         f'grid_label={grid}')


if __name__ == "__main__":

    _setup()
    args = parseArgs()
    main(os.path.abspath(args.datasets_list))
