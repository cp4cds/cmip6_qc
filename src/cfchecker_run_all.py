#!/usr/bin/env python

"""This script takes arguments from the command line and runs the script run_batch
for each of the models provided as an argument or for all models if none were
provided"""

import argparse
import subprocess
import os
import logging
import sys
from datetime import datetime as dt

import settings
# subprocess.call(["source", settings.SETUP_ENV_FILE], shell=True)

logging.basicConfig(
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(sys.stdout)
        ]
)

def arg_parse_all():
    """
    Parses arguments given at the command line
    :return: Namespace object built from attributes parsed from command line.
    """

    parser = argparse.ArgumentParser()
    
    parser.add_argument('-q', '--qc_check', nargs=1, type=str, choices=settings.qc_choices, required=False, default="cfchecker",
                        help=f"Chose which quality control method from {settings.qc_choices} to use default is to run all")
    
    parser.add_argument('-f', '--file', type=str, required=False,
                        help=f" If supplied only QC the datasets in the list.")

    return parser.parse_args()


def loop_over_all_cmip6(args):
    """
    Runs run batch for each of the models listed
    :param args: (namespace) Namespace object built from attributes parsed from command line
    """

    current_directory = os.getcwd()

    qc_type = args.qc_check[0]

    # iterate over models
    #
    # deck = os.path.join(settings.CMIP6_ARCHIVE_DIR, 'CMIP6', 'CMIP')
    # scenario = os.path.join(settings.CMIP6_ARCHIVE_DIR, 'CMIP6', 'ScenarioMIP')

    for cmip6, dirs, files in os.walk(settings.CMIP6_ARCHIVE_DIR):
        for dir in dirs:
            dir_path = os.path.join(cmip6, dir)
            # if dir_path.startswith(deck) or dir_path.startswith(scenario):
            # at the simulation level
            if len(dir_path.split('/')) == 8:
                # calls run_batch from command line
                cmd = f"python {current_directory}/cfchecker_run_batch.py --model {dir_path} --qc_check {qc_type}"
                subprocess.call(cmd, shell=True)
                logging.info(f"Running {dir_path}")
                

def qcloop_over_datasets(datasets_file):

    with open(datasets_file) as r:
        datasets = [line.strip() for line in r]

    now = dt.now().strftime('%Y%m%d.%H%M')
    odir = os.path.join('/group_workspaces/jasmin2/cp4cds1/vol3/c3s_34g/cmip6_qc/src','lotus-slurm-logs', now)
    if not os.path.isdir(odir):
        os.makedirs(odir)

    for ds in datasets:
        cmd_string = f'python cfchecker_run_unit.py -q cfchecker -id {ds}'
        sbatch_cmd = f'sbatch -p {settings.QUEUE} -t 18:00:00 ' \
              f'-o {odir}/%J.out -e {odir}/%J.err ' \
              f'--wrap "{cmd_string}"'
        subprocess.call(sbatch_cmd, shell=True)
        logging.info(f"running {sbatch_cmd}")


def main():
    """Runs script if called on command line"""

    args = arg_parse_all()

    if not (args.file):
        logging.info(f'no file supplied QCing all CMIP6 data')
        # loop_over_all_cmip6(args)
    else:
        logging.info(f'QC for datasets listed in file {args.file}')
        qcloop_over_datasets(args.file)


if __name__ == '__main__':
    main()
