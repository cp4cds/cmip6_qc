#!/usr/bin/env python

"""This script takes arguments from the command line and runs the script run_batch
for each of the models provided as an argument or for all models if none were
provided"""

import argparse
import subprocess
import os
import logging
import sys

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
    
    parser.add_argument('-q', '--qc_check', nargs=1, type=str, choices=settings.qc_choices, required=False, default="all",
                        help=f"Chose which quality control method from {settings.qc_choices} to use default is to run all")

    return parser.parse_args()


def loop_over_cmip6(args):
    """
    Runs run batch for each of the models listed
    :param args: (namespace) Namespace object built from attributes parsed from command line
    """

    current_directory = os.getcwd()

    qc_type = args.qc_check[0]

    # iterate over models
    for cmip6, dirs, files in os.walk(settings.CMIP6_ARCHIVE_DIR):
        for dir in dirs:
            dir_path = os.path.join(cmip6, dir)
            # at the simulation level
            if len(dir_path.split('/')) == 9:
                # calls run_batch from command line
                cmd = f"python {current_directory}/run_batch.py --simulation {dir_path} --qc_check {qc_type}"
                subprocess.call(cmd, shell=True)
                logging.info(f"Running {dir_path}")
                

def main():
    """Runs script if called on command line"""

    args = arg_parse_all()
    loop_over_cmip6(args)


if __name__ == '__main__':
    main()
