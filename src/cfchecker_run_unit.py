#!/usr/bin/env python

"""
This script takes two command line arguments
1. a dataset id
2. the type of qc: which is now alway cfchecker

Run as: python cfchecker_run_unit.py --dataset-id <id> --qc_check cfchecker
(qc_check type should be default but specify to make sure)

This can be run at the command line or as part of a batch process on LOTUS.

This script calls the CF-checker as defined in the script simple_cfcheck
and it is imported as SimpleCFChecker.
run_unit for each of the variables provided as an argument or for
all variables if none were provided.
"""


import sys
import os
import glob
import argparse
import logging
import subprocess

import settings
from simple_cfcheck import SimpleCFChecker

logging.basicConfig(level=logging.INFO,handlers=[logging.StreamHandler(sys.stdout)])
# subprocess.call(["source", settings.SETUP_ENV_FILE], shell=True)


def arg_parse_chunk():
    """
    Parses arguments given at the command line
    :return: Namespace object built from attributes parsed from command line.
    """

    parser = argparse.ArgumentParser()

    parser.add_argument('-id', '--dataset_id', type=str,
                        help=f'Dataset ID to be quality controlled')
    parser.add_argument('-q', '--qc_check', nargs=1, type=str, choices=settings.qc_choices, required=False, default="cfchecker",
                        help=f"Chose which quality control method from {settings.qc_choices} to use default is to run all")

    return parser.parse_args()


def find_files(dsid):
    """
    Finds files that correspond to the given arguments.
    :param dsid: (string) dataset id.
    :return: The netCDF files that correspond to the arguments.
    """
    dataset_dir = os.path.join(settings.CMIP6_ARCHIVE_DIR, dsid.replace('.', '/'))
    files = os.listdir(dataset_dir)
    nc_files = []
    for file in files:
        nc_files.append(os.path.join(dataset_dir, file))
    logging.debug(f"Found files: {nc_files}")

    return nc_files




def run_qc(dsid, ncfiles, qc, checker):
    """
    Runs the specific qc method for the NetCDF files given.
    Records the log files in a given directory.
    Only records the ".log" if the QC check was successfully executed. 
    :param stat: (string) Statistic to calculate as specified at the command line.
    :param model: (string) Model chosen as argument at command line.
    :param ensemble: (string) Ensemble chosen as argument at command line.
    :param var_id: (string) Variable chosen as argument at command line.
    :return: txt or NetCDF file depending on success/ failure of the job.
    """

    # define output file paths
    #current_directory = os.getcwd()  # get current working directory
    current_directory = os.path.dirname(os.getcwd()) # get directory above current to keep logs out of src dir

    cmip, mip, inst, model, experiment, ensemble, table, var, grid, version = dsid.split('.')

    logging.info(f"CHECKING {dsid}")

    if qc == "cfchecker":
        output_path = settings.CF_OUTPUT_PATH_TMPL.format(current_directory=current_directory,
                                                       cmip6=cmip, mip=mip, inst=inst, model=model, experiment=experiment,
                                                       ensemble=ensemble, table=table)
        # logging.info(f"Output path: {output_path}")

    if not os.path.exists(output_path):
        os.makedirs(output_path)

    logfile = os.path.join(output_path, dsid + '.psv')
    logging.info(f'LOGFILE: {logfile}')
    if os.path.exists(logfile):

        return True

    # Run QC check
    for ncfile in ncfiles:
        logging.info(f"will run qc {ncfile}")
        try:
            checker.run(ncfile, logfile)
        except:
            os.remove(logfile)



    # check for success file - if exists - continue
    # success_file = f'{output_path}/{ncfile}.log'
    # 
    # if os.path.exists(success_file):
    #         logging.info(f"CFChecker already successfully run for {success_file}")
    #         return

    # logging.info(f'Output file generated: {output_path}/{ncfile}.log')

    # if not os.path.exists(output_file):
    #     os.rmdir(output_path)
    #
    #     if not os.path.exists(no_output_path):
    #         os.makedirs(no_output_path)
    #
    #     open(os.path.join(no_output_path, f'{var_id}.nc.txt'), 'w')
    #
    #     print(f'[ERROR] Failed to generate output file: {output_path}/{var_id}.nc')
    #     return False

    # create success file
    # if not os.path.exists(success_path):
    #     os.makedirs(success_path)

    # open(os.path.join(success_path, f'{var_id}.nc.txt'), 'w')

    return True


def run_unit(dataset_id, qc_type):
    """
    For a given dataset calls run_unit.
    :param args: (namespace) Namespace object built from attributes parsed from command line
    """
    # set initial failure count
    # keep track of failures. Many failures expected for this example so the
    # limit is set high
    # good practice to include this
    failure_count = 0

    # turn arguments into string
    checker = SimpleCFChecker()
    dataset_path = os.path.join(settings.CMIP6_ARCHIVE_DIR, dataset_id.replace('.', '/'))

    # exit if too many failures
    # if failure_count >= settings.EXIT_AFTER_N_FAILURES:
    #     print('[ERROR] Maximum failure count met')
    #     sys.exit(1)

    # find files
    nc_files = find_files(dataset_id)

    """
    insert ncfiles check
    1. check nc_files exists as a list 
    2. assert each file is of type NetCDF4
    # check data is valid
    if not nc_files:

        if not os.path.exists(bad_data_path):
            os.makedirs(bad_data_path)

        open(os.path.join(bad_data_path, f'{var_id}.nc.txt'), 'w')  # creates empty file

        print(f'[ERROR] No valid files for {var_id}')
        return False

    # check date range is valid
    validity = is_valid_range(nc_files)
    if not validity:

        if not os.path.exists(bad_num_path):
            os.makedirs(bad_num_path)

        open(os.path.join(bad_num_path, f'{var_id}.nc.txt'), 'w')
        return False

    """
    run_qc(dataset_id, nc_files, qc_type, checker)
        # if unit is False:
        #     failure_count += 1
        #     return

    # logging.info(f"Completed job")


def main():
    """Runs script if called on command line"""

    args = arg_parse_chunk()
    run_unit(args.dataset_id, args.qc_check[0])


if __name__ == '__main__':
    main()
