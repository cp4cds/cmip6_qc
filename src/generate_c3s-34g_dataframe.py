#!/usr/bin/env Python

"""
This code takes the experiment level combined psv files and then generates an intermediate file by running:
python generate_c3s-34g_dataframe.py --create
The --filter option is redundant please now remove this.

The logic here should be:
1. Read in all psv files
2. Add some additional columns based on the information provided.
3. Write this out to file, currently using a binary pickle (pkl) file.

The rest is redundant for the current purposes and can be removed.
TODO: rewrite all of this.
"""

import os
import pandas as pd
import glob
import logging
import json
import argparse
import requests
import settings
import re
import subprocess
from datetime import datetime as dt

# input file could add this to argparse
C3S_RELEASE_DATASET_IDS = '../data/release3/dataset_ids_20210317.txt'
TODAY = dt.today().isoformat().split('T')[0]
basedir = '/gws/nopw/j04/cp4cds1_vol3/c3s_34g/cmip6_qc/src/qc_logs/cf/CMIP6/'
COLUMNS = 'filepath pid cfversion timestamp error_level error_type var_id error_details logfile '.split()
###
#THESE NEED TO BE CORRECTLY SET, e.g output dir must exist, maybe take these through argparse.
C3S34G_PRIORITY_VARS_FILE = "../data/variable_lists/c3s34g_variables.json"
CMIP6_DF_34G = f"../data/release3/cmip6-c3s34g-cf-df_{TODAY}.pkl"
CMIP6_DF_34G_csv = f"../data/release3/cmip6-c3s34g-cf-df_{TODAY}.csv"


# CMIP6_DF_AR6 = f"../data/pkl/cmip6-ar6wg1-cf-df_{TODAY}.pkl"
# ERRORS_DF_AR6 = "../data/pkl/cmip6-ar6wg1-cf-errors-df.pkl"
# CMIP6_DF = f"../data/pkl/cmip6-cf-df_{TODAY}.pkl"
# ERRORS_DF = "../data/pkl/cmip6-cf-errors-df.pkl"
# PRIORITY_VARS_FILE = "../data/variable_lists/AR6WG1_priorityVariables.json"
# odir = "/gws/nopw/j04/cp4cds1_vol3/c3s_34g/c3s_34g_qc_results/QC_results/CF"
# PIDBASE = "http://hdl.handle.net/"
# CF_results_path = "../../c3s_34g_qc_results/QC_results/CF/"
logging.basicConfig(format='[%(levelname)s]:%(message)s', level=logging.INFO)


def main():

    """
    This parser is now not needed as the only option is create so could be reconfigured without this unless
    you want this to add in additional options such as input file
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('--create', action='store_true', help='Create the dataframe')
    # parser.add_argument('--filter', action='store_true', help='Filter the dataframe to AR6WG1 datasets only')

    args = parser.parse_args()

    if args.create:
        df = create_c3s34g_df()
    else:
        df = pd.read_pickle(CMIP6_DF)
        # df = pd.read_pickle(CMIP6_DF_AR6)
        # df = pd.read_pickle(ERRORS_DF)
   if args.filter:
        df = filter_cmip6_df(df)


def _read(log_file):
    return pd.read_csv(log_file, sep='|', dtype=str, header=None, names=COLUMNS, na_values='')


def set_max_error_level(row):
    """
    For a given file it may fail on multiple reasons this determines the maximum
    """
    errors = []
    error_keys = list(settings.CF_ERROR_LEVEL.keys())
    for err in error_keys:
        if err in str(row.error_details):
            errors.append(settings.CF_ERROR_LEVEL[err])
    return _return_max(errors)


def _return_max(values):

    [ str(v) for v in values ]
    if 'major' in values:
        return 'major'
    if 'minor' in values:
        return 'minor'
    if 'na' in values:
        return None


def create_c3s34g_df():

    logging.debug(f'merging logs')
    df = merge_logs(basedir)
    logging.debug(f'logs merged')

    # Get rid of some odd characters from CF output that make parsing tricky
    for char in settings.SPEC_CHARS:
        df['error_details'] = df['error_details'].str.replace(char, '')

    df['model'] = df.filepath.apply(lambda s: s.split('/')[7].strip())
    df['dataset_id'] = df.filepath.apply(lambda path: '.'.join(path.split('/')[4:14]))

    df['error_var_type'] = 'N/A'
    df['error_var_type'][(df['error_level'] == 'ERROR') & (df['error_type'] == 'global')] = 'global'
    df['error_var_type'][(df['error_level'] == 'ERROR') & (df['error_type'] == 'variable')] = df.apply(lambda row: 'data' if row.var_id == row.filepath.split('/')[-1].split('_')[0].strip() else 'other', axis=1)

    df['cf_severity_level'] = 'unknown'
    df['cf_severity_level'][df['error_level'] == 'pass'] = 'pass'
    df['cf_severity_level'][df['error_level'] == 'ERROR'] = df.apply(lambda row: set_max_error_level(row), axis=1)
    # df['cf_severity_level'][df['error_level'] == 'ERROR'] = df.apply(lambda row: [ settings.CF_ERROR_LEVEL[err]  for err in error_keys if err in str(row.error_details) ], axis=1)
    # df_filtered = filter_df_to_ar6wg1(df)
    logging.debug(f'details added')

    """ 
    DUMP TO BINARY 'pickle, .pkl' file could also write to csv to see output df.to_csv('a-csv-file.csv')
    this would allow inspection of results during testing.
    """
    logging.debug(f'writing C3S 34g DF')
    df.to_pickle(CMIP6_DF_34G)
    return df

def merge_logs(basedir):

    logging.debug(f'working dir: {basedir}')
    files = glob.glob(f'{basedir}/*/*/*/*psv')
    _dfs = [_read(_) for _ in files]
    df = pd.concat(_dfs)
    return df


if __name__ == "__main__":
    main()



