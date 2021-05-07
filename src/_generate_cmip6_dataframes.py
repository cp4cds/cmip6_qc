
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

TODAY = dt.today().isoformat().split('T')[0]

basedir = '/gws/nopw/j04/cp4cds1_vol3/c3s_34g/cmip6_qc/src/qc_logs/cf/CMIP6/'
COLUMNS = 'filepath pid cfversion timestamp error_level error_type var_id error_details logfile '.split()
CMIP6_DF_AR6 = f"../data/pkl/cmip6-ar6wg1-cf-df_{TODAY}.pkl"
CMIP6_DF_34G = f"../data/release2/cmip6-c3s34g-cf-df_{TODAY}.pkl"
CMIP6_DF_34G_csv = f"../data/release2/cmip6-c3s34g-cf-df_{TODAY}.csv"
# ERRORS_DF_AR6 = "../data/pkl/cmip6-ar6wg1-cf-errors-df.pkl"
CMIP6_DF = f"../data/pkl/cmip6-cf-df_{TODAY}.pkl"
# ERRORS_DF = "../data/pkl/cmip6-cf-errors-df.pkl"
PRIORITY_VARS_FILE = "../data/variable_lists/AR6WG1_priorityVariables.json"
C3S34G_PRIORITY_VARS_FILE = "../data/variable_lists/c3s34g_variables.json"
# odir = "/gws/nopw/j04/cp4cds1_vol3/c3s_34g/c3s_34g_qc_results/QC_results/CF"
# PIDBASE = "http://hdl.handle.net/"
C3S_RELEASE_DATASET_IDS = '../release3/dataset_ids_20210317.txt'
# CF_results_path = "../../c3s_34g_qc_results/QC_results/CF/"
logging.basicConfig(format='[%(levelname)s]:%(message)s', level=logging.INFO)


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--create', action='store_true', help='Create the dataframe')
    parser.add_argument('--filter', action='store_true', help='Filter the dataframe to AR6WG1 datasets only')

    args = parser.parse_args()

    if args.create:
        df = create_cmip6_df()
    else:
        df = pd.read_pickle(CMIP6_DF)
        # df = pd.read_pickle(CMIP6_DF_AR6)
        # df = pd.read_pickle(ERRORS_DF)

    if args.filter:
        df = filter_cmip6_df(df)


def _read(log_file):
    return pd.read_csv(log_file, sep='|', dtype=str, header=None, names=COLUMNS, na_values='')


def filter_df_to_ar6wg1(df):

    df_new = pd.DataFrame([], list(df.columns))

    with open(PRIORITY_VARS_FILE) as json_file:
        ar6wg1 = json.load(json_file)

    for exp in ar6wg1["requested"].keys():
        print(exp)
        for table, vars in ar6wg1['requested'][exp].items():
            print(table)
            for v in vars:
                print(v)
                expt = df[df.filepath.str.contains(exp)].reset_index(drop=True)
                tab = expt[expt.filepath.str.contains(table)].reset_index(drop=True)
                var = tab[tab.filepath.str.contains(v)].reset_index(drop=True)
                df_new = df_new.append(var, ignore_index=True)

    return df_new


def filter_df_to_c3s34g(df):

    with open(C3S_RELEASE_DATASET_IDS) as r:
        ds_ids = [line.strip() for line in r]

    df_new = pd.DataFrame([], list(df.columns))

    for id in ds_ids:
        logging.debug(f'{id}')
        ds = df[df.dataset_id.str.contains(id)].reset_index(drop=True)
        df_new = df_new.append(ds, ignore_index=True)


    # with open(C3S34G_PRIORITY_VARS_FILE) as json_file:
    #     c3s34g_vars = json.load(json_file)
    #
    # for exp in settings.EXPERIMENTS:
    #     print(exp)
    #     for table, vars in c3s34g_vars['requested'].items():
    #         print(table)
    #         for v in vars:
    #             print(v)
    #             expt = df[df.filepath.str.contains(exp)].reset_index(drop=True)
    #             tab = expt[expt.filepath.str.contains(table)].reset_index(drop=True)
    #             var = tab[tab.filepath.str.contains(v)].reset_index(drop=True)
    #             df_new = df_new.append(var, ignore_index=True)

    return df_new


def set_max_error_level(row):
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


def create_cmip6_df():

    logging.debug(f'merging logs')
    df = merge_logs(basedir)
    logging.debug(f'logs merged')

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

    logging.debug(f'writing CMIP6 DF')
    df.to_pickle(CMIP6_DF)

    logging.debug(f'filtering')
    df_filtered = filter_df_to_c3s34g(df)
    df_filtered.to_pickle(CMIP6_DF_34G)
    df_filtered.to_csv(CMIP6_DF_34G_csv)
    logging.debug(f'filtered')

    # errors_dataframe = df_filtered[df_filtered['error_level'] == 'ERROR'].reset_index()
    # errors_dataframe.to_pickle(ERRORS_DF)
    return df_filtered


def merge_logs(basedir):

    logging.debug(f'working dir: {basedir}')
    files = glob.glob(f'{basedir}/*/*/*/*psv')
    _dfs = [_read(_) for _ in files]
    df = pd.concat(_dfs)
    return df


def filter_cmip6_df(df):

    # df['dataset_id'] = df.filepath.apply(lambda path: '.'.join(path.split('/')[4:14]))
    # df_filtered = filter_df_to_ar6wg1(df)

    logging.debug(f'filtering')
    df_filtered = filter_df_to_c3s34g(df)
    df_filtered.to_pickle(CMIP6_DF_34G)
    df_filtered.to_csv(CMIP6_DF_34G_csv)
    logging.debug(f'filtered')

    # logging.debug(f'about to filter')
    # df_filtered = filter_df_to_c3s34g(df)
    # df_filtered.to_pickle(CMIP6_DF_AR6)
    # df_filtered.to_csv("../data/csv/cmip6-ar6wg1-cf.csv")
    # # errors_dataframe = df_filtered[df_filtered['error_level'] == 'ERROR'].reset_index()
    # errors_dataframe.to_pickle(ERRORS_DF_AR6)

    return df_filtered


if __name__ == "__main__":
    main()



