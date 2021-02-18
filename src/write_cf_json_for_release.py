#!/usr/bin/env Python

import os
import pandas as pd
import glob
import json
import argparse
import requests
import logging
from datetime import datetime as dt
logging.basicConfig(format='[%(levelname)s]:%(message)s', level=logging.INFO)

basedir = '/gws/nopw/j04/cp4cds1_vol3/c3s_34g/cmip6_qc/qc_logs/cf/CMIP6/'
# 'AerChemMIP/BCC/BCC-ESM1/ssp370/r1i1p1f1/Amon'
# COLUMNS = 'filepath pid cfversion timestamp level var_id errtype  error logfile '.split()
COLUMNS = 'filepath pid cfversion timestamp error_level error_type var_id error_details logfile '.split()
CMIP6_DF_AR6 = "../data/cmip6-ar6wg1-cf-df.pkl"
ERRORS_DF_AR6 = "../data/cmip6-ar6wg1-cf-errors-df.pkl"
CMIP6_DF = "../data/cmip6-cf-df.pkl"
ERRORS_DF = "../data/cmip6-cf-errors-df.pkl"
PRIORITY_VARS_FILE = "../data/AR6WG1_priorityVariables.json"
odir = "/gws/nopw/j04/cp4cds1_vol3/c3s_34g/c3s_34g_qc_results/QC_results/CF"
PIDBASE = "http://hdl.handle.net/"
# CF_results_path = "/gws/nopw/j04/cp4cds1_vol3/c3s_34g/c3s_34g_qc_results/QC_Results"
MISSING_FILENAME = "missing_datasets_ceda.txt"

def get_dataset_pids(filename):

    logging.info(f'Reading release datasets {filename}')

    d = {}
    with open(filename) as reader:
        for line in reader:
            d[line.split(',')[0].strip()] = line.split(',')[1].strip()

    return d


def main(datasets_file, dataframe_file, missing_file, ofile):

    # Read in data
    datasets_dict = get_dataset_pids(datasets_file)
    logging.info(f'Reading qc dataframe {dataframe_file}')
    dfo = pd.read_pickle(dataframe_file)
    df = dfo[dfo['filepath'].notna()]
    # print(df.iloc[3])


    # Set up json header
    jout = {}
    jout["header"] = {"application:": 'CF-checker', 'Author': 'Ruth Petrie', 'Institution': 'CEDA', 'Date': dt.today().isoformat(), 'version': '1.0'}
    jout["results"] = {}
    jout["datasets"] = {}
    # Loop over all datasets
    for dsid, pid in list(datasets_dict.items()):

        ddir = os.path.join('/badc/cmip6/data/', dsid.replace('.', '/'))
        if not os.path.exists(ddir):
            with open(f'{missing_file}', 'a+') as w:
                w.writelines(f'{dsid}\n')
            continue
        if len(os.listdir(ddir)) == 0:
            with open(f'{missing_file}', 'a+') as w:
                w.writelines(f'{dsid}\n')
            continue

        dataset_records = df[df.filepath.str.contains(dsid.replace('.', '/'))].reset_index(drop=True)
        if not len(dataset_records) > 0:
            with open(f'{missing_file}', 'a+') as w:
                w.writelines(f'QC_MISSING: {dsid}\n')
            continue

        json_ds_qc = {'error_severity': 'na', 'error_message': 'na'}
        ds_qcStatus = "pass"

        json_files = {}

        for index, row in dataset_records.iterrows():
            json_files[row.pid] = {'filename': os.path.basename(row.filepath),
                                  'qc_status': ['fail' if not row.error_level else 'pass'][0],
                                  'error_severity': row.cf_severity_level,
                                  'error_message': [row.error_details if row.error_details else 'na'][0]
                                  }
            if not row.error_level == 'pass':
                ds_qcStatus = "fail"

        jout["datasets"][pid] = {'dset_id': dsid, 'qc_status': ds_qcStatus, 'dataset_qc': json_ds_qc, 'files': json_files}

    # Output JSON
    json_obj = json.dumps(jout, indent=4)
    with open(f"{ofile}", "a+") as o:
        o.write(json_obj)


if __name__ == "__main__":


    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--datasets_list', type=str, help='A csv file of CMIP6 dataset ids and PIDS')
    parser.add_argument('-e', '--dataframe', type=str, help='A pandas dataframe of CMIP6 to compare against')
    parser.add_argument('-o', '--output_file', type=str, help='Ouput file')
    parser.add_argument('-r', '--release_dir', type=str, help='Directory of release to directory output files')
    args = parser.parse_args()

    if not os.path.exists(args.release_dir):
        print(args.release_dir)
        os.makedirs(release_dir)

    missing_file = os.path.join(args.release_dir, MISSING_FILENAME)
    if os.path.exists(missing_file):
        os.remove(missing_file)

    ofile = os.path.join(args.release_dir, args.output_file)
    if os.path.exists(ofile):
        os.remove(ofile)

    main(args.datasets_list, args.dataframe, missing_file, ofile)