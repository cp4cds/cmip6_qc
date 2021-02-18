#!/usr/bin/env Python

import os
import pandas as pd
import json
import argparse
import requests
import logging
from datetime import datetime as dt
logging.basicConfig(format='[%(levelname)s]:%(message)s', level=logging.INFO)

# basedir = '/gws/nopw/j04/cp4cds1_vol3/c3s_34g/cmip6_qc/qc_logs/cf/CMIP6/'
# # 'AerChemMIP/BCC/BCC-ESM1/ssp370/r1i1p1f1/Amon'
# # COLUMNS = 'filepath pid cfversion timestamp level var_id errtype  error logfile '.split()
# COLUMNS = 'filepath pid cfversion timestamp error_level error_type var_id error_details logfile '.split()
# CMIP6_DF_AR6 = "../data/cmip6-ar6wg1-cf-df.pkl"
# ERRORS_DF_AR6 = "../data/cmip6-ar6wg1-cf-errors-df.pkl"
# CMIP6_DF = "../data/cmip6-cf-df.pkl"
# ERRORS_DF = "../data/cmip6-cf-errors-df.pkl"
# PRIORITY_VARS_FILE = "../data/AR6WG1_priorityVariables.json"
# odir = "/gws/nopw/j04/cp4cds1_vol3/c3s_34g/c3s_34g_qc_results/QC_results/CF"
# PIDBASE = "http://hdl.handle.net/"
# # CF_results_path = "/gws/nopw/j04/cp4cds1_vol3/c3s_34g/c3s_34g_qc_results/QC_Results"
# MISSING_FILENAME = "missing_datasets_ceda.txt"

QCTEMPLATE = '../data/release2/QC_template.json'
DATASET_ID_PIDS_FILE = '../data/release2/dataset-ids-pids_release2_202002.csv'
CF_RESULTS_FILE = '../data/release2/cmip6-c3s34g-cf-df.pkl'
QC_OUTPUT = '../data/release2/QC_cfchecker_2.json'

def main():

    # OPEN FILES
    with open(QCTEMPLATE) as jsn:
        qc_template = json.load(jsn)
    ds_pids = list(qc_template["datasets"].keys())

    logging.debug(f'Number of dataset ids: {len(ds_pids)}')
    datasets_dict = get_dataset_pids(DATASET_ID_PIDS_FILE)
    cf_results_df = pd.read_pickle(CF_RESULTS_FILE)
    cf_dataset_dsids = list(cf_results_df['dataset_id'].dropna())
    cf_dataset_pids = set()
    for dsid in cf_dataset_dsids:
        cf_dataset_pids.add(datasets_dict[dsid])
    logging.debug(f'Dataset pids {list(cf_dataset_pids)[:3]}')


    # Complete header information
    qc_template['header']["application:"] = 'CF-checker'
    qc_template['header']["Author"] = 'Ruth Petrie'
    qc_template['header']["Institution"] = 'CEDA'
    qc_template['header']["Date"] = '2021-02-17'
    qc_template['header']["version"] = '1.0'

    # Loop over qc_template entries
    for ds_pid in ds_pids:
        logging.info(f'Dataset id {ds_pid}')
        if not ds_pid in list(cf_dataset_pids):
            logging.info(f'Dataset id not in CEDA set {ds_pid}')
            continue

        logging.debug(f'{qc_template["datasets"][ds_pid]}')
        f_pids = list(qc_template["datasets"][ds_pid]['files'].keys())
        logging.debug(f'File pids: {f_pids}')
        ds_qc_status = True
        for fpid in f_pids:
            fentry = qc_template["datasets"][ds_pid]['files'][fpid]
            cf_results = cf_results_df.loc[cf_results_df['pid'] == fpid]
            if cf_results.empty:
                logging.info(f'FILE DATAFRAME EMPTY PID MISMATCH {ds_pid, fpid}')
                ds_qc_status = False
                continue
            logging.debug(f'CF results {cf_results}')
            if cf_results.error_level.reset_index(drop=True)[0] == 'pass':
                fentry["qc_status"] = 'pass'
            else:
                fentry["qc_status"] = 'fail'
                ds_qc_status = False
            fentry["error_severity"] = cf_results.cf_severity_level.reset_index(drop=True)[0]
            fentry["error_message"] = [cf_results.error_details.reset_index(drop=True)[0] if cf_results.error_details.reset_index(drop=True)[0] else 'na'][0]

        if ds_qc_status:
            qc_template["datasets"][ds_pid]["qc_status"] = 'pass'
        else:
            qc_template["datasets"][ds_pid]["qc_status"] = 'fail'

        logging.debug(qc_template["datasets"][ds_pid])

    # Output JSON
    with open(QC_OUTPUT, "w+") as jo:
        json.dump(qc_template, jo, indent=4)#, separators=(',', ': '))

















        # for index, row in cf_results_df.iterrows():
        #     print(index, row)
        #     asdf
        #     qc_template["datasets"][id]['files'] =
        #
        # json_files[row.pid] = {'filename': os.path.basename(row.filepath),
        #                                   'qc_status': ['fail' if not row.error_level else 'pass'][0],
        #                                   'error_severity': row.cf_severity_level,
        #                                   'error_message': [row.error_details if row.error_details else 'na'][0]
        #                                   }
        #             if not row.error_level == 'pass':
        #                 ds_qcStatus = "fail"
        #
        #
        # qc_template["datasets"][id]["qc_status"] = 'tbc'
        # # files
        # for file in

def get_dataset_pids(filename):

    logging.debug(f'Reading release datasets {filename}')
    d = {}
    with open(filename) as reader:
        for line in reader:
            d[line.split(',')[0].strip()] = line.split(',')[1].strip()

    return d


# def main(datasets_file, dataframe_file, missing_file, ofile):
#
#     # Read in data
#     datasets_dict = get_dataset_pids(datasets_file)
#     logging.info(f'Reading qc dataframe {dataframe_file}')
#     dfo = pd.read_pickle(dataframe_file)
#     df = dfo[dfo['filepath'].notna()]
#     # print(df.iloc[3])
#
#
#     # Set up json header
#     jout = {}
#     jout["header"] = {"application:": 'CF-checker', 'Author': 'Ruth Petrie', 'Institution': 'CEDA', 'Date': dt.today().isoformat(), 'version': '1.0'}
#     jout["results"] = {}
#     jout["datasets"] = {}
#     # Loop over all datasets
#     for dsid, pid in list(datasets_dict.items()):
#
#         ddir = os.path.join('/badc/cmip6/data/', dsid.replace('.', '/'))
#         if not os.path.exists(ddir):
#             with open(f'{missing_file}', 'a+') as w:
#                 w.writelines(f'{dsid}\n')
#             continue
#         if len(os.listdir(ddir)) == 0:
#             with open(f'{missing_file}', 'a+') as w:
#                 w.writelines(f'{dsid}\n')
#             continue
#
#         dataset_records = df[df.filepath.str.contains(dsid.replace('.', '/'))].reset_index(drop=True)
#         if not len(dataset_records) > 0:
#             with open(f'{missing_file}', 'a+') as w:
#                 w.writelines(f'QC_MISSING: {dsid}\n')
#             continue
#
#         json_ds_qc = {'error_severity': 'na', 'error_message': 'na'}
#         ds_qcStatus = "pass"
#
#         json_files = {}
#
#         for index, row in dataset_records.iterrows():
#             json_files[row.pid] = {'filename': os.path.basename(row.filepath),
#                                   'qc_status': ['fail' if not row.error_level else 'pass'][0],
#                                   'error_severity': row.cf_severity_level,
#                                   'error_message': [row.error_details if row.error_details else 'na'][0]
#                                   }
#             if not row.error_level == 'pass':
#                 ds_qcStatus = "fail"
#
#         jout["datasets"][pid] = {'dset_id': dsid, 'qc_status': ds_qcStatus, 'dataset_qc': json_ds_qc, 'files': json_files}
#
#     # Output JSON
#     json_obj = json.dumps(jout, indent=4)
#     with open(f"{ofile}", "a+") as o:
#         o.write(json_obj)


if __name__ == "__main__":

    main()

    # parser = argparse.ArgumentParser()
    # parser.add_argument('-d', '--datasets_list', type=str, help='A csv file of CMIP6 dataset ids and PIDS')
    # parser.add_argument('-e', '--dataframe', type=str, help='A pandas dataframe of CMIP6 to compare against')
    # parser.add_argument('-o', '--output_file', type=str, help='Ouput file')
    # parser.add_argument('-r', '--release_dir', type=str, help='Directory of release to directory output files')
    # args = parser.parse_args()
    #
    # if not os.path.exists(args.release_dir):
    #     print(args.release_dir)
    #     os.makedirs(release_dir)
    #
    # missing_file = os.path.join(args.release_dir, MISSING_FILENAME)
    # if os.path.exists(missing_file):
    #     os.remove(missing_file)
    #
    # ofile = os.path.join(args.release_dir, args.output_file)
    # if os.path.exists(ofile):
    #     os.remove(ofile)
    #
    # main(args.datasets_list, args.dataframe, missing_file, ofile)