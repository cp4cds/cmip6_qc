#!/usr/bin/env Python

"""
A json release template will be provided for you (could make this an argument rather than hardcode the name.
You will need the datasets id and pids .csv file also provided for you
and the output of generate_c3s-34g_dataframe.py

This code loops over the template in its current form (i.e. if the dictonary levels change in the template this
code may need to be updated)

This code records all the CF file-level pass/fail information as well as aggregating the file level
CF results into a dataset level pass/fail. The results are written to an output json file.

The output file should be passed back to the 34g team via the other github repo.
"""


import os
import pandas as pd
import json
import argparse
import requests
import logging
from datetime import datetime as dt
logging.basicConfig(format='[%(levelname)s]:%(message)s', level=logging.INFO)



QCTEMPLATE = '../data/release3/QC_template_v5_2021-03-25.json'
DATASET_ID_PIDS_FILE = '../data/release3/dataset-ids-pids_release2_20200317.csv'
CF_RESULTS_FILE = '../data/release3/cmip6-c3s34g-cf-df_2021-04-11.pkl'
QC_OUTPUT = '../data/release3/QC_cfchecker.json'
PASSES = ['pass', 'minor']


def main():

    # OPEN FILES
    with open(QCTEMPLATE) as jsn:
        qc_template = json.load(jsn)

    json_keys = list(qc_template["datasets"].keys())
    logging.debug(f'Number of dataset ids: {len(json_keys)}')

    datasets_dict = get_dataset_pids(DATASET_ID_PIDS_FILE)
    cfres_dfs_df = pd.read_pickle(CF_RESULTS_FILE)
    cfres_dfs_df = cfres_dfs_df[cfres_dfs_df['filepath'].notna()].reset_index(drop=True)
    logging.debug(f'CF results dataframe size: {len(cfres_dfs_df)}')

    cf_dataset_dsids = list(cfres_dfs_df['dataset_id'].dropna().unique())
    logging.debug(f'CF dataset ids list: {len(cf_dataset_dsids)}')

    # Complete header information
    qc_template['header']["application:"] = 'CF-checker'
    qc_template['header']["Author"] = 'Ruth Petrie'
    qc_template['header']["Institution"] = 'CEDA'
    qc_template['header']["Date"] = '2021-02-17'
    qc_template['header']["version"] = '1.0'

    # Loop over qc_template entries
    for ds_pid in list(json_keys)[:]:
        ds_id = datasets_dict[ds_pid]
        logging.debug(f'Dataset pid {ds_pid}\nDataset id {ds_id}')
        logging.debug(f'/badc/cmip6/data/{ds_id.replace(".","/")}/')
        if not ds_id in list(cf_dataset_dsids):
            logging.debug(f'Dataset id not in CEDA set {ds_id}')
            continue

        dataset_df = cfres_dfs_df.loc[cfres_dfs_df['dataset_id'] == ds_id]
        ds_results = dataset_df.cf_severity_level
        dataset_qc_result = ds_results.isin(PASSES).all()
        if dataset_qc_result:
            qc_template["datasets"][ds_pid]["dset_qc_status"] = 'pass'
        else:
            qc_template["datasets"][ds_pid]["dset_qc_status"] = 'fail'
        # template_ds_entry = qc_template["datasets"][ds_pid]["qc_status"]
        # template_ds_entry = ds_qc_status
        logging.debug(f'DATASET QC STATUS {qc_template["datasets"][ds_pid]["dset_qc_status"]}')
        logging.debug(f'TEMPLATE ENTRY {qc_template["datasets"][ds_pid]}')
        f_pids = list(qc_template["datasets"][ds_pid]['files'].keys())

        logging.debug(f'File pids: {f_pids}')

        for fpid in f_pids:
            logging.debug(f'fpid {fpid}')
            fres_df = dataset_df[dataset_df['pid'] == fpid]
            if fres_df.empty:
                logging.info(f'FILE DATAFRAME EMPTY PID MISMATCH {ds_id, ds_pid, fpid, }')
                qc_template["datasets"][ds_pid]["dset_qc_status"] = 'fail'
                continue

            else:
                pid_cf_results_dict = dict(zip(fres_df.error_details, fres_df.cf_severity_level))
                err_msgs = list(pid_cf_results_dict.keys())
                severities = list(pid_cf_results_dict.values())
                template_entry = qc_template["datasets"][ds_pid]['files'][fpid]
                template_entry["file_error_severity"] = '; '.join(str(_) for _ in severities) # display as list severities
                template_entry["file_error_message"] = '; '.join(str(_) for _ in err_msgs) # display as list err_msgs
                if all(x in PASSES for x in severities):
                    template_entry["file_qc_status"] = 'pass'
                else:
                    template_entry["file_qc_status"] = 'fail'

    # Output JSON
    with open(QC_OUTPUT, "w+") as jo:
        json.dump(qc_template, jo, indent=4)
















        # for index, row in cfres_dfs_df.iterrows():
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
            d[line.split(',')[1].strip()] = line.split(',')[0].strip()
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