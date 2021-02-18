#!/usr/bin/env Python

import os
import json
import logging
import datetime as dt

today = dt.datetime.today().isoformat()[:10]
logging.basicConfig(format='[%(levelname)s]:%(message)s', level=logging.DEBUG)

QCDIR = '/gws/nopw/j04/cp4cds1_vol3/c3s_34g/cmip6_qc/data/release2-beta/'
MISSING_DATASETS = os.path.join(QCDIR, f'missing_ds_{today}.txt')
FAILED_DATASETS = os.path.join(QCDIR, f'failed_ds_{today}.txt')
PASSED_DATASETS = os.path.join(QCDIR, f'passed_ds_{today}.txt')
FILES = [MISSING_DATASETS, FAILED_DATASETS, PASSED_DATASETS]

for file in FILES:
    if os.path.exists(file):
        os.remove(file)


# CHECKS = ['cf', 'errata', 'nctime', 'prepare', 'range']


def read_qc_log(filename):
    logging.info(f"Reading file : {filename}")
    if os.path.exists(filename):
        with open(filename) as jsn_file:
            results = json.loads(jsn_file.read())
        return results
    else:
        return None


def write_log(missing_ds, ofile):
    with open(ofile, 'a+') as w:
        w.writelines(f"{missing_ds}\n")


def read_dsids_and_pids(ifile):
    logging.info(f"Reading file : {ifile}")
    dsids = {}
    with open(ifile, 'r') as r:
        for line in r:
            dsids[line.split(',')[1].strip()] = line.split(',')[0].strip()

    return dsids


def main():

    # Read all datasets for release 1
    release_datasets_file = '/gws/nopw/j04/cp4cds1_vol3/c3s_34g/cmip6_qc/data/release2-beta/datasets_list_2020-10.csv'
    ids = read_dsids_and_pids(release_datasets_file)
    pids = list(ids.keys())

    # load all logs
    CHECKS = []
    # cf_results = read_qc_log(os.path.join(QCDIR, 'QC_cfchecker.json'))
    # if cf_results: CHECKS.append('cf')
    errata_results = read_qc_log(os.path.join(QCDIR, 'QC_errata.json'))
    if errata_results: CHECKS.append('errata')
    # nctime_results = read_qc_log(os.path.join(QCDIR, 'QC_nctime.json'))
    # if nctime_results: CHECKS.append('nctime')
    # prepare_results = read_qc_log(os.path.join(QCDIR, 'QC_prepare.json'))
    # if prepare_results: CHECKS.append('prepare')
    # range_results = read_qc_log(os.path.join(QCDIR, 'QC_rangecheck.json'))
    # if range_results: CHECKS.append('ranges')
    # handle_results = read_qc_log(os.path.join(QCDIR, 'QC_handles.json'))
    # if handle_results: CHECKS.append('handle')

    logging.info(f'CHECKS: {CHECKS}')

    for pid in pids[2:3]:
        logging.info(f"Interogating {pid}")
        missing_datasets = set()
        results_status = {}

        for check in CHECKS:
            logging.debug(f'QC CHECK {check}')
            try:
                print(eval(f"{check}_results")['datasets'][pid])
                results_status[check] = eval(f"{check}_results")['datasets'][pid]
                logging.debug(f" {results_status[check]}")

            except Exception as error:
                logging.debug(f"Error in PID : {error}")
                missing_datasets.add(pid)

        if len(missing_datasets) > 0:
            write_log(f"{pid, ids[pid]}", MISSING_DATASETS)

        else:
            ds_status = {}
            for check in CHECKS:
                results_status[check] = eval(f"{check}_results")['datasets'][pid]
                ds_status[check] = results_status[check]['qc_status']

            for qc, status in ds_status.items():
                if not status == 'pass':
                    info.debug(f'dataset failed {qc, status}')
                    write_log(ds_status, FAILED_DATASETS)
                    break
                else:
                    write_log(f"{pid, ids[pid]}", PASSED_DATASETS)


if __name__ == "__main__":

    main()