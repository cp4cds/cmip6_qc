#!/usr/bin/env Python

import os
import argparse
import requests
import json
ARCHDIR = '/badc/cmip6/data/'
DATASETS_LIST = "/gws/nopw/j04/cp4cds1_vol3/c3s_34g/c3s_34g_qc_results/Catalogs/c3s34g-release1-datasets_v0.csv"
MISSING_DATA = "release2_datasets_not_at_ceda_2021-01-19.out"
def main():

    with open(DATASETS_LIST, 'r') as r:
        dss = [line.split(',')[0].strip() for line in r]

    with open(MISSING_DATA, 'r') as r:
        missing_dss = [line.strip() for line in r]

    for ds in dss:

        if ds in missing_dss:
            continue
        else:
            cf_log_dir = "/gws/nopw/j04/cp4cds1_vol3/c3s_34g/cmip6_qc/src/qc_logs/cf/"
            ds_dir = '.'.join(ds.split('.')[:7])
            log_dir = os.path.join(cf_log_dir, ds_dir.replace('.', '/'))
            cf_log_file = os.path.join(log_dir, ds+'.psv')

            if not os.path.exists(cf_log_file):
                print(ds)



if __name__ == "__main__":
    main()