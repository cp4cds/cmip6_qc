#!/usr/bin/env Python

import os
import argparse
import requests
import json
import sys

ARCHDIR = '/badc/cmip6/data/'
# DATASETS_LIST = "/gws/nopw/j04/cp4cds1_vol3/c3s_34g/c3s_34g_qc_results/Catalogs/dataset-ids-pids_release2_202002_2.csv"
DATASETS_LIST = sys.argv[1]

def main():

    with open(DATASETS_LIST, 'r') as r:
        dss = [line.split(',')[0].strip() for line in r]

    for ds in dss:
        dataset_dir = os.path.join(ARCHDIR, ds.replace('.', '/'))
        if os.path.isdir(dataset_dir):
            print(f"DATASET EXISTS: {ds}")
        else:
            print(f"DATASET MISSING: {ds}")


if __name__ == "__main__":
    main()