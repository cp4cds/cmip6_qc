#!/usr/bin/env Python

import os
import argparse
import requests
import json
ARCHDIR = '/badc/cmip6/data/'
DATASETS_LIST = "/gws/nopw/j04/cp4cds1_vol3/c3s_34g/c3s_34g_qc_results/Catalogs/c3s34g-release1-datasets_v0.csv"

def main():

    with open(DATASETS_LIST, 'r') as r:
        dss = [line.split(',')[0].strip() for line in r]

    for ds in dss:
        dataset_dir = os.path.join(ARCHDIR, ds.replace('.', '/'))

        if not os.path.isdir(dataset_dir):
            print(f"DATASET NOT AT CEDA {ds}")
        # else:
        #     print(f"OK {ds}")


if __name__ == "__main__":
    main()