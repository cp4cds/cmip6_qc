#!/usr/bin/env Python

import os
import logging
logging.basicConfig(format='[%(levelname)s]:%(message)s', level=logging.INFO)

CMIPDIR = '/badc/cmip6/data/'
BASEDIR = '/group_workspaces/jasmin2/cp4cds1/vol3/c3s_34g/cmip6_qc/'
QCPASSED_DATASETS = 'data/c3s_34g_release-1_passed_datasets.txt'

with open(os.path.join(BASEDIR, QCPASSED_DATASETS)) as r:
    datasets = [ line.strip() for line in r ]

for ds in datasets:
    # logging.info(f'INTEROGATING {ds}')

    for root, dirs, files in os.walk(os.path.join(CMIPDIR, ds.replace('.', '/'))):
        for file in files:
            filepath = os.path.join(root, file)
            if not os.path.isfile(filepath):
                logging.warning(f'NOT A FILE {filepath}')
            else:
                print(f'{filepath}')


