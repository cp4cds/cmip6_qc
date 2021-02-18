#!/usr/bin/env Python

import os

# with open('../data/release2-beta/QC-required.txt') as r:
with open('../data/release2-beta/datasets_to_fetch.txt') as r:
    dataset_ids = [line.strip() for line in r]

for ds in dataset_ids:

    logfile = os.path.join('qc_logs/cf/', '.'.join(ds.split('.')[:7]).replace('.', '/'), ds+'.psv')
    if not os.path.isfile(logfile):
        print(ds)
