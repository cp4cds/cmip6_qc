#!/usr/bin/env Python
"""
Purpose is to ensure that for all datasets there is a CF record
"""

import os
import sys

ifile = sys.argv[1]
"""
ifile = '../data/release2-beta/QC-required.txt'
Input is simply a list of dataset ids
"""

with open(ifile) as r:
    dataset_ids = [line.strip() for line in r]

for ds in dataset_ids:

    logfile = os.path.join('qc_logs/cf/', '.'.join(ds.split('.')[:7]).replace('.', '/'), ds+'.psv')
    if not os.path.isfile(logfile):
        print(ds)
