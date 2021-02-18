#!/usr/bin/env Python

import os
import json

with open('../data/c3s_34g_release-1_passed_datasets.txt') as r:
    ds_ok = [ line.strip() for line in r ]

with open('../../c3s_34g_qc_results/QC_Results/QC_prepare.json') as jsn_file:
    results = json.loads(jsn_file.read())

errata_fails = set()
counter = 0

print(results.keys)

for handle in results.keys():
    if not results[handle]['qc_status'] == 'pass':
        dsid = results[handle]['dset_id'].strip()
        if dsid in ds_ok:
            print(f"WTF: {dsid} ")

