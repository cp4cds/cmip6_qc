#!/usr/bin/env Python

import os
import sys
import requests
import json
import subprocess

dsid = sys.argv[1]

cache='/gws/nopw/j04/esgf_repl/synda-cmip6/data/'
datanode = 'esgf-data.dkrz.de'
datanode = 'esgf-node.llnl.gov'

def main(dsid):

    print(dsid)

    cachedir = os.path.join(cache, dsid.replace('.', '/'))
    era, mip, ins, model, expt, ensemble, table, var, grid, version = dsid.split('.')
    url = f'https://{datanode}/esg-search/search/?offset=0&limit=1000&' \
          f'mip_era=CMIP6&activity_id={mip}&source_id={model}&experiment_id={expt}&' \
          f'member_id={ensemble}&table_id={table}&variable_id={var}&grid_label={grid}&' \
          f'type=File&replica=false&latest=true&fields=id,checksum&format=application%2Fsolr%2Bjson'
    print(url)
    resp = requests.get(url)
    content = resp.json()
    checksums = {}
    for res in content['response']['docs']:
        # print('.'.join(res['id'].split('|')[0].split('.')[10:]))
        # print(res['checksum'][0])

        fname = str('.'.join(res['id'].split('|')[0].split('.')[10:]))
        if not fname.endswith('.nc'):
            continue
        checksums[fname] = res['checksum'][0]

    total_files = len(list(checksums.items()))
    # total_files = len(content['response']['docs'])
    print(f'Files expected {total_files}')

    nfiles = len(os.listdir(cachedir))
    print(f'Files found {nfiles}')
    print(cachedir)

    if not total_files == nfiles:
        print(f'file number mismatch\nESGF nfiles {total_files}\nFilesystem files {nfiles}')
    for file in os.listdir(cachedir):
        cmd = f'sha256sum {os.path.join(cachedir, file)}'
        res = subprocess.getoutput(cmd)
        fs_checksum, filename = res.split('  ')[0], res.split('  ')[1].split('/')[-1]

        if not checksums[filename] == fs_checksum:
            print("ERROR on fs")


main(dsid)