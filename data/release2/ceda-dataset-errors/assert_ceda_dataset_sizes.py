
import os
import argparse
import requests
import logging
import json
import sys

ARCHDIR = '/badc/cmip6/data/'
CACHE_DIR = '/gws/nopw/j04/esgf_repl/synda-cmip6/data/'
# DATASETS_LIST = "/gws/nopw/j04/cp4cds1_vol3/c3s_34g/c3s_34g_qc_results/Catalogs/dataset-ids-pids_release2_202002_2.csv"
DATASETS_LIST = sys.argv[1]
CORRECT_SIZES = 'fix_dataset_correct_sizes.txt'


def _setup():
    logging.basicConfig(format='[%(levelname)s]:%(message)s', level=logging.INFO)


def main():

    with open(DATASETS_LIST, 'r') as r:
        dss = [line.split(',')[0].strip() for line in r]

    sizes_dict = {}
    with open(CORRECT_SIZES) as r:
        for line in r:
            sizes_dict[line.split(':')[0].strip()] = line.split(':')[1].strip()

    for ds in dss:
        archive_ds_dir = os.path.join(ARCHDIR, ds.replace('.', '/'))
        archive_ds_dir_nfiles = len(os.listdir(archive_ds_dir))
        cache_ds_dir = os.path.join(CACHE_DIR, ds.replace('.', '/'))
        cache_ds_dir_nfiles = len(os.listdir(cache_ds_dir))

        if os.path.exists(cache_ds_dir):
            if not cache_ds_dir_nfiles == int(sizes_dict[ds]):
                print(f'{ds} {cache_ds_dir_nfiles}: {sizes_dict[ds]}')
            # print(f'{ds}: {cache_ds_dir_nfiles} {sizes_dict[ds]}')


        # if sizes_dict[ds] == cache_ds_dir_nfiles:
        #     print(f'{ds} ARCHIVE HAS CORRECT NUMBER OF FILES')

        #
        # if os.path.isdir(archive_ds_dir):
        #     print(f"{ds} ARCHIVEDIR EXISTS: {len(os.listdir(archive_ds_dir))}")
        # else:
        #     print(f"{ds} ARCHIVEDIR MISSING")
        # if os.path.isdir(cache_ds_dir):
        #     print(f"{ds} CACHEDIR EXISTS: {len(os.listdir(archive_ds_dir))}")
        # else:
        #     print(f"{ds} CACHEDIR MISSING")


if __name__ == "__main__":
    _setup()
    main()