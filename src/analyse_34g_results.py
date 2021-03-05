#!/usr/bin/env Python

import os
import pandas as pd
import glob
import json
import argparse
import requests
import settings
import re
import subprocess

REUSLTS_DIR = '/gws/nopw/j04/cp4cds1_vol3/c3s_34g/cmip6_qc/data/release2/'
COLUMNS = 'filepath pid cfversion timestamp error_level error_type var_id error_details logfile '.split()
RESULTS_FILE = 'cmip6-c3s34g-cf-df.pkl'


def main():

    rfile = os.path.join(REUSLTS_DIR, RESULTS_FILE)
    df = pd.read_pickle(rfile)
    df = df[df['filepath'].notna()].reset_index(drop=True)
    #
    # passes = df.loc[df['cf_severity_level'] == 'pass']
    # major = df.loc[df['cf_severity_level'] == 'major']
    # minor = df.loc[df['cf_severity_level'] == 'minor']
    # other = df.loc[(df['cf_severity_level'] != 'minor') & (df['cf_severity_level'] != 'major') & (df['cf_severity_level'] != 'pass')]
    #
    # print(len(df))
    # print(len(passes), len(major), len(minor))
    # print(len(passes)+len(major)+len(minor))

    xdf = df[(df['cf_severity_level'] != 'pass') & (df['cf_severity_level'] != 'major') & (df['cf_severity_level'] != 'minor')]

    for row in xdf.rows:
        print(row['cf_error_severity'], row['error_details'])

    cdf=df.loc[~df['cf_severity_level'].isin(['pass', 'major', 'minor'])]
    for idx, row in cdf.iterrows():
        print(idx, row['cf_severity_level'], row['error_details'])

if __name__ == "__main__":
    main()