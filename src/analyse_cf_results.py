
import os
import pandas as pd
import glob


basedir = '/group_workspaces/jasmin2/cp4cds1/vol3/c3s_34g/cmip6_qc/qc_logs/cf/CMIP6/'
# 'AerChemMIP/BCC/BCC-ESM1/ssp370/r1i1p1f1/Amon'
COLUMNS = 'drs filename pid cfversion errtype var_id level error timestamp'.split()
CMIP6_DF = "cmip6-cf-df.pkl"

def _read(log_file):
    return pd.read_csv(log_file, sep='|', dtype=str, header=None, names=COLUMNS, na_values='')


def merge_logs(basedir):

    files = glob.glob(f'{basedir}/*/*/*/*psv')
    _dfs = [ _read(_) for _ in files ]
    df = pd.concat(_dfs)
    return df


def show(df):
    for row in df.iterrows():
        print(row)


def interrogate(df):

    print(f'Unique errors: {df.error.unique()} {df.error.unique()}')
    print(f'N passed: {len(df[df["error"] == "pass"])}')
    print(f"TOTAL {len(df)}")
    # print('\nErrors:')
    # for rec in df[df['error'] != 'pass'].iterrows():
    #     print(f'\n{rec}')


def create_cmip6_df():
    df = merge_logs(basedir)
    df.to_pickle(CMIP6_DF)


def main():
    df = pd.read_pickle(CMIP6_DF)
    # show(df)
    interrogate(df)
    # return df


if __name__ == "__main__":
    main()