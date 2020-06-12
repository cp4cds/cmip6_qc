
import os
import pandas as pd
import glob
import json
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)


basedir = '/group_workspaces/jasmin2/cp4cds1/vol3/c3s_34g/cmip6_qc/qc_logs/cf/CMIP6/'
# 'AerChemMIP/BCC/BCC-ESM1/ssp370/r1i1p1f1/Amon'
# COLUMNS = 'filepath pid cfversion timestamp level var_id errtype  error logfile '.split()
COLUMNS = 'filepath pid cfversion timestamp error_level error_type var_id error_details logfile '.split()
CMIP6_DF_AR6 = "../data/cmip6-ar6wg1-cf-df.pkl"
ERRORS_DF_AR6 = "../data/cmip6-ar6wg1-cf-errors-df.pkl"
CMIP6_DF = "../data/cmip6-cf-df.pkl"
ERRORS_DF = "../data/cmip6-cf-errors-df.pkl"
PRIORITY_VARS_FILE = "../data/AR6WG1_priorityVariables.json"

def _read(log_file):
    return pd.read_csv(log_file, sep='|', dtype=str, header=None, names=COLUMNS, na_values='')


def filter_df_to_ar6wg1(df):

    df_new = pd.DataFrame([], list(df.columns))

    with open(PRIORITY_VARS_FILE) as json_file:
        ar6wg1 = json.load(json_file)

    for exp in ar6wg1["requested"].keys():
        for table, vars in ar6wg1['requested'][exp].items():
            for v in vars:
                expt = df[df.filepath.str.contains(exp)].reset_index(drop=True)
                tab = expt[expt.filepath.str.contains(table)].reset_index(drop=True)
                var = tab[tab.filepath.str.contains(v)].reset_index(drop=True)
                df_new = df_new.append(var, ignore_index=True)

    return df_new

def create_cmip6_df():


    df = merge_logs(basedir)
    df['model'] = df.filepath.apply(lambda s: s.split('/')[7].strip())
    df['dataset_id'] = df.filepath.apply(lambda path: '.'.join(path.split('/')[4:14]))
    df['error_var_type'] = 'N/A'
    df['error_var_type'][(df['error_level'] == 'ERROR') & (df['error_type'] == 'global')] = 'global'
    df['error_var_type'][(df['error_level'] == 'ERROR') & (df['error_type'] == 'variable')] = \
        df.apply(lambda row: 'data' if row.var_id == row.filepath.split('/')[-1].split('_')[0].strip() else 'other', axis=1)
    df['cf_severity_level'] = 'Not yet defined'
    df['cf_severity_level'][df['error_level'] == 'pass'] = 'Good'

    df_filtered = filter_df_to_ar6wg1(df)

    df_filtered.to_pickle(CMIP6_DF)

    errors_dataframe = df_filtered[df_filtered['error_level'] == 'ERROR'].reset_index()
    errors_dataframe.to_pickle(ERRORS_DF)
    return df_filtered

def merge_logs(basedir):

    files = glob.glob(f'{basedir}/*/*/*/*psv')
    _dfs = [ _read(_) for _ in files ]
    df = pd.concat(_dfs)
    return df


def show(df):

    for row in df.iterrows():
        print(row)

def interrogate(df):

    print(f'N passed: {len(df[df["level"] == "pass"])}')
    print(f"TOTAL {len(df)}")
    print('\nErrors:')
    for rec in df[df['error'] != 'pass'].iterrows():
        print(f'\n{rec}')
    
def global_errors(df):
    
    gl_errs = df[(df.error_level == 'ERROR') & (df.error_type == 'global')].reset_index()
    global_errs_by_model = gl_errs.groupby([gl_errs.error_details, gl_errs.model]).index.count().reset_index().pivot(columns='model', index='error_details', values='index')
    global_errs_by_model.to_csv('../data/global_errors_by_model.csv')

def variable_error_types(df):
    
    var_errs = df[(df.error_level == 'ERROR') & (df.error_type == 'variable')].reset_index()
    variable_errors_by_id = var_errs.groupby([var_errs.error_details, var_errs.var_id]).index.count().reset_index().rename(columns={'index': 'count'})
    variable_errors_by_id.to_csv('../data/variable_errors.csv')


def number_of_variables_by_type(df, vtype):
    
    errs = df[df['error_var_type'] == vtype].reset_index()
    print(f"{vtype} \n {errs.groupby(errs.error_details).index.count()}")


def write_to_json(df):

    print(df.iloc[0].filepath)

    df['dataset_id'] = df.filepath.apply(lambda path: '.'.join(path.split('/')[4:14]))
    ds_g = df.groupby(['dataset_id', 'model'])
    # for file, group in ds_g:


    # print(df.iloc[0].dataset_id)
    # for res in df.iterrows():


def main():

    create = False
    if create:
        df = create_cmip6_df()
    else:
        df = pd.read_pickle(CMIP6_DF)
        # df = pd.read_pickle(ERRORS_DF)

    filter = True
    if filter:
        df['dataset_id'] = df.filepath.apply(lambda path: '.'.join(path.split('/')[4:14]))
        df_filtered = filter_df_to_ar6wg1(df)
        df_filtered.to_pickle(CMIP6_DF_AR6)
        errors_dataframe = df_filtered[df_filtered['error_level'] == 'ERROR'].reset_index()
        errors_dataframe.to_pickle(ERRORS_DF_AR6)



    # write_to_json(df)
    # asdf
    #
    # for var_type in ["global", "data", "other"]:
    #     number_of_variables_by_type(df, var_type)
    # # print(df.iloc[1])
    # variable_errors(df)
    # global_errors(df)
    # variable_error_types(df)
    # interrogate(df)
    # return df


if __name__ == "__main__":
    main()



