import os
import pandas as pd
import glob
import json
import argparse
import requests

# import hddump
# from b2handle.handleclient import EUDATHandleClient

basedir = '/group_workspaces/jasmin2/cp4cds1/vol3/c3s_34g/cmip6_qc/qc_logs/cf/CMIP6/'
# 'AerChemMIP/BCC/BCC-ESM1/ssp370/r1i1p1f1/Amon'
# COLUMNS = 'filepath pid cfversion timestamp level var_id errtype  error logfile '.split()
COLUMNS = 'filepath pid cfversion timestamp error_level error_type var_id error_details logfile '.split()
CMIP6_DF_AR6 = "../data/cmip6-ar6wg1-cf-df.pkl"
ERRORS_DF_AR6 = "../data/cmip6-ar6wg1-cf-errors-df.pkl"
CMIP6_DF = "../data/cmip6-cf-df.pkl"
ERRORS_DF = "../data/cmip6-cf-errors-df.pkl"
PRIORITY_VARS_FILE = "../data/AR6WG1_priorityVariables.json"
odir = "/group_workspaces/jasmin2/cp4cds1/vol3/c3s_34g/c3s_34g_qc_results/QC_results/CF"
PIDBASE = "http://hdl.handle.net/"
CF_results_path = "../../c3s_34g_qc_results/QC_results/CF/"


#
#
# def _get_handle_by_handle_string(handle_string, handle_client_instance):
#     """Using the EUDATHandle service, this function reads the required handle using the handle_string.
#     :param handle_string: String
#     :param handle_client_instance: EUDATClient instance
#     :returns: json formatted handle
#     :rtype: str
#     """
#     encoded_dict = handle_client_instance.retrieve_handle_record(handle_string)
#     if encoded_dict is not None:
#         handle_record = {k.decode('utf8'): v.decode('utf8') for k, v in encoded_dict.items()}
#         return handle_record
#     else:
#         raise exceptions.HandleNotFoundError
#
#
# def _get_parent(input_handle_string):
#     """Given a handle, this will harvest all the errata data related to that handle as well as the previous versions.
#     :param input_handle_string: Handle identifier
#     :return: errata information, dset/file_id
#     """
#     handle_client = EUDATHandleClient.instantiate_for_read_access()
#     handle = _get_handle_by_handle_string(input_handle_string, handle_client)
#     print(handle['IS_PART_OF'])


def get_parent(handle):
    url = f"http://hdl.handle.net/api/handles/{handle.lstrip('hdl:')}"
    resp = requests.get(url)
    res = resp.json()
    if not res:
        return False
    if not res['responseCode'] == 1:
        return False
    return res['values'][5]['data']['value']


def _read(log_file):
    return pd.read_csv(log_file, sep='|', dtype=str, header=None, names=COLUMNS, na_values='')


def filter_df_to_ar6wg1(df):
    df_new = pd.DataFrame([], list(df.columns))

    with open(PRIORITY_VARS_FILE) as json_file:
        ar6wg1 = json.load(json_file)

    for exp in ar6wg1["requested"].keys():
        print(exp)
        for table, vars in ar6wg1['requested'][exp].items():
            print(table)
            for v in vars:
                print(v)
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
        df.apply(lambda row: 'data' if row.var_id == row.filepath.split('/')[-1].split('_')[0].strip() else 'other',
                 axis=1)
    df['cf_severity_level'] = 'Not yet defined'
    df['cf_severity_level'][df['error_level'] == 'pass'] = 'Good'

    df_filtered = filter_df_to_ar6wg1(df)

    df_filtered.to_pickle(CMIP6_DF)

    errors_dataframe = df_filtered[df_filtered['error_level'] == 'ERROR'].reset_index()
    errors_dataframe.to_pickle(ERRORS_DF)
    return df_filtered


def merge_logs(basedir):
    files = glob.glob(f'{basedir}/*/*/*/*psv')
    _dfs = [_read(_) for _ in files]
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
    global_errs_by_model = gl_errs.groupby([gl_errs.error_details, gl_errs.model]).index.count().reset_index().pivot(
        columns='model', index='error_details', values='index')
    global_errs_by_model.to_csv('../data/global_errors_by_model.csv')


def variable_error_types(df):
    var_errs = df[(df.error_level == 'ERROR') & (df.error_type == 'variable')].reset_index()
    variable_errors_by_id = var_errs.groupby(
        [var_errs.error_details, var_errs.var_id]).index.count().reset_index().rename(columns={'index': 'count'})
    variable_errors_by_id.to_csv('../data/variable_errors.csv')


def number_of_variables_by_type(df, vtype):
    errs = df[df['error_var_type'] == vtype].reset_index()
    print(f"{vtype} \n {errs.groupby(errs.error_details).index.count()}")


def write_release1_to_json(df):
    # Get release 1 pids
    with open("../../c3s_34g_qc_results/Catalogs/c3s34g-release1-datasets_v0.csv", 'r') as r:
        datasets = [line.split(',')[0].strip() for line in r]

    ofilename = f"CF_results_release1.json"
    print(os.path.join(CF_results_path, ofilename))
    jout = {}
    jout["header"] = {"application:": 'CF-checker', 'institution': 'CEDA'}

    jsn = {}
    for ds in datasets[:1]:
        print(ds)
        dss_rec = df[df.filepath.str.contains(ds.replace('.', '/'))].reset_index(drop=True)
        print(dss_rec)
        ds_status = []
        for ds_rec in dss_rec:
            ds_status.append(dss_rec[dss_rec.error_level])
        if "ERROR" in ds_status:
            dsStatus = 'ERROR'
        else:
            dsStatus = 'pass'

        for index, row in res.iterrows():
            jsn[row.pid] = {'filename': {os.path.basename(row.filepath)},
                            'timestamp': row.timestamp,
                            'qc_status': row.error_level,
                            'qc_message': row.error_details}

        jsn_ds[get_parent(row.pid)] = {"dset_id": ds, "qc_status": dsStatus, "files": jsn}

    jout["results"] = jsn_ds
    json_obj = json.dumps(jout, indent=4)
    # with open(f"{os.path.join(CF_results_path, ofilename)}", "a+") as o:
    with(open(test.json, 'a+')) as o:
        o.write(json_obj)

    # df_mip_model = df_model[df_model.filepath.str.contains(filter_on_mip)].reset_index(drop=True)

    # uniq_ds = df_mip_model.dataset_id.unique()
    #
    # jsn = {}
    # for ds in uniq_ds:
    #     res = df_mip_model[df_mip_model.dataset_id == ds].reset_index(drop=True)
    #     for index, row in res.iterrows():
    #         jsn[row.pid] = {'dset_id': row.dataset_id, 'timestamp': row.timestamp, 'qc_status': row.error_level,
    #                         'qc_message': row.error_details}
    #
    # jout = {}
    # jout["header"] = {"application:": 'CF-checker', 'institution': 'CEDA'}
    # jout["results"] = jsn
    # json_obj = json.dumps(jout, indent=4)
    # ofilename = f"CF_results_{filter_on_mip}_{filter_on_model}.json"
    # print(os.path.join(CF_results_path, ofilename))
    # with open(f"{os.path.join(CF_results_path, ofilename)}", "w+") as o:
    #     o.write(json_obj)
    #


def write_to_json(df):
    filter_on_model = 'GISS-E2-1-G'
    filter_on_mip = "ScenarioMIP"
    # df_mip_model = df[(df.model == filter_on_model) and (df.filepath.str.contains('ScenarioMIP'))].reset_index(drop=True)
    df_model = df[df.model == filter_on_model].reset_index(drop=True)
    df_mip_model = df_model[df_model.filepath.str.contains(filter_on_mip)].reset_index(drop=True)

    uniq_ds = df_mip_model.dataset_id.unique()

    jsn = {}
    for ds in uniq_ds:
        res = df_mip_model[df_mip_model.dataset_id == ds].reset_index(drop=True)
        for index, row in res.iterrows():
            jsn[row.pid] = {'dset_id': row.dataset_id, 'timestamp': row.timestamp, 'qc_status': row.error_level,
                            'qc_message': row.error_details}

    jout = {}
    jout["header"] = {"application:": 'CF-checker', 'institution': 'CEDA'}
    jout["results"] = jsn
    json_obj = json.dumps(jout, indent=4)
    ofilename = f"CF_results_{filter_on_mip}_{filter_on_model}.json"
    print(os.path.join(CF_results_path, ofilename))
    with open(f"{os.path.join(CF_results_path, ofilename)}", "w+") as o:
        o.write(json_obj)


def filter_cmip6_df(df):
    # df['dataset_id'] = df.filepath.apply(lambda path: '.'.join(path.split('/')[4:14]))
    df_filtered = filter_df_to_ar6wg1(df)
    df_filtered.to_pickle(CMIP6_DF_AR6)
    errors_dataframe = df_filtered[df_filtered['error_level'] == 'ERROR'].reset_index()
    errors_dataframe.to_pickle(ERRORS_DF_AR6)

    return df_filtered


def write_markdown(error_egs, error_cat):
    md_file = "md_summaries.md"
    with open(md_file, 'w+') as w:
        w.writelines(f"# {error_cat} \n\n")
        w.writelines(f"# {error_egs[0].error_details.strip('[').strip(']')}\n\n")

        for eg in error_egs:
            w.writelines(f"PATH : {os.path.basename(eg.filepath)}\n\n")
            pid = eg.pid.split(':')[1]
            w.writelines(f"PID : [{pid}]({PIDBASE}{pid})")
            w.writelines("\n\n```\n")
            ncdump = f"ncdump -h {eg.filepath}"
            w.writelines(f"{os.popen(ncdump).read()}")
            w.writelines("\n```\n\n")


def interrogate_errors(df):
    gl = df[df.error_var_type == 'global'].reset_index(drop=True)
    gerrs_u = gl.error_details.unique()
    gerrs = gl[gl.error_details.isin(gerrs_u)].reset_index(drop=True)
    # models = gerrs.model.unique()

    # write_to_json(df)
    # write_markdown([gerrs.iloc[0]], "Global Errors")

    # global_error_egs = []
    # for m in list(models)[:1]:
    #     gerr_for_model = gerrs[gerrs.model == m].reset_index(drop=True)#
    #     gerr_for_model['expt'] = gerr_for_model.filepath.apply(lambda s: s.split('/')[8].strip())
    #     expts = gerr_for_model.expt.unique()
    #     for e in list(expts):
    #         expt_example = gerr_for_model[gerr_for_model.expt == e].reset_index(drop=True).iloc[0]
    #
    #         write_markdown(expt_example)
    #


def filter_df(df):

    variant = 'r1i1p1f1'
    df['dataset_id'] = df.filepath.apply(lambda path: '.'.join(path.split('/')[4:14])).reset_index(drop=True)
    df = df[df.dataset_id.str.contains(variant)].reset_index(drop=True)
    df.to_pickle("../data/cmip6-ar6wg1-cf-errors-r1i1p1f1.pkl")
    df.to_csv("../data/cmip6-ar6wg1-cf-errors-r1i1p1f1.csv")

    # s = df.iloc[0]
    # ens = s[s['dataset_id']].str.contains('r1i1p1f1')
    # print(ens)

def main(dataframe):

    df = pd.read_pickle(dataframe)
    filter_df(df)


if __name__ == "__main__":

    dataframe = "../data/cmip6-ar6wg1-cf-errors-df.pkl"
    main(dataframe)
