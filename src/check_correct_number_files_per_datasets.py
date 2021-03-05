#!/usr/bin/env Python

import os
import json
import requests
import logging
logging.basicConfig(format='[%(levelname)s]:%(message)s', level=logging.INFO)
RELEASE_FILE = '../data/release2/dataset-ids-pids_release2_202002.csv'
# RELEASE_FILE = '../data/release2/ceda-dataset-errors/all-ceda-replicas.txt'
INCONSISTENT_PIDS = 'ceda-dataset-pid-inconsistencies.out'
DATANODE = 'esgf-data.dkrz.de'

def get_esgf_data(res):
    return [res['data_node'], res['number_of_files']]

def main():
    with open(INCONSISTENT_PIDS) as r:
        ds_pid = [line.strip() for line in r]

        ds_ids = {}
    with open(RELEASE_FILE) as r:
        for line in r:
            _id = line.split(',')[0].strip()
            _pid = line.split(',')[1].strip()
            ds_ids[_pid] = _id

    for ds in ds_pid:

        logging.debug(f'Dataset pid {ds}')
        id = ds_ids[ds]
        # id = 'CMIP6.ScenarioMIP.CAS.FGOALS-g3.ssp460.r1i1p1f1.Amon.prsn.gn.v20191216'
        logging.debug(f'Dataset id {id}')

        era, mip, inst, model, expt, ens, table, var, grid, version = id.split('.')
        url = f"https://{DATANODE}/esg-search/search?type=Dataset&mip_era=CMIP6&" \
              f"activity_id={mip}&source_id={model}&experiment_id={expt}&" \
              f"member_id={ens}&table_id={table}&variable_id={var}&" \
              f"distrib=true&latest=true&retracted=false&" \
              f"fields=id,number_of_files,replica,data_node&" \
              f"format=application%2Fsolr%2Bjson&limit=10000"

        resp = requests.get(url)
        content = resp.json()
        results = content['response']['docs']
        nresults = len(results)

        master_done = False
        master_data = [get_esgf_data(res) for res in results if not res['replica']]
        master_node = master_data[0][0]
        master_nfiles = master_data[0][1]
        if not master_node:
            master_data = 'unknown'
            master_nfiles = 9999
            logging.warning(f'Master not found: {url}')
        else:
            master_done = True

        ceda_done = False
        ceda_data = [get_esgf_data(res) for res in results if res['data_node'] == 'esgf-data3.ceda.ac.uk']
        ceda_nfiles = ceda_data[0][1]
        if not ceda_nfiles:
            logging.warning(f'{id} not found at CEDA')
        else:
            ceda_done = True

        if master_done and ceda_done:
            if not master_nfiles == ceda_nfiles:
                print(f'{id}: FILE NUMBER MISMATCH CEDA {ceda_nfiles} MASTER {master_node}: {master_nfiles}')


        # url = f"https://{DATANODE}/esg-search/search/?type=Dataset&query=" \
        #       f"instance_id%3ACMIP6.ScenarioMIP.NASA-GISS.GISS-E2-1-G.ssp126.r1i1p1f2.Amon.ua.gn.v20200115&mip_era=CMIP6&activity_id%21=input4MIPs&facets=mip_era%2Cactivity_id%2Cmodel_cohort%2Cproduct%2Csource_id%2Cinstitution_id%2Csource_type%2Cnominal_resolution%2Cexperiment_id%2Csub_experiment_id%2C" \
        #       f"variant_label%2Cgrid_label%2Ctable_id%2Cfrequency%2Crealm%2Cvariable_id%2Ccf_standard_name%2Cdata_node&" \
        #       "latest=true&offset=0&limit=10&format=application%2Fsolr%2Bjson"
        #

main()