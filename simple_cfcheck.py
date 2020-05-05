#!/usr/bin/env Python

import datetime
import os
import cfchecker.cfchecks as cfc
from netCDF4 import Dataset

nc_path = '/badc/cmip6/data/CMIP6/AerChemMIP/BCC/BCC-ESM1/hist-piNTCF/r1i1p1f1/Amon/tas/gn/v20190621/tas_Amon_BCC-ESM1_hist-piNTCF_r1i1p1f1_gn_185001-201412.nc'

class SimpleCFChecker(object):

    def __init__(self, verbose=False):
        self._checker = cfc.CFChecker(version=None, silent=True)
        self.verbose = verbose

    def run(self, nc_path, log_path):
        now = datetime.datetime.now().date().isoformat()
        handle, conventions = self.get_attributes(nc_path)
        res = self._checker.checker(nc_path)
        g = res['global']
        v = res['variables']
        dsid = os.path.dirname(nc_path).strip("/badc/cmip6/data/").replace('/', '.')

        errs = self.get_errors(dsid, nc_path, g, 'global', '', now, handle, conventions)

        for var_id in v.keys():
            errs.extend(self.get_errors(dsid, nc_path, v[var_id], 'variable', var_id, now, handle, conventions))

        if not errs:
            errs.append(f'{dsid}|{os.path.basename(nc_path)}|{handle}|{conventions}||||pass|{now}')
        self.log_errors(errs, log_path)

    def get_attributes(self, nc_path):
        """
        Returns a list of attributes
        :param nc_path: 
        :param attributes: []
        :return: 
        """
        
       
        dataset = Dataset(nc_path)
        for att in dataset.ncattrs():
            if att == "tracking_id": 
                tracking_id = getattr(dataset, att).strip()
            if att == "Conventions": 
                conventions = getattr(dataset, att).strip()

        return tracking_id, conventions
    


    def log_errors(self, errs, log_path):

        with open(log_path, 'a') as writer:
            for line in errs:
                writer.write(f'{line}\n')

    def get_errors(self, dsid, nc_path, rec, etype, detail, timestamp, pid, convention):
        errs = []
        for level in ['FATAL', 'ERROR', 'WARNING']:
             result = rec.get(level)
             if self.verbose or result:
                 errs.append(f'{dsid}|{os.path.basename(nc_path)}|{pid}|{convention}|{etype}|{detail}|{level}|{result}|{timestamp}')
        return errs

checker = SimpleCFChecker()
log_path = 'out.log'
for i in range(3):
    checker.run(nc_path, log_path)
with open(log_path) as reader:
    for line in reader:
        print(line)