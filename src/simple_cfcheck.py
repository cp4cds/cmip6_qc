#!/usr/bin/env Python
"""
This is the CF checker module for use in the quality control of CMIP6 data.

The CF logs are written to a CF_logs directory, the logs are parsed
and condensed with only the key bits of information written out as a pipe separated
value (psv) file. This is for ease of future processing.

TODO Update the CF log dir to pick this up from settings
"""


import datetime
import os
import subprocess
import cfchecker.cfchecks as cfc
from netCDF4 import Dataset


CF_LOG_DIR = "/gws/nopw/j04/cp4cds1_vol3/c3s_34g/cmip6_qc/cf_logs/"
good_nc_path = '/badc/cmip6/data/CMIP6/AerChemMIP/BCC/BCC-ESM1/hist-piNTCF/r1i1p1f1/Amon/tas/gn/v20190621/tas_Amon_BCC-ESM1_hist-piNTCF_r1i1p1f1_gn_185001-201412.nc'
bad_nc_path = "/badc/cmip6/data/CMIP6/DCPP/EC-Earth-Consortium/EC-Earth3/dcppA-hindcast/s2000-r1i1p1f1/day/tasmax/gr/latest/tasmax_day_EC-Earth3_dcppA-hindcast_s2000-r1i1p1f1_gr_20101101-20111031.nc"

class SimpleCFChecker(object):

    def __init__(self, verbose=False):
        self._checker = cfc.CFChecker(version=None, silent=True)
        self.verbose = verbose

    def run(self, nc_path, log_path):
        now = datetime.datetime.now().date().isoformat()
        pid_handle, conventions = self.get_attributes(nc_path)
        res = self._checker.checker(nc_path)
        g = res['global']
        v = res['variables']
        cf_dir = os.path.dirname(nc_path).strip('/badc/cmip6/data')
        cf_filename = os.path.basename(nc_path).replace('.nc', '.log')
        cflogfile = os.path.join(CF_LOG_DIR, cf_dir, cf_filename)
        errs = self.get_errors(nc_path, g, 'global', '', now, pid_handle, conventions, cflogfile)


        for var_id in v.keys():
            errs.extend(self.get_errors(nc_path, v[var_id], 'variable', var_id, now, pid_handle, conventions, cflogfile))

        if not errs:
            errs.append(f'{nc_path}|{pid_handle}|{conventions}|{now}|pass||||')
        else:
            cmd = f"cfchecks {nc_path}"
            if not os.path.isdir(os.path.dirname(cflogfile)):
                os.makedirs(os.path.dirname(cflogfile))
            with open(cflogfile, 'w') as cfout:
                subprocess.call(cmd.split(), stdout = cfout)

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

    def get_errors(self, nc_path, rec, etype, detail, timestamp, pid, convention, cflogfile):
        errs = []
        for level in ['FATAL', 'ERROR', 'WARN']:
             result = rec.get(level)
             if self.verbose or result:
                 errs.append(f'{nc_path}|{pid}|{convention}|{timestamp}|{level}|{etype}|{detail}|{result}|{cflogfile}')
        return errs

#checker = SimpleCFChecker()
#log_path = 'out.log'
#for i in range(3):
  #checker.run(bad_nc_path, log_path)
#with open(log_path) as reader:
   #for line in reader:
      #print(line)
