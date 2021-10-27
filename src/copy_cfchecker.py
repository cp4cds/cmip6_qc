#!/usr/bin/env Python

import os
import subprocess
import cfchecker.cfchecks as cfc
from netCDF4 import Dataset


test_file = "/badc/cmip6/data/CMIP6/DCPP/EC-Earth-Consortium/EC-Earth3/dcppA-hindcast/s2000-r1i1p1f1/day/tasmax/gr/latest/tasmax_day_EC-Earth3_dcppA-hindcast_s2000-r1i1p1f1_gr_20101101-20111031.nc"
cfchecker = cfc.CFChecker(version=None, silent=True)
res = cfchecker.checker(test_file)
g = res['global']
v = res['variables']
print(v)

errs = []
for level in ['FATAL', 'ERROR', 'WARN']:
   result = g.get(level)
   
   #errs.append(f'{test_file}|{level}|{result}')
   #print(errs)

for var_id in v.keys():
   var_result = v[var_id].get(level)
   errs.append(f'{test_file}|{level}|{var_id}|{var_result}')
print(errs)


'''	errs = self.get_errors(nc_path, g, 'global', '', now, pid_handle, conventions, cflogfile)

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
        for level in ['FATAL', 'ERROR', 'WARNING']:
             result = rec.get(level)
             if self.verbose or result:
                 errs.append(f'{nc_path}|{pid}|{convention}|{timestamp}|{level}|{etype}|{detail}|{result}|{cflogfile}')
        return errs'''

# checker = SimpleCFChecker()
# log_path = 'out.log'
# for i in range(3):
#     checker.run(bad_nc_path, log_path)
# with open(log_path) as reader:
#     for line in reader:
#         print(line)
