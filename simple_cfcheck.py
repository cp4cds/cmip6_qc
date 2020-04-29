#!/usr/bin/env Python

import datetime
import os
import cfchecker.cfchecks as cfc
from netCDF4 import Dataset

class SimpleCFChecker(object):

    def __init__(self, verbose=False):
        self._checker = cfc.CFChecker(version=None, silent=True)
        self.verbose = verbose

    def run(self, nc_path, log_path):
        now = datetime.datetime.now().date().isoformat()
        handle = self.get_handle(nc_path)
        res = self._checker.checker(nc_path)
        g = res['global']
        v = res['variables']
        errs = self.get_errors(nc_path, g, 'global', '', now, handle)

        for var_id in v.keys():
            errs.extend(self.get_errors(nc_path, v[var_id], 'variable', var_id, now, handle))

        if not errs:
            errs.append(f'{os.path.basename(nc_path)}|{handle}||||pass|{now}')
        self.log_errors(errs, log_path)

    def get_handle(self, nc_path):

        dataset = Dataset(nc_path)
        for att in dataset.ncattrs():
            if att == "tracking_id":
                return getattr(dataset, att).strip()

    def log_errors(self, errs, log_path):

        with open(log_path, 'a') as writer:
            for line in errs:
                writer.write(f'{line}\n')

    def get_errors(self, nc_path, rec, etype, detail, timestamp, pid):
        errs = []
        for level in ['FATAL', 'ERROR', 'WARNING']:
             result = rec.get(level)
             if self.verbose or result:
                 errs.append(f'{os.path.basename(nc_path)}|{pid}|{etype}|{detail}|{level}|{result}|{timestamp}')
        return errs

