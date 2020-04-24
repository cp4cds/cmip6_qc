#!/usr/bin/env Python

import os
import sys
import logging
import subprocess

current_dir = os.getcwd()
logging.basicConfig(
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

def run_cfchecker(ncfile, logfile=None):


    error_file = logfile + '.err'
    try:
        logging.info(f"CF CHECK: {logfile}")
        run_cmd = ["cfchecks", ncfile]
        cf_out = open(output_file, "w")
        subprocess.call(run_cmd, stdout=cf_out)
        cf_out.close()

        # cf_out, cf_err = open(output_file, "w"), open(error_file, "w")
        # subprocess.call(run_cmd, stdout=cf_out, stderr=cf_err)
        # cf_out.close(), cf_err.close()

        return True
    except:
        logging.info(f"CF CHECK FAIL: {error_file}")
        return False
        pass



def main(ncfile):

    filename=os.path.basename(ncfile)
    run_cfchecker(ncfile, logfile=filename)

if __name__ == "__main__":
    
    ncfile = sys.argv[1]
    main(ncfile)