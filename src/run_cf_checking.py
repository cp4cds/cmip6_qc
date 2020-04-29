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

    print("running_cf_checker")
    error_file = logfile.strip('.log') + '.err'
    logging.info(f"CF LOG: {logfile}")
    logging.info(f"CF ERROR LOG: {error_file}")
    run_cmd = f"cfchecks {ncfile}"

    if 1:#try:
        cf_out = open(logfile, "w")#, open(error_file, "w")
        subprocess.call(run_cmd.split(), stdout=cf_out)#, stderr=cf_err)
        cf_out.close()#, cf_err.close()
    else:# except:
        pass
    # if os.path.exists(error_file):
    #     logging.info(f"CF CHECK FAIL: {error_file}")
    #     return False

    return True
    # cf_out, cf_err = open(output_file, "w"), open(error_file, "w")
    # subprocess.call(run_cmd, stdout=cf_out, stderr=cf_err)
    # cf_out.close(), cf_err.close()


    

def main(ncfile):

    filename=os.path.basename(ncfile)
    run_cfchecker(ncfile, logfile=filename)

if __name__ == "__main__":
    
    ncfile = sys.argv[1]
    main(ncfile)