#!/usr/bin/env Python

EXIT_AFTER_N_FAILURES = 1000000
CMIP6_ARCHIVE_DIR = "/badc/cmip6/data/"
qc_choices = ["all", "cfchecker", "prepare", "nctime"]
SETUP_ENV_FILE = "/group_workspaces/jasmin2/cp4cds1/vol3/c3s_34g/cmip6_qc/setup-env.sh"
#
# START_DATE = '1900-01-01'
#
# END_DATE = '2000-01-01'

# lotus settings

QUEUE = 'short-serial'

WALLCLOCK = '24:00'

# Output path templates

LOTUS_OUTPUT_PATH_TMPL = "{current_directory}/lotus_logs/{cmip6}/{mip}/{inst}/{model}/{experiment}"

CF_OUTPUT_PATH_TMPL = "{current_directory}/qc_logs/cf/{cmip6}/{mip}/{inst}/{model}/{experiment}/{ensemble}/{table}/{var}/{grid}/{version}/"
# SUCCESS_PATH_TMPL = "{current_directory}/ALL_OUTPUTS/success/{stat}/{model}/{ensemble}"
# BAD_DATA_PATH_TMPL = "{current_directory}/ALL_OUTPUTS/bad_data/{stat}/{model}/{ensemble}"
# BAD_NUM_PATH_TMPL = "{current_directory}/ALL_OUTPUTS/bad_num/{stat}/{model}/{ensemble}"
# NO_OUTPUT_PATH_TMPL = "{current_directory}/ALL_OUTPUTS/no_output/{stat}/{model}/{ensemble}"

