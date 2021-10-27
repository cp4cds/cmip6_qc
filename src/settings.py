#!/usr/bin/env Python

EXIT_AFTER_N_FAILURES = 1000000
CMIP6_ARCHIVE_DIR = "/badc/cmip6/data/"
qc_choices = ["all", "cfchecker", "prepare", "nctime"]
SETUP_ENV_FILE = "/gws/nopw/j04/cp4cds1_vol3/c3s_34g/cmip6_qc/setup-env.sh"
#
# START_DATE = '1900-01-01'
#
# END_DATE = '2000-01-01'

# lotus settings

QUEUE = 'short-serial'
WALLCLOCK = '24:00:00'

# Output path templates

#LOTUS_OUTPUT_PATH_TMPL = "{current_directory}/lotus-slurm-logs/{cmip6}/{mip}/{inst}/{model}/{experiment}"
LOTUS_OUTPUT_PATH_TMPL = "{current_directory}/lotus-slurm-logs/{cmip6}/{mip}/{inst}/{model}"
CF_OUTPUT_PATH_TMPL = "{current_directory}/qc_logs/cf/{cmip6}/{mip}/{inst}/{model}/{experiment}/{ensemble}/{table}"
# SUCCESS_PATH_TMPL = "{current_directory}/ALL_OUTPUTS/success/{stat}/{model}/{ensemble}"
# BAD_DATA_PATH_TMPL = "{current_directory}/ALL_OUTPUTS/bad_data/{stat}/{model}/{ensemble}"
# BAD_NUM_PATH_TMPL = "{current_directory}/ALL_OUTPUTS/bad_num/{stat}/{model}/{ensemble}"
# NO_OUTPUT_PATH_TMPL = "{current_directory}/ALL_OUTPUTS/no_output/{stat}/{model}/{ensemble}"

EXPERIMENTS = ['amip', 'historical', 'piControl', 'ssp119', 'ssp126', 'ssp245', 'ssp370', 'ssp434', 'ssp460', 'ssp534-over', 'ssp585', 'dcppA-hindcast', 'dcppB-forecast' ]

SPEC_CHARS = [ '[', ']', '"', '\\']

CF_ERROR_LEVEL = {}

CF_ERROR_LEVEL["(2.3): Invalid variable name'] on variable 3basin"] = "major"
CF_ERROR_LEVEL["(7.1): bounds attribute referencing non-existent variable"] = "major"
CF_ERROR_LEVEL["(5): co-ordinate variable not monotonic"] = "major"
CF_ERROR_LEVEL["(7.1): Incorrect number of dimensions for boundary variable: time_bounds"] = "major"



CF_ERROR_LEVEL["(4.3.3): ap_bnds is not declared as a variable"] = "na"
CF_ERROR_LEVEL["(4.3.3): b_bnds is not declared as a variable"] = "na"
CF_ERROR_LEVEL["(4.3.3): ps is not declared as a variable"] = "na"
CF_ERROR_LEVEL["(5): Dimensions must be a subset of dimension"] = "na"
CF_ERROR_LEVEL["(7.1): Boundary var lev_bnds has inconsistent standard_name to lev"] = "na"
CF_ERROR_LEVEL["(7.2): Invalid cell_measures syntax"] = "na"

CF_ERROR_LEVEL["(7.1): Boundary var time_bnds should not have attribute units"] = "minor"
CF_ERROR_LEVEL["'(7.1): Boundary var time_bnds should not have attribute units'"] = "minor"
CF_ERROR_LEVEL["Attribute missing_value of incorrect type (expecting 'Data Variable' type, got 'Numeric' type)"] = "minor"
CF_ERROR_LEVEL["external variable must not be present in this file"] = "minor"
CF_ERROR_LEVEL["Invalid attribute name: _CoordinateAxisType"] = "minor"
CF_ERROR_LEVEL["(2.6.3): Variable areacella named as an external variable must not be present in this file"] = "minor"
CF_ERROR_LEVEL["(3.1): Units are not consistent with those given in the standard_name table."] = "minor"
CF_ERROR_LEVEL["(3.3): Invalid standard_name: olevel"] = "minor"
CF_ERROR_LEVEL["(3.3): Invalid standard_name: Latitude"] = "minor"
CF_ERROR_LEVEL["(3.3): Invalid standard_name modifier"] = "minor"
CF_ERROR_LEVEL["(3.3): Invalid standard_name: bounds"] = "minor"
CF_ERROR_LEVEL["(3.3): Invalid syntax for 'standard_name' attribute: 'number of layers'"] = "minor"
CF_ERROR_LEVEL["(3.3): Invalid standard_name: alevel"] = "minor"
CF_ERROR_LEVEL["(3.3): Invalid standard_name: Vertical"] = "minor"
CF_ERROR_LEVEL["(3.3): Invalid standard_name: ocean_sigma_z"] = "minor"
CF_ERROR_LEVEL["(3.3): Invalid region name:"] = "minor"
CF_ERROR_LEVEL["(4.3.3): Formula term nsigma not present in formula for ocean_sigma_z_coordinate"] = "minor"
CF_ERROR_LEVEL["(4.3.3): formula_terms attribute only allowed on coordinate variables"] = "minor"
CF_ERROR_LEVEL["(4.3.3): No formula defined for standard name: ocean_sigma_z"] = "minor"
CF_ERROR_LEVEL["(4.3.3): Formula term nsigma not present in formula for ocean_sigma_z_coordinate"] = "minor"
CF_ERROR_LEVEL["(5): coordinates attribute referencing non-existent variable"] = "minor"
CF_ERROR_LEVEL["(7.3): Invalid syntax for cell_methods attribute"] = "minor"
CF_ERROR_LEVEL["(7.3): Invalid 'name' in cell_methods attribute"] = "minor"
CF_ERROR_LEVEL["(7.1): Incorrect dimensions for boundary variable: lat_bnds"] = "minor"
CF_ERROR_LEVEL["(7.1): Boundary var lev_bnds has inconsistent units to lev"] = "minor"
CF_ERROR_LEVEL["(7.1): Boundary var time_bnds has inconsistent calendar to time"] = "minor"
CF_ERROR_LEVEL["(7.2): cell_measures variable areacello must either exist in this netCDF file or be named by the external_variables attribute"] = "minor"
CF_ERROR_LEVEL["(7.2): cell_measures variable areacella must either exist in this netCDF file or be named by the external_variables attribute"] = "minor"
CF_ERROR_LEVEL["(7.3): Invalid unit hours, in cell_methods comment"] = "minor"
