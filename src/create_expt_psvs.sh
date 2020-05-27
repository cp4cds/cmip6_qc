#!/bin/bash

basedir=/group_workspaces/jasmin2/cp4cds1/vol3/c3s_34g/cmip6_qc/qc_logs/cf/CMIP6

find ${basedir} -mindepth 4 -maxdepth 4 -type d |
#    expt_dir=../qc_logs/cf/CMIP6/AerChemMIP/BCC/BCC-ESM1/hist-piNTCF
    while read expt_dir; do
        expt=$(echo ${expt_dir} | cut -d '/' -f 14)
        ofile=${expt_dir}/${expt}.psv
        echo $ofile
        cat ${expt_dir}/*/*/*psv > ${ofile}
    done