#!/bin/bash

basedir=/gws/nopw/j04/cp4cds1_vol3/c3s_34g/cmip6_qc/src/qc_logs/cf/CMIP6

find ${basedir} -mindepth 3 -maxdepth 3 -type d |

    while read model_dir ; do
        model=$(echo $model_dir | cut -d '/' -f 13)
        ofile=${model_dir}/$model.psv
        echo $ofile
        cat ${model_dir}/*/*.psv > $ofile
    done
