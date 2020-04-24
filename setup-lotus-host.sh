#!/bin/bash
ds=$1
qc=$2
source /group_workspaces/jasmin2/cp4cds1/vol3/c3s_34g/cmip6_qc/setup-env.sh
python /group_workspaces/jasmin2/cp4cds1/vol3/c3s_34g/cmip6_qc/run_chunk.py --dataset_id ${ds} --qc_check ${qc}