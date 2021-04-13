#!/bin/bash

db=/gws/nopw/j04/cmip6_prep_vol1/synda/cmip6_sdt_backups/sdt.db.2021-03-15
datasets=missing_datasets_20210312.txt
#ds=CMIP6.CMIP.EC-Earth-Consortium.EC-Earth3-Veg.historical.r12i1p1f1.Omon.zos.gr.v20200925
#sqlite3 $db <<EOF
#select file_functional_id, status from file where file_functional_id like '%$ds%';
#EOF
cat $datasets | while read ds; do
    echo $ds
      sqlite3 $db \
      "SELECT file_functional_id, status FROM file WHERE file_functional_id LIKE '%$ds%'"
done
