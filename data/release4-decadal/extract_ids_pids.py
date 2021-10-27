#!/usr/bin/env Python

import pandas as pd

df = pd.read_csv("files_filtered_r4_decadal.csv.gz")

selected_columns = df[["dset_id","dset_pid"]]

new_df = selected_columns.copy()

#print(new_df)

ids_pids = new_df.drop_duplicates()

#print(len(ids_pids))

ids_pids.to_csv('./ids_pids_29-09-21.csv', sep=',',index =None)

