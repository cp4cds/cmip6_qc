# cmip6_qc
CF quality control of CMIP6 data data CEDA

## 1. Run the QC over the identified datsets for C3S release

Ag will generate a list of dataset ids that require CF checking, you can then use 
`python cfchecker_run_all.py --file <dataset_ids-file>`

This will call 
- `cfchecker_run_batch.py`
- `cfchecker_run_chunk.py`
- `cfchecker_run_unit.py`

in turn and sends the jobs to Lotus. Each dataset is sent to lotus as some datasets are large.
This produces a CF results file in the form of a psv file in a directory called `qc_logs`
TODO move the `qc_logs` directory up one level set this in settings and then refer to the output 
directory from there. 

## 2 Combine the CF results using 

2.1 `./create_expt_psvs.sh`
2.2 `./create_model_psvs.sh`

These can take a while to run. 

## 3 Generate a combined results file

Run `python generate_c3s-34g_dataframe.py --create`

TODO: re-write this so you understand what is happening

## 4. Complete the QC_template.json template.

A QC_template.json file will be provided. Use the script `complete_json_release_template.py`
to fill out this template with the QC results. 

Once complete name the output file `QC_cfchecker.json` and upload to the appropriate github branch
of https://github.com/cp4cds/c3s_34g_qc_results/ in directory QC_Results. Although a local copy already exists on the system
others may be working on this. You can add your results file via a pull request or you can clone your own copy and push as others 


## Some other potentially useful files
- `check_for_missing_psvfiles.py`: this takes the dataset ids and makes sure that the 
output psv file exists for a dataset. If not then you can rerun the `cfchecker_run_all.py`
- `simple_cfcheck.py`: python script to check cf-checker is running ok. 
- `_*py` older files that may have useful information - when you are confident you don't
need these then remove from github and local repo. 