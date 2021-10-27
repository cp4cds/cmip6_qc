# cmip6_qc
CF quality control of CMIP6 data data CEDA

To get started you will need: a list of dataset ids and a list of variables in json format.

## 1. Run the QC over the identified datsets for C3S release

Ag will generate a list of dataset ids that require CF checking, you can then use 
`python cfchecker_run_all.py --file <dataset_ids-file> --qc_check cfchecker`

Note- this script will tell you which datasets are 'missing' ie. not in the CEDA Archive. You can run this script multiple times and it will not repeat the QC as long as there are log files showing the QC was already run.

If using the `--file option` this calls directly: 
- `cfchecker_run_unit.py`

_Running without a file uses ABC unit approach calling batch and chunk._ 

`cfchecker_run_unit.py` sends the jobs to Lotus. Each dataset is sent to lotus as some datasets are large.
To see the jobs in lotus run 'squeue -u <username>'

This produces a CF results file in the form of a psv file in a directory called `qc_logs`

## 2 Combine the CF results using 
  
Once you have all the QC results of the datasets you want to check (no 'missing') then run:

- `nohup ./create_expt_psvs.sh > /outdir/ofile.out &`
- `nohup ./create_model_psvs.sh > /outdir/ofile.out &`

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
- `_*py` older files that may have useful information - when you are confident you don't
need these then remove from github and local repo. 
