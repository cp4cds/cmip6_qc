#!/usr/bin/env python

"""This script takes arguments from the command line and submits the script
run_chunk to lotus for each of the ensembles provided as an argument or for
all ensembles if none were provided."""


import argparse
import os
import subprocess
import logging
import sys
import settings

logging.basicConfig(
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)



def arg_parse_batch():
    """
    Parses arguments given at the command line
    :return: Namespace object built from attributes parsed from command line.
    """

    parser = argparse.ArgumentParser()

    parser.add_argument('-m', '--model', type=str,
                        help=f'Model to be quality controlled, supply full path e.g. /badc/cmip6/data/CMIP6/C4MIP/IPSL/IPSL-CM6A-LR')
    parser.add_argument('-q', '--qc_check', nargs=1, type=str, choices=settings.qc_choices, required=False, default="all",
                        help=f"Chose which quality control method from {settings.qc_choices} to use default is to run all")


    return parser.parse_args()


def loop_over_models(args):
    """
    Submits run_chunk to lotus for each of the ensembles listed.
    :param args: (namespace) Namespace object built from attributes parsed from command line
    """

    # turn arguments into string
    qc_type = args.qc_check[0]
    mip_model_path = args.model
    cmip, mip, inst, model = mip_model_path.strip(settings.CMIP6_ARCHIVE_DIR).split('/')

    # define lotus output file path
    current_directory = os.getcwd()  # get current working directory

    # define lotus output file path
    experiments = os.listdir(mip_model_path)
    for exp in experiments:
        
        lotus_output_path = settings.LOTUS_OUTPUT_PATH_TMPL.format(current_directory=current_directory,
                                            cmip6=cmip, mip=mip, inst=inst, model=model, experiment=exp)
    
        # make output directory
        if not os.path.exists(lotus_output_path):
            os.makedirs(lotus_output_path)
    
        output_base = f"{lotus_output_path}/{mip}_{inst}_{model}_{exp}"
    
        # submit to lotus
        simulation = os.path.join(mip_model_path, exp)
    
        bsub_command = f"bsub -q {settings.QUEUE} -W {settings.WALLCLOCK} " \
                       f"-o {output_base}.out -e {output_base}.err " \
                       f"python run_chunk.py --simulation {simulation} --qc_check {qc_type}"
        subprocess.call(bsub_command, shell=True)
        # logging.info(f"running {bsub_command}")


def main():
    """Runs script if called on command line"""

    args = arg_parse_batch()
    loop_over_models(args)


if __name__ == '__main__':
    main()