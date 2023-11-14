#!/usr/bin/env python

print(
    """
   ___       _ __   _____       __               __ 
  / _ )__ __(_) /__/ / _ \___ _/ /____ ____ ___ / /_
 / _  / // / / / _  / // / _ `/ __/ _ `(_-</ -_) __/
/____/\_,_/_/_/\_,_/____/\_,_/\__/\_,_/___/\__/\__/ 
                                                   
"""
)

import os
import sys
import json
import argparse

from pocket_coffea.utils.dataset import build_datasets

parser = argparse.ArgumentParser(description='Build dataset fileset in json format')
parser.add_argument(
    '--cfg',
    default=os.getcwd() + "/datasets/datasets_definitions.json",
    help='Config file with parameters specific to the current run',
    required=False,
)
parser.add_argument(
    "-k", "--keys", nargs="+", required=False, help="Keys of the datasets to be created. If None, the keys are read from the datasets definition file."
)
parser.add_argument(
    '-d',
    '--download',
    action='store_true',
    default=False,
    help='Download datasets from DAS',
    required=False,
)
parser.add_argument(
    '-o',
    '--overwrite',
    action='store_true',
    help="Overwrite existing .json datasets",
    default=False,
)
parser.add_argument(
    '-c',
    '--check',
    action='store_true',
    help="Check existence of the datasets",
    default=False,
)
parser.add_argument(
    '-s',
    '--split-by-year',
    help="Split datasets by year",
    action="store_true",
    default=False,
)
parser.add_argument("-l", "--local-prefix", help="Prefix of the local path where the datasets are stored", type=str, default=None)
parser.add_argument(
    "-ws",
    "--whitelist-sites",
    help="List of sites to be whitelisted",
    nargs="+",
    type=str,
)
parser.add_argument(
    "-bs",
    "--blacklist-sites",
    help="List of sites to be blacklisted",
    nargs="+",
    type=str,
)
parser.add_argument("-rs", "--regex-sites", help="Regex string to be used to filter the sites", type=str)
parser.add_argument("-p", "--parallelize", help="Number of parallel processes to be used to fetch the datasets", type=int, default=4)
args = parser.parse_args()

print(vars(args))

build_datasets(**vars(args))
