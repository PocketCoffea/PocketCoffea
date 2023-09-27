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
    "-k", "--keys", nargs="+", required=False, help="Dataset keys to select"
)
parser.add_argument(
    '-d',
    '--download',
    action='store_true',
    default=False,
    help='Download dataset files on local machine',
    required=False,
)
parser.add_argument(
    '-o',
    '--overwrite',
    action='store_true',
    help="Overwrite existing file definition json",
    default=False,
)
parser.add_argument(
    '-c',
    '--check',
    action='store_true',
    help="Check file existance in the local prefix",
    default=False,
)
parser.add_argument(
    '-s',
    '--split-by-year',
    help="Split output files by year",
    action="store_true",
    default=False,
)
parser.add_argument("-l", "--local-prefix", help="Local prefix", type=str, default=None)
parser.add_argument(
    "-ws",
    "--whitelist-sites",
    help="List of sites in the whitelist",
    nargs="+",
    type=str,
)
parser.add_argument(
    "-bs",
    "--blacklist-sites",
    help="List of sites in the blacklist",
    nargs="+",
    type=str,
)
parser.add_argument("-rs", "--regex-sites", help="Regex to filter sites", type=str)
parser.add_argument("-p", "--parallelize", help="Number of workers", type=int, default=4)
args = parser.parse_args()

print(vars(args))

build_datasets(**vars(args))
