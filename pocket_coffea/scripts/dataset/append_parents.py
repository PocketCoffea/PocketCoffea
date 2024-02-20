import os
import argparse
import json
import numpy as np
from coffea.util import load

from pocket_coffea.utils.dataset import Dataset

parser = argparse.ArgumentParser(description='Append the parent dataset attribute to the datasets definitions')
parser.add_argument(
    '--cfg',
    help='Config file with datasets parameters',
    required=True,
)
parser.add_argument(
    "-k", "--keys", nargs="+", required=False, help="Dataset keys to select"
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
    help="Check datasets parents only",
    default=False,
)
args = parser.parse_args()
config = json.load(open(args.cfg))

if args.keys:
    keys = args.keys
else:
    keys = config.keys()

for key in keys:
    print("*" * 40)
    print("> Working on dataset: ", key)
    if key not in config:
        print("Key: not found in the dataset configuration file")
        exit(1)
    dataset_cfg = config[key]
    dataset = Dataset(
        name=key,
        cfg=dataset_cfg,
        append_parents=True
    )
    config[key] = dataset.cfg
    if args.check:
        for file in config[key]["files"]:
            print("das_names:\t\t", file["das_names"])
            print("das_parents_names:\t", file["das_parents_names"])

if args.overwrite:
    with open(args.cfg, 'w') as fp:
        json.dump(config, fp, indent=4)
        fp.close()
