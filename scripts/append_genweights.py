import os
import argparse
import json
import numpy as np
from coffea.util import load

from pocket_coffea.utils.dataset import Dataset

parser = argparse.ArgumentParser(description='Build dataset fileset in json format')
parser.add_argument(
    '--cfg',
    default=os.getcwd() + "/datasets/datasets_definitions.json",
    help='Config file with datasets parameters',
    required=False,
)
parser.add_argument(
    '-i', '--inputfile',
    default=os.getcwd() + "/datasets/datasets_definitions.json",
    help='Input file with sum_genweights',
    required=True,
)
parser.add_argument(
    '-o',
    '--overwrite',
    action='store_true',
    help="Overwrite existing file definition json",
    default=False,
)
args = parser.parse_args()
config = json.load(open(args.cfg))
output = load(args.inputfile)

for dataset, dict_dataset in config.items():
    for file in dict_dataset["files"]:
        year = file["metadata"]["year"]
        dataset_key = f"{dataset}_{year}"
        if dataset_key in output["sum_genweights"].keys():
            file["metadata"]["sum_genweights"] = np.float64(output["sum_genweights"][dataset_key])

if args.overwrite:
    with open(args.cfg, 'w') as fp:
        json.dump(config, fp, indent=4)
        fp.close()
