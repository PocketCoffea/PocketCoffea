import os
import argparse
import json
from PocketCoffea.utils.Dataset import Dataset

parser = argparse.ArgumentParser(description='Build dataset file in json format')
parser.add_argument('--cfg', default=os.getcwd() + "/datasets/datasets_definitions.json", help='Config file with parameters specific to the current run', required=False)
parser.add_argument("-k", "--key", required=True, help="Dataset key")
parser.add_argument('-d', '--download', action='store_true', default=False, help='Download dataset files on local machine', required=False)

args = parser.parse_args()
config = json.load(open(args.cfg))

if args.key not in config:
    print("Key: not found in the dataset configuration file")
    exit(1)

dataset_cfg = config[args.key]

dataset = Dataset(name=args.key, cfg=dataset_cfg)
dataset.save()

if args.download:
    dataset.download()
