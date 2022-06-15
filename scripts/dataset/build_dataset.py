import os
import argparse
import json
from PocketCoffea.utils.Dataset import Dataset

parser = argparse.ArgumentParser(description='Build dataset file in json format')
parser.add_argument('--cfg', default=os.getcwd() + "/datasets/datasets_definitions.json", help='Config file with parameters specific to the current run', required=False)
parser.add_argument("-k", "--keys", nargs="+", required=False, help="Dataset keys")
parser.add_argument('-d', '--download', action='store_true', default=False, help='Download dataset files on local machine', required=False)
parser.add_argument('-o','--overwrite', action='store_true', help="Overwrite existing files", default=False)
parser.add_argument('-c','--check', action='store_true', help="Check file existance in the local prefix", default=False)

args = parser.parse_args()
config = json.load(open(args.cfg))

if args.keys:
    keys = args.keys
else:
    keys = config.keys()
    
for key in keys:
    if key not in config:
        print("Key: not found in the dataset configuration file")
        exit(1)
    dataset_cfg = config[key]
    dataset = Dataset(name=key, cfg=dataset_cfg)
    dataset.save(overwrite=args.overwrite)
    if args.check:
        dataset.check_samples()
    
if args.download:
    dataset.download()
