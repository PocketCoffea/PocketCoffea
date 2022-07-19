import os
import sys
import argparse
import json

# Include PocketCoffea to python paths (needed if running from outside PocketCoffea)
PATH_TO_SCRIPT = '/'.join(sys.argv[0].split('/')[:-1])
PATH_TO_MODULE = os.path.abspath(os.path.join(os.path.abspath(PATH_TO_SCRIPT), "../.."))
if not PATH_TO_MODULE in sys.path:
    sys.path.append(PATH_TO_MODULE)

from PocketCoffea.utils.Dataset import Dataset

parser = argparse.ArgumentParser(description='Build dataset file in json format')
parser.add_argument('--cfg', default=os.getcwd() + "/datasets/datasets_definitions.json", help='Config file with parameters specific to the current run', required=False)
parser.add_argument("-k", "--keys", nargs="+", required=False, help="Dataset keys")
parser.add_argument('-d', '--download', action='store_true', default=False, help='Download dataset files on local machine', required=False)
parser.add_argument('-o','--overwrite', action='store_true', help="Overwrite existing files", default=False)
parser.add_argument('-c','--check', action='store_true', help="Check file existance in the local prefix", default=False)
parser.add_argument('-s','--split-by-year',help="Split output files by year", action="store_true", default=False)

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
    dataset.save(overwrite=args.overwrite,split=args.split_by_year)
    if args.check:
        dataset.check_samples()
    
if args.download:
    dataset.download()
