import os
import argparse

from utils.Configurator import Configurator
from utils.Dataset import Dataset

parser = argparse.ArgumentParser(description='Build dataset file in json format')
parser.add_argument('--cfg', default=os.getcwd() + "/config/test.py", help='Config file with parameters specific to the current run', required=False)
parser.add_argument('-d', '--download', action='store_true', default=False, help='Download dataset files on local machine', required=False)

args = parser.parse_args()
config = Configurator(args.cfg, create_dataset=True)

dataset = Dataset(config.dataset, config.storage_prefix, config.json)
dataset.save()

if args.download:
    dataset.download()
