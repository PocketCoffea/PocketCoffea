import os
import argparse

from utils.Configurator import Configurator
from utils.Dataset import Dataset

parser = argparse.ArgumentParser(description='Build dataset file in json format')
parser.add_argument('--cfg', default=os.getcwd() + "/config/test.py", help='Config file with parameters specific to the current run', required=False)

args = parser.parse_args()
config = Configurator(args.cfg, dataset=True)

dataset       = Dataset(config.dataset, "root://xrootd-cms.infn.it//")
dataset_local = Dataset(config.dataset, config.prefix)
dataset.save(config.json)
dataset_local.save(config.json.replace('.json', '_local.json'))
