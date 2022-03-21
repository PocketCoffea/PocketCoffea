import os
import argparse
from utils.Dataset import Dataset

parser = argparse.ArgumentParser(description='Build dataset file in json format')
parser.add_argument('-i', '--input', default=os.getcwd() + "/dataset/DAS/RunIISummer20UL18.txt", help="JSON file containing the datasets' names on the DAS system", required=True)

args = parser.parse_args()

with open(args.input, 'r') as f:
	datasets = f.readlines()

for name in datasets:
	dataset = Dataset(name)
	print(dataset.filelist)
	#dataset.Print()
