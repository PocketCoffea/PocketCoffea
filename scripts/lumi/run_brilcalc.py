import os
import argparse
import json
from PocketCoffea.parameters.lumi import goldenJSON, runs

parser = argparse.ArgumentParser(description='Build dataset file in json format')
parser.add_argument("-o", "--output", default="PocketCoffea/parameters/lumi", required=False, help="Output folder")
parser.add_argument("--dataset", default="SingleMuon", required=True, help="Dataset name")
parser.add_argument("--year", required=True, help="Data taking year")

args = parser.parse_args()

normtag = "/cvmfs/cms-bril.cern.ch/cms-lumi-pog/Normtags/normtag_PHYSICS.json"

if not os.path.exists(args.output):
    os.makedirs(args.output)

runs_dataset = runs[args.dataset][args.year]

for era, runs_list in runs_dataset.items():
    run_begin = runs_list[0]
    run_end   = runs_list[-1]
    command = f"brilcalc lumi -u /fb --normtag {normtag} -i {goldenJSON[args.year]} --begin {run_begin} --end {run_end} -o lumi_{args.dataset}_{args.year}{era}_{run_begin}-{run_end}.csv"
    os.command(command)
