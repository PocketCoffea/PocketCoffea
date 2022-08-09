import os
import csv
import argparse
import json
from PocketCoffea.parameters.lumi import goldenJSON, runs

parser = argparse.ArgumentParser(description='Build dataset file in json format')
parser.add_argument("-o", "--output", default="PocketCoffea/parameters/lumi", required=False, help="Output folder")
parser.add_argument("--dataset", default="SingleMuon", required=True, help="Dataset name")
parser.add_argument("--year", type=str, required=True, help="Data taking year")

args = parser.parse_args()

normtag = "/cvmfs/cms-bril.cern.ch/cms-lumi-pog/Normtags/normtag_PHYSICS.json"

if not os.path.exists(args.output):
    os.makedirs(args.output)

runs_dataset = runs[args.dataset][args.year]

lumi_dict = {args.year : {}}
for era, runs_list in runs_dataset.items():
    run_begin = runs_list[0]
    run_end   = runs_list[-1]
    out_file  = os.path.join(args.output, f"lumi_{args.dataset}_{args.year}{era}_{run_begin}-{run_end}.csv")
    command = f"brilcalc lumi -u /fb --normtag {normtag} -i {goldenJSON[args.year]} --begin {run_begin} --end {run_end} -o {out_file}"
    print(command)
    os.system(command)
    with open(out_file, 'r') as f:
        csvreader = csv.reader(f)
        for i, row in enumerate(csvreader):
            if 'Summary' in row:
                lumi_recorded = float(csvreader[i+2][-1])
                lumi_dict[args.year][era] = lumi_recorded
    f.close()

summary_file = os.path.join(args.output, f"lumi_summary_{args.dataset}_{args.year}.json")
with open(summary_file, 'w') as f:
    json.dump(lumi_dict, summary_file, indent=2)
f.close()
