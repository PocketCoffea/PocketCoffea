import os
import csv
import argparse
import json
from pocket_coffea.parameters.lumi import goldenJSON, runs

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
        rows = []
        for row in csvreader:
            rows.append(row)
        for i, row in enumerate(rows):
            for word in row:
                if 'Summary' in word:
                    lumi_recorded = float(rows[i+2][-1])
                    lumi_dict[args.year][era] = lumi_recorded
    f.close()

lumi_dict[args.year]['tot'] = sum(lumi_dict[args.year].values())
summary_file = os.path.join(args.output, f"lumi_summary_{args.dataset}_{args.year}.json")
with open(summary_file, 'w') as f:
    json.dump(lumi_dict, f, indent=2)
f.close()
