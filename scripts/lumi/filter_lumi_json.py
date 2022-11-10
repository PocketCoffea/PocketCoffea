import os
import argparse
import json
from pocket_coffea.parameters.lumi import runs

parser = argparse.ArgumentParser(description='Build Lumisection file in json format')
parser.add_argument("-i", "--input", required=True, help="Input luminosity JSON file")
parser.add_argument("-o", "--output", default="PocketCoffea/parameters/lumi", required=False, help="Output folder")
parser.add_argument("--dataset", default="SingleMuon", required=True, help="Dataset name")
parser.add_argument("--year", required=True, help="Data taking year")

args = parser.parse_args()

if not os.path.exists(args.output):
    os.makedirs(args.output)

runs_dataset = runs[args.dataset][args.year]

with open(args.input, 'r') as file:
    runs_total = json.load(file)

for era, runs_list in runs_dataset.items():
    output_list = []
    for tag, run in runs_total:
        run_number = int(list(run.keys())[0])
        if run_number in runs_list:
            output_list.append([tag, run])


    filename = os.path.basename(args.input)
    filename = os.path.join(args.output, f"{filename.split('.json')[0]}_{args.dataset}_{args.year}{era}.json")
    print(f"Saving run list into {filename}")
    with open(filename, 'w') as out_file:
        json.dump(output_list, out_file, separators=(',\n', ': '))
    out_file.close()
