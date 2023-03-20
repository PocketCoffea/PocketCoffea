#!/usr/bin/env python
import os
from multiprocessing import Pool
import argparse
from collections import defaultdict
from coffea.util import load
import subprocess
import json

parser = argparse.ArgumentParser(description='Hadd skimmed files')
# Inputs
parser.add_argument(
    '-fl',
    '--files-list',
    required=True,
    type=str,
    help='Parquet file containing the skimmed files metadata',
)
parser.add_argument("-o", "--outputdir", required=False, type=str, help="Output folder")
parser.add_argument(
    "--only-datasets", nargs="+", type=str, help="Restring list of datasets"
)
parser.add_argument("-f", "--files", type=int, help="Limit number of files")
parser.add_argument("-e", "--events", type=int, help="Limit number of files")
parser.add_argument(
    "-s",
    "--scaleout",
    type=int,
    help="Number of threads to process the hadd.",
    default=2,
)
parser.add_argument("--overwrite", action="store_true", help="Overwrite files")
parser.add_argument(
    "--dry", action="store_true", help="Do not execute hadd, save metadata"
)
args = parser.parse_args()

df = load(args.files_list)

workload = []
groups_metadata = {}

for dataset in df["skimmed_files"].keys():
    if args.only_datasets and dataset not in args.only_datasets:
        continue
    groups_metadata[dataset] = defaultdict(dict)
    nevents_tot = 0
    nfiles = 0
    group = []
    ngroup = 1
    for file, nevents in zip(
        df["skimmed_files"][dataset], df["nskimmed_files"][dataset]
    ):
        if (args.files and (nfiles + 1) > args.files) or (
            args.events and (nevents_tot + nevents) > args.events
        ):
            outfile = f"{args.outputdir}/{dataset}/{dataset}_{ngroup}.root"
            workload.append((outfile, group[:]))
            groups_metadata[dataset]["files"][outfile] = group[:]
            group.clear()
            ngroup += 1
            nfiles = 0
            nevents_tot = 0
        else:
            group.append(file)
            nfiles += 1
            nevents_tot += nevents

    # add last group
    if len(group):
        outfile = f"{args.outputdir}/{dataset}/{dataset}_{ngroup}.root"
        workload.append((outfile, group[:]))
        groups_metadata[dataset]["files"][outfile] = group[:]

print(f"We will hadd {len(workload)} groups of files.")
print("Samples:", groups_metadata.keys())


def do_hadd(group):
    try:
        print("Running: ", group[0])
        if args.overwrite:
            subprocess.run(["hadd", "-f", group[0], *group[1]], check=True)
        else:
            subprocess.run(["hadd", group[0], *group[1]], check=True)
        return group[0], 0
    except subprocess.CalledProcessError as e:
        print("Error producing group: ", group[0])
        print(e.stderr)
        return group[0], 1


if not args.dry:
    p = Pool(args.scaleout)
    results = p.map(do_hadd, workload)

    print("\n\n\n")
    for group, r in results:
        if r != 0:
            print("#### Failed hadd: ", group)

json.dump(groups_metadata, open("./hadd.json", "w"), indent=2)

# Now saving the dataset definition file
dataset_definition = {
    s: {"files": list(d['files'].keys())} for s, d in groups_metadata.items()
}

json.dump(dataset_definition, open("./skimmed_dataset_definition.json", "w"), indent=2)
