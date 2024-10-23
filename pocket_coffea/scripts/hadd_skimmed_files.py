#!/usr/bin/env python
import os
from multiprocessing import Pool
from functools import partial
import argparse
from collections import defaultdict
from coffea.util import load
import subprocess
import json
import click

def do_hadd(group, overwrite=False):
    try:
        print("Running: ", group[0])
        if overwrite:
            subprocess.run(["hadd", "-f", group[0], *group[1]], check=True)
        else:
            subprocess.run(["hadd", group[0], *group[1]], check=True)
        return group[0], 0
    except subprocess.CalledProcessError as e:
        print("Error producing group: ", group[0])
        print(e.stderr)
        return group[0], 1

@click.command()
@click.option(
    '-fl',
    '--files-list',
    required=True,
    type=str,
    help='Parquet file containing the skimmed files metadata',
)
@click.option("-o", "--outputdir", type=str, help="Output folder")
@click.option(
    "--only-datasets", 
    type=str, 
    multiple=True, 
    help="Restring list of datasets"
)
@click.option("-f", "--files", type=int, help="Limit number of files")
@click.option("-e", "--events", type=int, help="Limit number of files")
@click.option(
    "-s",
    "--scaleout",
    type=int,
    help="Number of threads to process the hadd.",
    default=2,
)
@click.option("--overwrite", is_flag=True, help="Overwrite files")
@click.option("--dry", is_flag=True, help="Do not execute hadd, save metadata")
def hadd_skimmed_files(files_list, outputdir, only_datasets, files, events, scaleout, overwrite, dry):
    '''
    Regroup skimmed datasets by joining different files (like hadd for ROOT files) 
    '''
    df = load(files_list)
    workload = []
    groups_metadata = {}
    
    for dataset in df["skimmed_files"].keys():
        if only_datasets and dataset not in only_datasets:
            continue
        groups_metadata[dataset] = defaultdict(dict)
        nevents_tot = 0
        nfiles = 0
        group = []
        ngroup = 1
        for file, nevents in zip(
            df["skimmed_files"][dataset], df["nskimmed_events"][dataset]
        ):
            if (files and (nfiles + 1) > files) or (
                events and (nevents_tot + nevents) > events
            ):
                outfile = f"{outputdir}/{dataset}/{dataset}_{ngroup}.root"
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
            outfile = f"{outputdir}/{dataset}/{dataset}_{ngroup}.root"
            workload.append((outfile, group[:]))
            groups_metadata[dataset]["files"][outfile] = group[:]

    print(f"We will hadd {len(workload)} groups of files.")
    print("Samples:", groups_metadata.keys())

    if not dry:
        p = Pool(scaleout)
        # Create one output folder for each dataset
        for outfile, group in workload:
            basedir = os.path.dirname(outfile)
            if not os.path.exists(basedir):
                os.makedirs(basedir)
        results = p.map(partial(do_hadd, overwrite=overwrite), workload)

        print("\n\n\n")
        for group, r in results:
            if r != 0:
                print("#### Failed hadd: ", group)

    json.dump(groups_metadata, open("hadd.json", "w"), indent=2)

    # Now saving the dataset definition file
    dataset_metadata = df["datasets_metadata"]["by_dataset"]
    dataset_definition = {}
    for s, d in groups_metadata.items():
        metadata = dataset_metadata[s]
        skim_efficiency = df["cutflow"]["skim"][s] / df["cutflow"]["initial"][s]
        metadata["size"] = int(skim_efficiency * int(df["datasets_metadata"]["by_dataset"][s]["size"])) # Compute the (approximate) size of the skimmed dataset
        metadata["nevents"] = sum(df["nskimmed_events"][s])
        metadata["skim_efficiency"] = skim_efficiency
        dataset_definition[s] = {"metadata": metadata, "files": list(d['files'].keys())}

    json.dump(dataset_definition, open("skimmed_dataset_definition.json", "w"), indent=2)

    print("DONE!")


if __name__ == "__main__":
    hadd_skimmed_files()
