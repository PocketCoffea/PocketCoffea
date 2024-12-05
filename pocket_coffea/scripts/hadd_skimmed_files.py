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
        chain = R.TChain('Events')
        for inputfile in group[1]:
            chain.Add(inputfile)

        output_file = R.TFile.Open(group[0], "RECREATE")
        output_tree = chain.CloneTree(-1)  # Clone all entries
        output_tree.Write()
        output_file.Close()
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
@click.option("-fs", "--filter-samples",  type=str,  help="Filter list of samples (comma separated)")
@click.option(
    "--filter-datasets", 
    type=str, 
    help="Restricting list of datasets (comma separated))"
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
def hadd_skimmed_files(files_list,  outputdir, filter_samples,
                       filter_datasets, files, events, scaleout, overwrite, dry):
    '''
    Regroup skimmed datasets by joining different files (like hadd for ROOT files) 
    '''
    df = load(files_list)
    only_samples = None
    only_datasets = None
    if filter_samples:
        if "," in filter_samples:
            only_samples = filter_samples.split(",")
        else:
            only_samples = [filter_samples]
    if filter_datasets:
        if "," in filter_datasets:
            only_datasets = filter_datasets.split(",")
        else:
            only_datasets = [filter_datasets]
        
    workload = []
    groups_metadata = {}
    if files is None or files > 500:
        files = 500 # to avoid max size of command args
    
    for dataset in df["skimmed_files"].keys():
        if only_datasets and dataset not in only_datasets:
            continue
        if only_samples:
            # Check the samples of this datasets
            sample = df["datasets_metadata"]["by_dataset"][dataset]["sample"]
            if sample not in only_samples:
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
    json.dump(groups_metadata, open("hadd.json", "w"), indent=2)

    # Create one output folder for each dataset
    for outfile, group in workload:
        basedir = os.path.dirname(outfile)
        if not os.path.exists(basedir):
            os.makedirs(basedir)

    if not dry:
        p = Pool(scaleout)
        
        results = p.map(partial(do_hadd, overwrite=overwrite), workload)

        print("\n\n\n")
        for group, r in results:
            if r != 0:
                print("#### Failed hadd: ", group)

                
    # Now saving the dataset definition file
    dataset_metadata = df["datasets_metadata"]["by_dataset"]
    dataset_definition = {}
    for s, d in groups_metadata.items():
        metadata = dataset_metadata[s]
        skim_efficiency = df["cutflow"]["skim"][s] / df["cutflow"]["initial"][s]
        metadata["size"] = str(int(skim_efficiency * int(df["datasets_metadata"]["by_dataset"][s]["size"]))) # Compute the (approximate) size of the skimmed dataset
        metadata["nevents"] = str(sum(df["nskimmed_events"][s]))
        metadata["skim_efficiency"] = str(skim_efficiency)
        if metadata["isMC"] in ["True", "true", True]:
            metadata["skim_rescale_genweights"] = str(df["sum_genweights"][s] / df["sum_genweights_skimmed"][s]) # Compute the rescale factor for the genweights as the inverse of the skim genweighed efficiency
        metadata["isSkim"] = "True"
        dataset_definition[s] = {"metadata": metadata, "files": list(d['files'].keys())}

    json.dump(dataset_definition, open("skimmed_dataset_definition_hadd.json", "w"), indent=2)

    # Preparing the files for submissions
    # writing out a script with the hadd commands
    with open("do_hadd.py", "w") as f:
        f.write(f"""import os
import sys
import json
from multiprocessing import Pool
import subprocess
import ROOT as R

def do_hadd(group):
    try:
        outputfile, inputfiles = group
        print("Working on ", outputfile)
        chain = R.TChain('Events')
        for inputfile in inputfiles:
            chain.Add(inputfile)

        output_file = R.TFile.Open(outputfile, "RECREATE")
        output_tree = chain.CloneTree(-1)  # Clone all entries
        output_tree.Write()
        output_file.Close()
        del chain
        del output_tree
        del output_file
    except Exception as e:
        print(e)
        return outputfile

config = json.load(open("hadd.json"))
workload = []
p = Pool({scaleout})

for dataset, conf in config.items():
    if len(sys.argv)> 1 and sys.argv[1] not in dataset:
        continue
    for outputfile, inputfiles in conf["files"].items():
        workload.append((outputfile, inputfiles))

     
failed = p.map(do_hadd, workload)

print("DONE!")
print("Failed files:")
for f in failed:
    if f:
        print(f)""")

    with open("do_hadd_job_splitbyfile.py", "w") as f:
        f.write(f"""#!/bin/python3
import os
import sys
import json
from multiprocessing import Pool
import subprocess
import ROOT as R

def do_hadd(group):
    try:
        outputfile, inputfiles = group
        print("Working on ", outputfile)
        chain = R.TChain('Events')
        for inputfile in inputfiles:
            chain.Add(inputfile)

        output_file = R.TFile.Open(outputfile, "RECREATE")
        output_tree = chain.CloneTree(-1)  # Clone all entries
        output_tree.Write()
        output_file.Close()
        del chain
        del output_tree
        del output_file
    except Exception as e:
        print(e)
        return outputfile

config = json.load(open("hadd.json"))
files = config[sys.argv[1]]["files"][sys.argv[2]]

failed = []
out = do_hadd((sys.argv[2], files))
if out:
    failed.append(out)
        
print("DONE!")
print("Failed files: ", failed)
if len(failed):
    sys.exit(1)""")


    abs_local_path = os.path.abspath(".")
    os.makedirs(f"{abs_local_path}/condor", exist_ok=True)
    sub = {
        "Executable": "do_hadd_job.py",
        "Universe": "vanilla",
        "Error": f"{abs_local_path}/condor/hadd_job_$(ClusterId).$(ProcId).err",
        "Output": f"{abs_local_path}/condor/hadd_job_$(ClusterId).$(ProcId).out",
        "Log": f"{abs_local_path}/condor/hadd_job_$(ClusterId).$(ProcId).log",
        'MY.SendCredential': True,
        '+JobFlavour': f'"espresso"',
        'arguments': "$(dataset)",
        'should_transfer_files':'YES',
        'when_to_transfer_output' : 'ON_EXIT',
        'transfer_input_files' : f"{abs_local_path}/do_hadd_job_splitbyfile.py, {abs_local_path}/hadd.json",
    }
    with open("hadd_job.sub", "w") as f:
        for k, v in sub.items():
            f.write(f"{k} = {v}\n")
        # Now adding the arguments
        f.write("queue dataset, group from (\n")
        for dataset, conf in groups_metadata.items():
            f.write(f'{dataset}\n')
        f.write(")\n")
    
    print("DONE!")
    
    sub = {
        "Executable": "do_hadd_job_splitbyfile.py",
        "Universe": "vanilla",
        "Error": f"{abs_local_path}/condor/hadd_job_$(ClusterId).$(ProcId).err",
        "Output": f"{abs_local_path}/condor/hadd_job_$(ClusterId).$(ProcId).out",
        "Log": f"{abs_local_path}/condor/hadd_job_$(ClusterId).$(ProcId).log",
        'MY.SendCredential': True,
        '+JobFlavour': f'"espresso"',
        'arguments': "$(dataset) $(group)",
        'should_transfer_files':'YES',
        'when_to_transfer_output' : 'ON_EXIT',
        'transfer_input_files' : f"{abs_local_path}/do_hadd_job_splitbyfile.py, {abs_local_path}/hadd.json",
    }
    with open("hadd_job_splitbyfile.sub", "w") as f:
        for k, v in sub.items():
            f.write(f"{k} = {v}\n")
        # Now adding the arguments
        f.write("queue dataset, group from (\n")
        for dataset, conf in groups_metadata.items():
            for group in conf["files"].keys():
                f.write(f'{dataset} {group}\n')
        f.write(")\n")
    
    print("DONE!")


if __name__ == "__main__":
    hadd_skimmed_files()
