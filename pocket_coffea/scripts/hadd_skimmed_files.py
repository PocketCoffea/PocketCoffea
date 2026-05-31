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



def _strip_xrootd_prefix(path):
    """Strip a ``root://host/`` prefix so ``os.path.exists`` can probe the
    FUSE-mounted path (typical lxplus / worker-node EOS layout). If the
    path does not start with ``root://``, it is returned unchanged.
    """
    if path.startswith("root://"):
        rest = path[len("root://"):]
        slash = rest.find("/")
        if slash != -1:
            # canonical form is root://host//abs/path → "/abs/path"
            return rest[slash + 1:]
    return path


def _check_output_file(item):
    """Validate one hadded output file.

    item = (outfile, expected_nevents, existence_only).
    Returns (outfile, status, message, observed_nevents) where status is one
    of "ok" | "missing" | "unreadable" | "wrong_nevents". observed_nevents
    is the entry count read from the file (or None if it could not be read
    or existence_only is set).
    """
    outfile, expected, existence_only = item
    local_path = _strip_xrootd_prefix(outfile)
    if not os.path.exists(local_path):
        return outfile, "missing", f"file does not exist ({local_path})", None
    if existence_only:
        return outfile, "ok", "exists", None
    try:
        import ROOT as R
        R.gErrorIgnoreLevel = R.kError
        f = R.TFile.Open(outfile, "READ")
        if not f or f.IsZombie():
            if f:
                f.Close()
            return outfile, "unreadable", "TFile.Open failed / zombie", None
        tree = f.Get("Events")
        if not tree:
            f.Close()
            return outfile, "unreadable", "no Events tree", None
        nentries = int(tree.GetEntries())
        f.Close()
        if expected is not None and nentries != expected:
            return (
                outfile,
                "wrong_nevents",
                f"expected {expected} entries, got {nentries}",
                nentries,
            )
        return outfile, "ok", f"{nentries} entries", nentries
    except Exception as e:
        return outfile, "unreadable", f"exception: {e}", None


_DO_HADD_JOB_SPLITBYFILE_TEMPLATE = """#!/bin/python3
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

json_file = sys.argv[3] if len(sys.argv) > 3 else "hadd.json"
config = json.load(open(json_file))
files = config[sys.argv[1]]["files"][sys.argv[2]]

failed = []
out = do_hadd((sys.argv[2], files))
if out:
    failed.append(out)

print("DONE!")
print("Failed files: ", failed)
if len(failed):
    sys.exit(1)
"""


def _write_do_hadd_job_splitbyfile_script(path="do_hadd_job_splitbyfile.py"):
    """Write the per-file hadd wrapper. Accepts an optional 3rd argv giving
    the JSON config path (defaults to hadd.json) so a single wrapper script
    can drive both the original and resubmission .sub files."""
    with open(path, "w") as f:
        f.write(_DO_HADD_JOB_SPLITBYFILE_TEMPLATE)


def do_hadd(group, overwrite=False):
    try:
        chain = R.TChain('Events')
        for inputfile in group[1]:
            chain.Add(inputfile)
        # Create the output directory if it does not exist
        output_dir = os.path.dirname(group[0])
        os.makedirs(output_dir, exist_ok=True)
        if os.path.exists(group[0]):
            if overwrite:
                print(f"Output file {group[0]} already exists, but overwrite is enabled. It will be overwritten.")
            else:
                print(f"Output file {group[0]} already exists. Skipping hadd for this group.")
                return group[0], 0
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
@click.option(
    "--check",
    is_flag=True,
    help=(
        "Do not run hadd. Instead, validate that every expected output file "
        "exists, opens as ROOT, contains an Events tree, and has the expected "
        "number of entries. Writes hadd_failed.json (same shape as hadd.json, "
        "containing only the failing groups), hadd_failed.txt (one line per "
        "failure) and hadd_failed_splitbyfile.sub for resubmission."
    ),
)
def hadd_skimmed_files(files_list,  outputdir, filter_samples,
                       filter_datasets, files, events, scaleout, overwrite, dry, check):
    '''
    Regroup skimmed datasets by joining different files (like hadd for ROOT files) 
    '''
    try:
        import ROOT as R
        root_available = True
    except ImportError:
        print("ROOT is not available. Please make sure to have ROOT installed and configured properly to run this script.")
        root_available = False

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
                groups_metadata[dataset]["nevents_per_outfile"][outfile] = nevents_tot
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
            groups_metadata[dataset]["nevents_per_outfile"][outfile] = nevents_tot

    print(f"We will hadd {len(workload)} groups of files.")
    print("Samples:", groups_metadata.keys())
    json.dump(groups_metadata, open("hadd.json", "w"), indent=2)

    if check:
        existence_only = not root_available
        if existence_only:
            print(
                "ROOT is not available — falling back to existence-only check "
                "(no Events tree / entry-count validation)."
            )

        check_items = []
        for dataset, conf in groups_metadata.items():
            for outfile in conf["files"].keys():
                expected = conf["nevents_per_outfile"].get(outfile)
                check_items.append((outfile, expected, existence_only))

        mode = "existence" if existence_only else "existence + ROOT + entries"
        print(f"\nValidating {len(check_items)} hadded output files ({mode}) ...")
        if scaleout and scaleout > 1:
            with Pool(scaleout) as p:
                results = p.map(_check_output_file, check_items)
        else:
            results = [_check_output_file(it) for it in check_items]

        # Group results by status
        by_status = defaultdict(list)
        outfile_to_dataset = {}
        for dataset, conf in groups_metadata.items():
            for outfile in conf["files"].keys():
                outfile_to_dataset[outfile] = dataset
        for outfile, status, msg, observed in results:
            by_status[status].append((outfile, msg, observed))

        n_ok = len(by_status.get("ok", []))
        n_missing = len(by_status.get("missing", []))
        n_unreadable = len(by_status.get("unreadable", []))
        n_wrong = len(by_status.get("wrong_nevents", []))
        n_failed = n_missing + n_unreadable + n_wrong

        print("\n=== Hadd output check summary ===")
        print(f"  OK:           {n_ok}")
        print(f"  Missing:      {n_missing}")
        print(f"  Unreadable:   {n_unreadable}")
        print(f"  Wrong events: {n_wrong}")
        print(f"  Total failed: {n_failed} / {len(check_items)}")

        if n_failed == 0:
            print("\nAll output files are present and look healthy.")
            return

        # Detailed listing of failures
        for status in ("missing", "unreadable", "wrong_nevents"):
            entries = by_status.get(status, [])
            if not entries:
                continue
            print(f"\n--- {status} ({len(entries)}) ---")
            for outfile, msg, _observed in entries:
                print(f"  [{outfile_to_dataset.get(outfile, '?')}] {outfile}  ({msg})")

        # Write hadd_failed.json in the same shape as hadd.json containing
        # only the failing groups. The accompanying hadd_failed_splitbyfile.sub
        # passes hadd_failed.json explicitly as the 3rd argument of
        # do_hadd_job_splitbyfile.py so the original hadd.json is untouched.
        failed_by_dataset = defaultdict(lambda: defaultdict(dict))
        for status in ("missing", "unreadable", "wrong_nevents"):
            for outfile, _msg, _obs in by_status.get(status, []):
                dataset = outfile_to_dataset[outfile]
                inputs = groups_metadata[dataset]["files"][outfile]
                failed_by_dataset[dataset]["files"][outfile] = inputs
                failed_by_dataset[dataset]["nevents_per_outfile"][outfile] = (
                    groups_metadata[dataset]["nevents_per_outfile"][outfile]
                )

        json.dump(failed_by_dataset, open("hadd_failed.json", "w"), indent=2)
        with open("hadd_failed.txt", "w") as fh:
            for status in ("missing", "unreadable", "wrong_nevents"):
                for outfile, msg, _obs in by_status.get(status, []):
                    dataset = outfile_to_dataset[outfile]
                    fh.write(f"{status}\t{dataset}\t{outfile}\t{msg}\n")

        # (Re)write the per-file wrapper next to the resubmit sub so that any
        # stale copy on disk (e.g. from a hadd run that predates the
        # 3rd-argv json-file support) is replaced. Without this, the wrapper
        # would silently fall back to reading "hadd.json" and the resubmit
        # would not see the failures listed in hadd_failed.json.
        _write_do_hadd_job_splitbyfile_script("do_hadd_job_splitbyfile.py")

        abs_local_path = os.path.abspath(".")
        os.makedirs(f"{abs_local_path}/condor", exist_ok=True)
        resubmit_sub = {
            "Executable": "do_hadd_job_splitbyfile.py",
            "Universe": "vanilla",
            "Error": f"{abs_local_path}/condor/hadd_resubmit_$(ClusterId).$(ProcId).err",
            "Output": f"{abs_local_path}/condor/hadd_resubmit_$(ClusterId).$(ProcId).out",
            "Log": f"{abs_local_path}/condor/hadd_resubmit_$(ClusterId).$(ProcId).log",
            'MY.SendCredential': True,
            '+JobFlavour': f'"espresso"',
            'arguments': "$(dataset) $(group) hadd_failed.json",
            'should_transfer_files': 'YES',
            'when_to_transfer_output': 'ON_EXIT',
            'transfer_input_files': (
                f"{abs_local_path}/do_hadd_job_splitbyfile.py, "
                f"{abs_local_path}/hadd_failed.json"
            ),
        }
        with open("hadd_failed_splitbyfile.sub", "w") as fh:
            for k, v in resubmit_sub.items():
                fh.write(f"{k} = {v}\n")
            fh.write("queue dataset, group from (\n")
            for dataset, conf in failed_by_dataset.items():
                for group in conf["files"].keys():
                    fh.write(f"{dataset} {group}\n")
            fh.write(")\n")

        print(
            f"\nWrote hadd_failed.json ({n_failed} groups), "
            "hadd_failed.txt and hadd_failed_splitbyfile.sub. "
            "Submit with: condor_submit hadd_failed_splitbyfile.sub"
        )
        return

    if not dry and root_available:
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
            # Carry the authoritative pre-skim totals into the hadd dataset metadata as
            # well — so a hadded skim is also self-recovering w.r.t. zero-event input
            # chunks (see save_skimed_dataset_definition / BaseProcessorABC.postprocess).
            if s in df.get("sum_genweights", {}):
                metadata["sum_genweights"] = float(df["sum_genweights"][s])
            if s in df.get("sum_signOf_genweights", {}):
                metadata["sum_signOf_genweights"] = float(df["sum_signOf_genweights"][s])
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
        # Create the output directory if it does not exist
        output_dir = os.path.dirname(group[0])
        os.makedirs(output_dir, exist_ok=True)
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

    with open("do_hadd_job.py", "w") as f:
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
        # Create the output directory if it does not exist
        output_dir = os.path.dirname(group[0])
        os.makedirs(output_dir, exist_ok=True)
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
files = config[sys.argv[1]]["files"]

failed = []
for outputfile, inputfiles in files.items():
    out = do_hadd((outputfile, inputfiles))
    if out:
        failed.append(out)
        
print("DONE!")
print("Failed files: ", failed)
if len(failed):
    sys.exit(1)""")
        
    _write_do_hadd_job_splitbyfile_script("do_hadd_job_splitbyfile.py")


    abs_local_path = os.path.abspath(".")
    os.makedirs(f"{abs_local_path}/condor", exist_ok=True)
    sub = {
        "Executable": "do_hadd_job.py",
        "Universe": "vanilla",
        "Error": f"{abs_local_path}/condor/hadd_job_$(ClusterId).$(ProcId).err",
        "Output": f"{abs_local_path}/condor/hadd_job_$(ClusterId).$(ProcId).out",
        "Log": f"{abs_local_path}/condor/hadd_job_$(ClusterId).log",
        'MY.SendCredential': True,
        '+JobFlavour': f'"espresso"',
        'arguments': "$(dataset)",
        'should_transfer_files':'YES',
        'when_to_transfer_output' : 'ON_EXIT',
        'transfer_input_files' : f"{abs_local_path}/do_hadd_job.py, {abs_local_path}/hadd.json",
    }
    with open("hadd_job.sub", "w") as f:
        for k, v in sub.items():
            f.write(f"{k} = {v}\n")
        # Now adding the arguments
        f.write("queue dataset from (\n")
        for dataset, conf in groups_metadata.items():
            f.write(f'{dataset}\n')
        f.write(")\n")
    
    print("DONE!")
    
    sub = {
        "Executable": "do_hadd_job_splitbyfile.py",
        "Universe": "vanilla",
        "Error": f"{abs_local_path}/condor/hadd_job_$(ClusterId).$(ProcId).err",
        "Output": f"{abs_local_path}/condor/hadd_job_$(ClusterId).$(ProcId).out",
        "Log": f"{abs_local_path}/condor/hadd_job_$(ClusterId).log",
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
