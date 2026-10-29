"""Helpers to split manual jobs in smaller jobs at file level (check-jobs --split-jobs).

Kept dependency-free so they can be unit-tested without rucio / condor.
"""
import os

import yaml

# ~1700 jobs with their file lists: the pure-python dumper takes ~12 s.
_Dumper = getattr(yaml, "CSafeDumper", yaml.SafeDumper)


def split_fileset(filesets, n):
    '''Split the files of a job fileset ({dataset: {"files", "metadata"}}) in
    min(n, nfiles) contiguous chunks of near-equal size. Each chunk keeps the
    dataset metadata, as the job splitting of the manual executors does.'''
    pairs = [(dataset, f) for dataset, fs in filesets.items() for f in fs["files"]]
    n = min(n, len(pairs))
    if n < 2:
        raise ValueError(f"cannot split a job with {len(pairs)} file(s)")
    chunks = []
    for i in range(n):
        chunk = {}
        for dataset, f in pairs[i * len(pairs) // n:(i + 1) * len(pairs) // n]:
            chunk.setdefault(dataset, {"files": [], "metadata": filesets[dataset]["metadata"]})["files"].append(f)
        chunks.append(chunk)
    return chunks


def save_jobs_config(jobs_dir, jobs_config):
    '''Atomic write: merge-outputs or another check-jobs can read the file meanwhile.'''
    path = os.path.join(jobs_dir, "jobs_config.yaml")
    with open(path + ".tmp", "w") as f:
        yaml.dump(jobs_config, f, Dumper=_Dumper)
    os.replace(path + ".tmp", path)
