"""Helpers for detecting and handling held HTCondor jobs in the manual-job
executors' jobs folder (used by `pocket-coffea check-jobs`).

A job held by condor (e.g. for going over the memory limit) stays in the
queue and its wrapper never toggles the .running/.failed flag files, so the
only trace is the 012 event in the condor log. These helpers are
dependency-free (no rucio/rich/coffea) so they can be unit tested anywhere.
"""

import glob
import os
import re

_EVENT_PAT = re.compile(r"^(\d{3}) \((\d+)\.(\d+)\.\d+\)")


def parse_request_memory_mb(value):
    """Parse a condor RequestMemory value ("2GB", "4000MB", "4000") into MB.
    A bare number is MB, following the condor convention. Returns None if the
    value cannot be parsed."""
    m = re.match(r"^([\d.]+)\s*([KMGT]B?)?$", str(value).strip().strip('"'), re.IGNORECASE)
    if not m:
        return None
    scale = {"K": 1 / 1024, "M": 1, "G": 1024, "T": 1024 ** 2}
    unit = (m.group(2) or "M")[0].upper()
    return float(m.group(1)) * scale[unit]


def parse_hold_memory_mb(reason):
    """Extract (limit, last measured usage) in MB from a memory-hold reason like
    'Job has gone over cgroup memory limit of 12000 megabytes. Last measured
    usage: 23884 megabytes.'. Either can be None if not present."""
    limit = re.search(r"memory limit of (\d+)", reason)
    used = re.search(r"[Ll]ast measured usage: (\d+)", reason)
    return (int(limit.group(1)) if limit else None,
            int(used.group(1)) if used else None)


def bump_memory(sub_file, limit_mb=None, measured_mb=None):
    """Raise RequestMemory in the sub file after a memory hold. The new request
    is the largest of 2x the current request, 1.5x the enforced limit and 1.2x
    the measured usage (the limit can exceed the request: e.g. lxplus provisions
    3GB/core regardless of a smaller RequestMemory), rounded up to the next GB.
    Returns the new value in MB, or None if nothing could be computed."""
    with open(sub_file) as f:
        lines = f.readlines()
    current_mb = None
    for line in lines:
        if line.strip().lower().startswith("requestmemory"):
            current_mb = parse_request_memory_mb(line.split("=", 1)[1])
    candidates = [f * v for f, v in ((2, current_mb), (1.5, limit_mb), (1.2, measured_mb))
                  if v is not None]
    if not candidates:
        return None
    new_mb = ((int(max(candidates)) + 999) // 1000) * 1000
    new_line = f"RequestMemory = {new_mb}MB\n"
    with open(sub_file, "w") as f:
        for line in lines:
            if line.strip().lower().startswith("requestmemory"):
                f.write(new_line)
                new_line = None
            elif line.strip() == "queue" and new_line:
                # No RequestMemory line in the sub file: add one
                f.write(new_line)
                f.write(line)
                new_line = None
            else:
                f.write(line)
    return new_mb


def find_held_jobs(jobs_folder):
    """Scan every condor log in the jobs folder and return the jobs whose most
    recent event is a hold (012), i.e. currently held; a job that was held and
    then released or removed does not show up. Returns
    {(cluster_id, proc_id, job_id): (hold_reason, schedd)} where job_id is the
    pocket-coffea job number (the ProcId in the original cluster log
    job_<cluster>.log, the filename suffix in a resubmitted job's
    job_<cluster>.<job_id>.log) and schedd is the submission host (for
    condor_rm -name), or None if it could not be determined."""
    held = {}
    for log_file in glob.glob(f"{jobs_folder}/logs/job_*.log"):
        m = re.match(r"job_(\d+)(?:\.(\d+))?\.log$", os.path.basename(log_file))
        if not m:
            continue
        jobid_from_name = m.group(2)
        with open(log_file) as f:
            lines = f.readlines()
        schedd = None
        last_event = {}
        for il, line in enumerate(lines):
            em = _EVENT_PAT.match(line)
            if not em:
                continue
            code, cluster_id, proc_id = em.group(1), em.group(2), int(em.group(3))
            if schedd is None and code == "000":
                sm = re.search(r"alias=([\w.-]+)", line)
                schedd = sm.group(1) if sm else None
            reason = ""
            if code == "012" and il + 1 < len(lines):
                reason = lines[il + 1].strip()
            last_event[(cluster_id, proc_id)] = (code, reason)
        for (cluster_id, proc_id), (code, reason) in last_event.items():
            if code != "012":
                continue
            job_id = int(jobid_from_name) if jobid_from_name is not None else proc_id
            held[(cluster_id, proc_id, job_id)] = (reason, schedd)
    return held
