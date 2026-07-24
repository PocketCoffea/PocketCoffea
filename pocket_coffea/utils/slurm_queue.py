"""SLURM partition/walltime helpers shared by the manual-job tooling.

The SLURM analogue of :mod:`pocket_coffea.utils.htcondor_queue`. SLURM sites
(e.g. the PSI Tier-3) schedule jobs by ``--partition`` + ``--time`` instead of
HTCondor's named ``+JobFlavour`` ladder. When a job is killed for hitting its
time limit we bump it up the ``PARTITIONS`` ladder *and* raise ``--time`` to the
new partition's limit (bumping the partition alone would not extend the
walltime); a user can also force a specific partition on resubmission.

The submission-file rewriters operate on the per-job ``job_<n>.slurm`` sbatch
scripts written by the manual SLURM executors (see
``executors_T3_CH_PSI.ExecutorFactorySlurmPSI``). Helpers that shell out to
SLURM (``sbatch``/``scancel``) are kept small and monkey-patchable so the
callers stay unit-testable on machines without SLURM.
"""

import glob
import os
import re

import yaml

# Partition ladder at the PSI Tier-3, shortest to longest walltime.
PARTITIONS = [
    "quick",
    "standard",
    "long",
]

# Walltime granted when a job is moved onto a partition by bump_queue/set_queue.
# These track the partition maxima at PSI T3; only the rewritten --time value
# depends on them, so being conservative is safe.
PARTITION_WALLTIMES = {
    "quick": "01:00:00",
    "standard": "12:00:00",
    "long": "7-00:00:00",
}


def _read_sbatch_option(line):
    """Extract the value from a ``#SBATCH --key=value`` line, tolerating both
    ``--key=value`` and ``--key value`` spacing."""
    payload = line.replace("#SBATCH", "", 1).strip()
    if "=" in payload:
        return payload.split("=", 1)[1].strip()
    parts = payload.split(None, 1)
    return parts[1].strip() if len(parts) > 1 else ""


def _rewrite_sbatch_options(slurm_file, replacements):
    """Rewrite ``#SBATCH --<key>`` lines in `slurm_file` according to
    `replacements` (a ``{key: new_value}`` dict, keys without leading dashes).

    Returns the ``{key: old_value}`` dict of the options actually found (a key
    missing from the file is left un-added — sbatch defaults then apply).
    """
    with open(slurm_file) as f:
        lines = f.readlines()
    found = {}
    with open(slurm_file, "w") as f:
        for line in lines:
            stripped = line.strip()
            matched = None
            if stripped.startswith("#SBATCH"):
                for key in replacements:
                    if re.match(rf"#SBATCH\s+--{re.escape(key)}[=\s]", stripped):
                        matched = key
                        break
            if matched is not None:
                found[matched] = _read_sbatch_option(stripped)
                f.write(f"#SBATCH --{matched}={replacements[matched]}\n")
            else:
                f.write(line)
    return found


def bump_queue(slurm_file, shift=1):
    """Bump the ``--partition`` in `slurm_file` up the ``PARTITIONS`` ladder by
    `shift` steps (capped at the longest partition), raising ``--time`` to the
    new partition's walltime.

    Returns the new partition name, or ``None`` when the file has no
    ``--partition`` line, in which case nothing is changed.
    """
    with open(slurm_file) as f:
        content = f.read()
    current = None
    for line in content.splitlines():
        stripped = line.strip()
        if stripped.startswith("#SBATCH") and re.match(r"#SBATCH\s+--partition[=\s]", stripped):
            current = _read_sbatch_option(stripped)
            break
    if current is None:
        return None
    if current in PARTITIONS:
        nxt = PARTITIONS[min(PARTITIONS.index(current) + shift, len(PARTITIONS) - 1)]
    else:
        # Unknown partition (site-specific extra queue): jump to the longest
        # known one rather than raising ValueError on PARTITIONS.index.
        nxt = PARTITIONS[-1]
    replacements = {"partition": nxt}
    if nxt in PARTITION_WALLTIMES:
        replacements["time"] = PARTITION_WALLTIMES[nxt]
    _rewrite_sbatch_options(slurm_file, replacements)
    return nxt


def set_queue(slurm_file, new_queue, job=None):
    """Force the ``--partition`` in `slurm_file` to `new_queue` (used by
    ``check-jobs --recreate-queue``), raising ``--time`` to the partition's
    walltime when the partition is a known one.

    Returns True when the partition was actually changed, False when it was
    already `new_queue` or the file has no ``--partition`` line.
    """
    replacements = {"partition": new_queue}
    if new_queue in PARTITION_WALLTIMES:
        replacements["time"] = PARTITION_WALLTIMES[new_queue]
    found = _rewrite_sbatch_options(slurm_file, replacements)
    if "partition" not in found:
        return False
    if found["partition"] == new_queue:
        return False
    label = f"{job}: " if job else ""
    print(f"[queue] {label}{found['partition']} -> {new_queue}")
    return True


def bump_mem(slurm_file, factor=2):
    """Multiply the ``--mem`` request in `slurm_file` by `factor` (used after an
    out-of-memory kill). Accepts ``--mem=4G``/``4000M``/``4000`` forms.

    Returns the new ``--mem`` string, or ``None`` when the file has no parseable
    ``--mem`` line (nothing is changed then).
    """
    with open(slurm_file) as f:
        content = f.read()
    current = None
    for line in content.splitlines():
        stripped = line.strip()
        if stripped.startswith("#SBATCH") and re.match(r"#SBATCH\s+--mem[=\s]", stripped):
            current = _read_sbatch_option(stripped)
            break
    if current is None:
        return None
    m = re.fullmatch(r"(\d+)\s*([KMGT]?)B?", current, flags=re.IGNORECASE)
    if m is None:
        return None
    new_mem = f"{int(m.group(1)) * factor}{m.group(2).upper()}"
    _rewrite_sbatch_options(slurm_file, {"mem": new_mem})
    return new_mem


# Markers slurmstepd (and the kernel oom reaper) leave in the job's stderr when
# the job is killed rather than failing on its own.
_TIMEOUT_MARKERS = ("DUE TO TIME LIMIT",)
_OOM_MARKERS = ("oom-kill", "oom_kill", "Out Of Memory", "OUT_OF_MEMORY")


def classify_slurm_log(log_path):
    """Classify a SLURM job log as ``"timeout"``, ``"oom"``, or ``None``.

    Scans for the slurmstepd cancellation messages (``CANCELLED ... DUE TO TIME
    LIMIT``, ``oom-kill``). Returns ``None`` for an unreadable/absent file so
    callers can probe logs that may still be being written.
    """
    try:
        with open(log_path, errors="replace") as f:
            content = f.read()
    except OSError:
        return None
    if any(marker in content for marker in _TIMEOUT_MARKERS):
        return "timeout"
    if any(marker in content for marker in _OOM_MARKERS):
        return "oom"
    return None


def detect_scheduler(jobs_folder):
    """Return the batch scheduler (``"condor"`` or ``"slurm"``) a manual jobs_dir
    was created for.

    Prefers the explicit ``scheduler`` key that SLURM executors write into
    ``jobs_config.yaml``; falls back to probing for ``job_<n>.slurm`` files.
    Defaults to ``"condor"`` (the historical scheduler, whose jobs_config
    predates the key).
    """
    cfg_path = os.path.join(str(jobs_folder), "jobs_config.yaml")
    if os.path.isfile(cfg_path):
        try:
            with open(cfg_path) as f:
                cfg = yaml.safe_load(f) or {}
            scheduler = cfg.get("scheduler")
            if scheduler:
                return scheduler
        except yaml.YAMLError:
            pass
    if glob.glob(os.path.join(str(jobs_folder), "job_*.slurm")):
        return "slurm"
    return "condor"


def read_job_name(slurm_file):
    """Return the ``--job-name`` declared in `slurm_file`, or ``None``."""
    try:
        with open(slurm_file) as f:
            lines = f.read().splitlines()
    except OSError:
        return None
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("#SBATCH") and re.match(r"#SBATCH\s+--job-name[=\s]", stripped):
            return _read_sbatch_option(stripped)
    return None


def scancel_job(slurm_file):
    """scancel any still-queued/running SLURM instance of the job described by
    `slurm_file`, matched on its ``--job-name`` (scancel only ever cancels the
    calling user's jobs). Returns the scancel output, or ``""`` when the file
    has no job-name to match on.
    """
    name = read_job_name(slurm_file)
    if not name:
        return ""
    try:
        return os.popen(f"scancel -v --jobname={name} 2>&1").read().strip()
    except Exception as e:
        return f"scancel failed: {e}"
