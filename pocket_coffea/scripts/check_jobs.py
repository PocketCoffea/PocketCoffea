'''Simple script that checks the status of the jobs submitted by the manual-job
executors (HTCondor at lxplus/rubin, SLURM at T3_CH_PSI).

The scheduler is auto-detected from the jobs folder (``scheduler`` key in
jobs_config.yaml, falling back to probing for ``job_<n>.slurm`` files); all
flag-file handling is scheduler-agnostic, only submission, queue bumping and
kill-reason detection dispatch on it.

The status of the jobs can be checked by looking at the file in the jobs folder.

- job_x.idle: The job is waiting to be executed
- job_x.running: The job is running
- job_x.done: The job has finished
- job_x.failed: The job has failed
    
    where x is the job id.
'''

import os
import sys
import click
import glob
import subprocess as sp
import yaml
from pathlib import Path
from rich.console import Console
from rich.table import Table
from rich.progress import Progress
from rich import print as rprint
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
import time
import re
import cloudpickle
from copy import deepcopy
from pocket_coffea.utils.rucio import get_xrootd_sites_map, get_rucio_client
from pocket_coffea.utils.site_rewrite import (
    find_other_file,
    rewrite_fileset_blocklist,
    rewrite_fileset_to_redirector,
    GLOBAL_XROOTD_REDIRECTOR,
)
from pocket_coffea.utils import htcondor_queue, slurm_queue
from pocket_coffea.utils.htcondor_queue import QUEUES as queues, bump_queue, set_queue
from pocket_coffea.utils.slurm_queue import classify_slurm_log, detect_scheduler
from pocket_coffea.utils.job_progress import (
    aggregate_by_group,
    load_job_to_group_map,
    render_progress_bar,
)
from collections import Counter, defaultdict

# Backwards-compatible alias: check-jobs used to define bump_jobqueue locally.
# The implementation now lives in pocket_coffea.utils.htcondor_queue.
bump_jobqueue = bump_queue



def get_tables(tot_jobs, idle_jobs, running_jobs, done_jobs, failed_jobs, details=False):
    # Summary table
    table1 = Table(title="Job Summary")
    table1.add_column("Total jobs", style="cyan", no_wrap=True)
    table1.add_column("Idle jobs", style="blue", no_wrap=True)
    table1.add_column("Running jobs", style="magenta", no_wrap=True)
    table1.add_column("Done jobs", style="green", no_wrap=True)
    table1.add_column("Failed jobs", style="red", no_wrap=True)
    table1.add_row(str(len(tot_jobs)),
                  str(len(idle_jobs)),
                  str(len(running_jobs)),
                  str(len(done_jobs)),
                  str(len(failed_jobs)))
    # Create a table to display the status
    if details:
        table2 = Table(title="Job Status")
        table2.add_column("Job ID", style="cyan", no_wrap=True)
        table2.add_column("Submitted", style="blue", no_wrap=True)
        table2.add_column("Running", style="magenta", no_wrap=True)
        table2.add_column("Done", style="green", no_wrap=True)
        table2.add_column("Failed", style="red", no_wrap=True)
        for job in tot_jobs:
            table2.add_row(job,
                          "X" if job in idle_jobs else "",
                          "X" if job in running_jobs else "",
                          "X" if job in done_jobs else "",
                          "X" if job in failed_jobs else "")
    else:
        table2 = None
    return table1, table2


# Layout setup
def create_layout(with_progress=False):
    """Two-column layout. The left column carries the summary table (and
    the per-group progress table when `with_progress` is True) and gets
    twice the width of the log panel on the right, since that's where the
    interesting content lives."""
    layout = Layout()
    layout.split_row(
        Layout(name="left", ratio=2),
        Layout(name="right", ratio=1),
    )
    if with_progress:
        # Fixed-height summary panel so it doesn't grow at the expense of the
        # per-group table; 9 rows covers the Panel border + Table title +
        # header row + data row + a bit of padding. Bumped from 7 to fit
        # everything without cropping the bottom of the table.
        layout["left"].split_column(
            Layout(name="summary", size=9),
            Layout(name="progress"),
        )
    return layout

def check_jobs_logs(jobs_folder):
     # Idle jobs
    idle_jobs = [ a.split("/")[-1][:-5] for a in glob.glob(f"{jobs_folder}/job_*.idle")]
    # Running jobs
    running_jobs = [a.split("/")[-1][:-8] for a in glob.glob(f"{jobs_folder}/job_*.running")]
    # Done jobs
    done_jobs = [ a.split("/")[-1][:-5] for a in glob.glob(f"{jobs_folder}/job_*.done")]
    # Failed jobs
    failed_jobs = [ a.split("/")[-1][:-7] for a in glob.glob(f"{jobs_folder}/job_*.failed")]
    return idle_jobs, running_jobs, done_jobs, failed_jobs


def get_progress_table(group_counts, label, multi_sample_overlap=False, bar_width=30):
    """Build a rich Table showing per-group progress, sorted by % done
    ascending so straggling groups surface at the top. Includes a stacked
    coloured progress bar column (done / running / idle / failed)."""
    title = f"Progress by {label}"
    if multi_sample_overlap:
        title += "  [dim](jobs touching multiple samples are counted under each)[/]"
    table = Table(title=title)
    table.add_column(label.capitalize(), style="cyan", no_wrap=True)
    table.add_column("Total", justify="right")
    table.add_column("Idle", justify="right", style="blue")
    table.add_column("Running", justify="right", style="magenta")
    table.add_column("Done", justify="right", style="green")
    table.add_column("Failed", justify="right", style="red")
    table.add_column("Progress", justify="left", no_wrap=True)
    table.add_column("% Done", justify="right")

    rows = sorted(group_counts.items(), key=lambda kv: (kv[1]["pct_done"], kv[0]))
    for name, counts in rows:
        pct = f"{counts['pct_done']:.1f}%" if counts["total"] else "n/a"
        pct_style = "green" if counts["pct_done"] >= 99.5 else (
            "yellow" if counts["pct_done"] >= 50 else "red"
        )
        table.add_row(
            name,
            str(counts["total"]),
            str(counts["idle"]),
            str(counts["running"]),
            str(counts["done"]),
            str(counts["failed"]),
            render_progress_bar(counts, width=bar_width),
            f"[{pct_style}]{pct}[/]",
        )
    return table

def update_blacklist(xrootdfaillist,blacklist_threshold):
    sitepathlist = [i.split("/store/")[0] for i in xrootdfaillist]
    failedsitecounter = Counter(sitepathlist)
    blacklist_sites = []
    for site,fails in failedsitecounter.items():
        if fails > blacklist_threshold:
            blacklist_sites.append(site)
    return blacklist_sites

def condor_rm_job(job):
    """condor_rm any still-queued/running HTCondor instance of `job` (a
    ``job_<n>`` name) so a recreated job's old instance can't keep running and
    double-write its output.

    The instance is matched on the per-job ``config_job_<n>.pkl`` that appears
    in the job's condor ``Arguments`` (unique per job index, so ``job_1`` is not
    confused with ``job_10``). Returns the ``condor_rm`` output, or ``""`` if
    nothing matched.
    """
    idx = job.split("_")[-1]
    constraint = f'regexp("config_job_{idx}\\.pkl", Arguments)'
    try:
        return os.popen(f"condor_rm -constraint '{constraint}'").read().strip()
    except Exception as e:
        return f"condor_rm failed: {e}"


# ---------------------------------------------------------------------------
# Scheduler dispatch: everything flag-file based is scheduler-agnostic; only
# the submission-file extension, the submit/cancel CLIs and the queue model
# (condor +JobFlavour vs slurm --partition/--time) differ.

JOB_DESC_EXT = {"condor": "sub", "slurm": "slurm"}


def job_desc_file(jobs_folder, job, scheduler):
    """Path of the per-job submission file (``job_<n>.sub`` / ``job_<n>.slurm``)."""
    return f"{jobs_folder}/{job}.{JOB_DESC_EXT.get(scheduler, 'sub')}"


def queue_lib(scheduler):
    """The bump_queue/set_queue helper module for `scheduler`."""
    return slurm_queue if scheduler == "slurm" else htcondor_queue


def known_queues(scheduler):
    """The named queue/partition ladder for `scheduler`."""
    return slurm_queue.PARTITIONS if scheduler == "slurm" else queues


def submit_job_cmd(jobs_folder, job, scheduler):
    """Shell command resubmitting `job` with the scheduler's CLI."""
    if scheduler == "slurm":
        return f"cd {jobs_folder} && sbatch {job}.slurm"
    return f"cd {jobs_folder} && condor_submit {job}.sub"


def resubmit_job(jobs_folder, job, scheduler):
    """Resubmit `job` and parse the CLI output. Returns ``(log_line, success)``."""
    out = os.popen(submit_job_cmd(jobs_folder, job, scheduler), 'r').read()
    if scheduler == "slurm":
        line = out.strip().split("\n")[-1] if out.strip() else out
        return line, "Submitted batch job" in out
    # condor: the usual output ends with "1 job(s) submitted to cluster XXXX"
    if len(out.split('\n')) > 2:
        line = out.split('\n')[-2]
        return line, "job(s) submitted to cluster" in line
    return out, False


def remove_queued_job(jobs_folder, job, scheduler):
    """Kill any still-queued/running scheduler instance of `job`. Returns the
    scheduler CLI output."""
    if scheduler == "slurm":
        return slurm_queue.scancel_job(job_desc_file(jobs_folder, job, "slurm"))
    return condor_rm_job(job)


def recreate_jobs_oneshot(jobs_folder, jobs_to_recreate, *, use_redirector=False,
                          blocklist_sites=None, recreate_queue=None,
                          skip_bad_files=False, queue_shift=1, remove_running=False,
                          dry_run=False, scheduler=None):
    """One-shot recreate/resubmit of a chosen set of manual jobs.

    Ported from the manual-job executors' old ``--recreate-jobs`` path so the
    functionality lives in one place. Operates purely on the jobs_dir on-disk
    contract (``jobs_config.yaml`` + ``config_job_i.pkl`` + ``job_i.sub`` +
    the flag files), and — unlike the reactive ``--resubmit`` loop — can act on
    failed **and** running/idle jobs, e.g. to move everything off a blocklisted
    site or onto the global xrootd redirector mid-run.

    `jobs_to_recreate` is ``"auto"`` (scan ``*.failed``/``*.running``/``*.idle``
    flag files) or a comma list (``0,1,3`` or ``job_0,job_3``).

    When `remove_running` is set, each recreated job that is still queued in
    the scheduler (running/idle) is ``condor_rm``'d/``scancel``'d before
    resubmission so the old instance can't keep running and double-write its
    output.

    `scheduler` is ``"condor"``/``"slurm"``; when None it is auto-detected from
    the jobs folder.
    """
    jobs_folder = Path(jobs_folder)
    if scheduler is None:
        scheduler = detect_scheduler(jobs_folder)
    jobs_config_path = jobs_folder / "jobs_config.yaml"
    if not jobs_config_path.exists():
        rprint(f"[red]No jobs_config.yaml found in {jobs_folder}. Cannot recreate jobs.[/]")
        return
    with open(jobs_config_path) as f:
        jobs_config = yaml.safe_load(f)

    blocklist_sites = set(blocklist_sites or [])
    abs_jobdir = os.path.abspath(jobs_folder)

    # (Re)materialise the inner run-options YAML so a --skip-bad-files override
    # from this recreate call reaches the resubmitted jobs, idempotently
    # patching the wrapper/sub of an existing (possibly pre-feature) jobs_dir.
    ensure_sub_transfers = None
    if skip_bad_files:
        from pocket_coffea.executors.executors_manual_jobs import (
            write_inner_run_options,
            ensure_job_sh_forwards_inner_yaml,
            ensure_sub_transfers_inner_yaml,
            INNER_RUN_OPTIONS_FILENAME,
        )
        ensure_sub_transfers = ensure_sub_transfers_inner_yaml
        write_inner_run_options(str(jobs_folder), {"skip-bad-files": True})
        if ensure_job_sh_forwards_inner_yaml(f"{jobs_folder}/job.sh"):
            rprint(f"[recreate] Patched {jobs_folder}/job.sh to forward "
                   f"{INNER_RUN_OPTIONS_FILENAME} to the inner pocket-coffea run.")

    # Resolve the selector to a job list, and record which selected jobs are
    # currently failed/running (needed for the flag-flip and queue-bump below).
    if jobs_to_recreate == "auto":
        failedjobs = [f[:-len(".failed")] for f in os.listdir(jobs_folder) if f.endswith(".failed")]
        runningjobs = [f[:-len(".running")] for f in os.listdir(jobs_folder) if f.endswith(".running")]
        idlejobs = [f[:-len(".idle")] for f in os.listdir(jobs_folder) if f.endswith(".idle")]
        jobs_to_redo = failedjobs + runningjobs + idlejobs
        if not jobs_to_redo:
            rprint(f"[green]No *.failed/*.running/*.idle jobs found in {jobs_folder}; "
                   f"nothing to recreate.[/]")
            return
    else:
        jobs_to_redo = []
        for j in jobs_to_recreate.split(","):
            j = j.strip()
            if not j:
                continue
            jobs_to_redo.append(j if j.startswith("job_") else f"job_{j}")
        # Derive current flag states from disk (the old executor path left these
        # undefined for explicit lists, crashing on `job in runningjobs`).
        failedjobs = [j for j in jobs_to_redo if (jobs_folder / f"{j}.failed").exists()]
        runningjobs = [j for j in jobs_to_redo if (jobs_folder / f"{j}.running").exists()]
        idlejobs = [j for j in jobs_to_redo if (jobs_folder / f"{j}.idle").exists()]
    rprint(f"Recreating jobs: {jobs_to_redo}")

    # Optionally kill the still-queued (running/idle) HTCondor instance of each
    # recreated job so it can't keep running and double-write its output.
    if remove_running:
        queued = [j for j in jobs_to_redo if j in set(runningjobs) | set(idlejobs)]
        if queued:
            rprint(f"[recreate] Removing still-queued {scheduler} instances of: {queued}")
        for j in queued:
            if dry_run:
                rprint(f"[dim]Dry run, not removing the queued instance of {j}[/]")
                continue
            out = remove_queued_job(jobs_folder, j, scheduler)
            rprint(f"[recreate] remove {j}: {out or 'no matching queued job'}")

    # Jobs that failed due to an XRootD error get a per-file alternate-site lookup.
    xrootd_err_logs = os.popen(f"grep -il {jobs_folder}/logs/*.err -e 'XRootD error'").read().split()
    xrootd_fail_jobs = ["job_" + f.split("/")[-1].split(".")[-2] for f in xrootd_err_logs]
    if xrootd_err_logs:
        backupdir = f"{jobs_folder}/logs/processed"
        os.makedirs(backupdir, exist_ok=True)
        os.system(f"mv {' '.join(xrootd_err_logs)} {backupdir}")

    sitemap = get_xrootd_sites_map()
    if blocklist_sites:
        rprint(f"Blocklisting sites at recreate time: {sorted(blocklist_sites)}")

    # --use-redirector takes precedence over the per-site blocklist rewrite:
    # a "no Rucio, everything on the global xrootd redirector" one-shot.
    if use_redirector:
        rprint(f"[recreate] --use-redirector: rewriting every file to "
               f"{GLOBAL_XROOTD_REDIRECTOR} without per-site Rucio lookups.")
        if blocklist_sites:
            rprint("[recreate] WARNING: --blocklist-sites is set but --use-redirector "
                   "overrides it (no per-site Rucio resolution happens).")

    rucio_client = None
    if (xrootd_fail_jobs or blocklist_sites) and not use_redirector:
        try:
            rucio_client = get_rucio_client()
        except Exception as e:
            rprint(f"[yellow]WARNING: could not open a rucio client ({e}); "
                   f"replica lookups will fail.[/]")

    if recreate_queue is not None and recreate_queue not in known_queues(scheduler):
        rprint(f"[yellow]WARNING: recreate-queue={recreate_queue!r} is not in the known "
               f"{scheduler} queue list {known_queues(scheduler)}; writing your value verbatim.[/]")

    for job in jobs_to_redo:
        if job not in jobs_config["jobs_list"]:
            rprint(f"[yellow]Job {job} not found in jobs_config.yaml; skipping.[/]")
            continue

        # Source the ORIGINAL fileset from jobs_config.yaml (from-scratch) so
        # repeated recreates don't compound rewrites.
        new_fileset = deepcopy(jobs_config["jobs_list"][job]["filesets"])
        modified = False

        if use_redirector:
            new_fileset = rewrite_fileset_to_redirector(new_fileset)
            modified = True
        else:
            if job in xrootd_fail_jobs:
                rprint(f"Replacing input files in {job} since it failed due to an XRootD error.")
                for sample, dct in new_fileset.items():
                    dct['files'] = [
                        find_other_file(fl, sitemap, blocklist=blocklist_sites,
                                        rucio_client=rucio_client)
                        for fl in dct['files']
                    ]
                modified = True
            if blocklist_sites:
                new_fileset = rewrite_fileset_blocklist(new_fileset, sitemap, blocklist_sites,
                                                        rucio_client=rucio_client)
                modified = True

        if modified:
            cfgfile = f"{jobs_folder}/config_{job}.pkl"
            config = cloudpickle.load(open(cfgfile, "rb"))
            config.set_filesets_manually(new_fileset)
            cloudpickle.dump(config, open(cfgfile, "wb"))

        desc_file = job_desc_file(jobs_folder, job, scheduler)
        # The transfer_input_files patch only applies to condor .sub files; the
        # SLURM wrapper references the inner YAML by absolute path already.
        if skip_bad_files and ensure_sub_transfers is not None and scheduler == "condor":
            ensure_sub_transfers(desc_file, abs_jobdir)

        # Explicit queue override wins over the implicit timeout bump.
        if recreate_queue is not None:
            queue_lib(scheduler).set_queue(desc_file, recreate_queue, job)
        elif job in runningjobs:
            queue_lib(scheduler).bump_queue(desc_file, queue_shift)

        if dry_run:
            rprint(f"[dim]Dry run, not resubmitting {job}[/]")
            continue
        if job in failedjobs:
            os.system(f"rm {jobs_folder}/{job}.failed")
        elif job in runningjobs:
            os.system(f"rm {jobs_folder}/{job}.running")
        os.system(f"touch {jobs_folder}/{job}.idle")
        os.system(submit_job_cmd(jobs_folder, job, scheduler))
        rprint(f"[green]Resubmitted {job}[/]")


@click.command()
@click.option("-j", "--jobs-folder", type=str, help="Folder containing the jobs", required=True)
@click.option("-d","--details", is_flag=True, help="Show the details of the jobs")
@click.option("-r","--resubmit", is_flag=True, help="Resubmit the failed jobs")
@click.option("-m","--max-resubmit", type=int, help="Maximum number of resubmission", default=4)
@click.option("-b","--blacklist-threshold", type=int, help="Maximum number of allowed failed files at an xrootd site before it's blacklisted", default=3)
@click.option("-q","--queue-shift", type=int, help="How many queues to bump to if a job is removed due to time limit? E.g. 1 = bump to next queue, 2 = bump to next-to-next queue", default=1)
@click.option("--by", "group_by", type=click.Choice(["sample", "dataset", "none"]),
              default="sample",
              help="Show a per-group progress table below the summary. Requires "
                   "jobs_config.yaml in the jobs folder (created by manual-job "
                   "executors). Pass 'none' to disable. Default: sample.")
@click.option("--recreate", type=str, default=None,
              help="One-shot proactive recreate/resubmit of a chosen set of jobs. "
                   "Pass 'auto' (all *.failed/*.running/*.idle jobs) or a comma list "
                   "(e.g. 0,1,3 or job_0,job_3). Unlike --resubmit (which only reacts "
                   "to jobs that have already failed), this can act on running/idle "
                   "jobs too. Runs the recreate pass, then exits — unless --resubmit is "
                   "also given, in which case it continues into the babysitter loop.")
@click.option("--once", is_flag=True, default=False,
              help="Run a single monitor/resubmit iteration then exit, instead of "
                   "looping until all jobs finish.")
@click.option("--use-redirector", is_flag=True, default=False,
              help="Rewrite files through the global xrootd redirector "
                   f"({GLOBAL_XROOTD_REDIRECTOR}). With --recreate, every file of each "
                   "recreated job is pointed at the redirector (no Rucio lookups). In "
                   "the --resubmit loop, it is used as the fallback when no alternative "
                   "site is found for a failed file (instead of resubmitting unchanged).")
@click.option("--blocklist-sites", type=str, default=None,
              help="Comma-separated CMS site names (or xrootd prefixes) to avoid. "
                   "Unioned with check-jobs' automatic count-based blacklist. Files at "
                   "a blocklisted site are rewritten to an alternative replica via Rucio.")
@click.option("--recreate-queue", type=str, default=None,
              help="Force each resubmitted job's queue: the HTCondor +JobFlavour "
                   "(espresso, microcentury, longlunch, workday, tomorrow, testmatch, "
                   "nextweek) or the SLURM partition (quick, standard, long). Overrides "
                   "the implicit --queue-shift bump for jobs removed due to time limit.")
@click.option("--skip-bad-files", is_flag=True, default=False,
              help="Retroactively enable Coffea's skip-bad-files in the inner job of the "
                   "recreated/resubmitted jobs by (re)materialising inner_run_options.yaml "
                   "and patching the jobs_dir. Most useful together with --recreate.")
@click.option("--remove-running", is_flag=True, default=False,
              help="With --recreate, kill each recreated job's still-queued "
                   "(running/idle) scheduler instance before resubmitting, so a stuck job "
                   "can't keep running and double-write its output. HTCondor: condor_rm "
                   "matched on the unique config_job_<n>.pkl in the job's Arguments; "
                   "SLURM: scancel matched on the job_<n>.slurm --job-name.")
def check_jobs(jobs_folder, details, resubmit, max_resubmit, blacklist_threshold,
               queue_shift, group_by, recreate, once, use_redirector, blocklist_sites,
               recreate_queue, skip_bad_files, remove_running):
    # check if the user passed the parent folder
    subdirs = os.listdir(jobs_folder)
    if len(subdirs) == 1 and subdirs[0] == "job":
        jobs_folder = os.path.join(jobs_folder,"job")

    jobs_folder = Path(jobs_folder)
    scheduler = detect_scheduler(jobs_folder)
    if scheduler != "condor":
        rprint(f"[dim]Detected batch scheduler: {scheduler}[/]")

    # Parse the explicit --blocklist-sites list once (unioned with the auto
    # blacklist inside the resubmit loop).
    explicit_blocklist = {s.strip() for s in blocklist_sites.split(",")} if blocklist_sites else set()
    explicit_blocklist.discard("")
    # In the reactive loop, only fall back to the redirector when the user asked
    # for it; otherwise a failed file with no alternative is resubmitted unchanged.
    redirector_fallback = GLOBAL_XROOTD_REDIRECTOR if use_redirector else None

    # One-shot proactive recreate pass (replaces the old `run --recreate-jobs`).
    if recreate is not None:
        recreate_jobs_oneshot(
            jobs_folder, recreate,
            use_redirector=use_redirector,
            blocklist_sites=explicit_blocklist,
            recreate_queue=recreate_queue,
            skip_bad_files=skip_bad_files,
            queue_shift=queue_shift,
            remove_running=remove_running,
            scheduler=scheduler,
        )
        if not resubmit:
            return
    # Get the list of files in the folder
    desc_ext = JOB_DESC_EXT.get(scheduler, "sub")
    tot_jobs = [ a.split("/")[-1][:-(len(desc_ext) + 1)]
                 for a in glob.glob(f"{jobs_folder}/job_*.{desc_ext}") ]
    # Redo everything every 5 sec
    console = Console()

    failed_jobs_stats = {}
    tot_done = 0

    maxtimefile = f"{jobs_folder}/maxtime.txt"
    if os.path.isfile(maxtimefile):
            with open(maxtimefile,"r") as f:
                maxtimelist = [l.strip() for l in f.readlines()]
    else:
        maxtimelist = []

    # Load the per-job sample/dataset map if available and the user opted in.
    group_to_jobs = None
    group_label = None
    multi_sample_overlap = False
    if group_by != "none":
        sample_to_jobs, dataset_to_jobs = load_job_to_group_map(jobs_folder)
        if sample_to_jobs is None:
            rprint(f"[yellow]No jobs_config.yaml in {jobs_folder}; per-group progress "
                   f"table disabled.[/]")
        else:
            if group_by == "sample":
                group_to_jobs = sample_to_jobs
                group_label = "sample"
            else:
                group_to_jobs = dataset_to_jobs
                group_label = "dataset"
            # Detect uniform-split overlap: any job appearing under more than one group.
            all_jobs_listed = [j for jobs in group_to_jobs.values() for j in jobs]
            multi_sample_overlap = len(all_jobs_listed) != len(set(all_jobs_listed))

    if resubmit:
        sitemap = get_xrootd_sites_map()
        try:
            rucio_client = get_rucio_client()
        except Exception as e:
            print(f"WARNING: could not open a rucio client ({e}); replica lookups will fail.")
            rucio_client = None
        xrootdfailfile = f"{jobs_folder}/xrootdfaillist.txt"
        if os.path.isfile(xrootdfailfile):
            with open(xrootdfailfile,"r") as f:
                xrootdfaillist = [l.strip() for l in f.readlines()]
        else:
            xrootdfaillist = []
        os.makedirs(f"{jobs_folder}/logs/processedlogs", exist_ok=True)
        # Union the explicit --blocklist-sites with the automatic count-based
        # blacklist; the explicit list is never dropped on later recomputes.
        blacklist_sites = list(set(update_blacklist(xrootdfaillist,blacklist_threshold)) | explicit_blocklist)
        if len(blacklist_sites) > 0:
            print("Blacklisted sites:",blacklist_sites)
    
    # Main loop
    show_progress = group_to_jobs is not None
    layout = create_layout(with_progress=show_progress)
    idle_jobs, running_jobs, done_jobs, failed_jobs = check_jobs_logs(jobs_folder)
    tables = get_tables(tot_jobs, idle_jobs, running_jobs, done_jobs, failed_jobs, details=details)
    if show_progress:
        layout["summary"].update(Panel(tables[0], title="Job Status"))
        gc = aggregate_by_group(group_to_jobs, idle_jobs, running_jobs, done_jobs, failed_jobs)
        layout["progress"].update(Panel(
            get_progress_table(gc, group_label, multi_sample_overlap=multi_sample_overlap)))
    else:
        layout["left"].update(Panel(tables[0], title="Job Status"))
    layout["right"].update(Panel("No logs yet", title="Log"))
    
    log_text = []
    definitive_failed = []
    resubmitted_and_failed = []
    step = 0
    
    with Live(layout, refresh_per_second=1/5, console=console):  # Refresh rate
        try:
            while True:
                step += 1
                idle_jobs, running_jobs, done_jobs, failed_jobs = check_jobs_logs(jobs_folder)
                tables = get_tables(tot_jobs, idle_jobs, running_jobs, done_jobs, failed_jobs, details=details)
                # Update the left panel(s) with fresh tables
                if show_progress:
                    layout["summary"].update(Panel(tables[0], title="Job Status"))
                    gc = aggregate_by_group(group_to_jobs, idle_jobs, running_jobs, done_jobs, failed_jobs)
                    layout["progress"].update(Panel(
                        get_progress_table(gc, group_label, multi_sample_overlap=multi_sample_overlap),
                        title=f"Progress by {group_label}"))
                else:
                    layout["left"].update(Panel(tables[0], title="Job Status"))

                # Checking failed jobs
                if len(failed_jobs) > 0:
                    if len(failed_jobs) > len(definitive_failed) and not resubmit:
                        log_text.append("[red]Failed jobs found. Check the details below. Use --resubmit to resubmit the failed jobs[/]")
                    resubmit_count = 0
                    for failed_job in failed_jobs:
                        if failed_job in failed_jobs_stats:
                            if failed_job not in definitive_failed:
                                failed_jobs_stats[failed_job] += 1
                        else:
                            failed_jobs_stats[failed_job] = 1

                        failed_job_num = failed_job.split('_')[1]

                        if not failed_job in definitive_failed:
                            # Check the log file. Under SLURM the python
                            # traceback lands in the .err (sbatch --error), so
                            # probe those too, preferring the most recent.
                            glob_file = glob.glob(f"{jobs_folder}/logs/job_*.{failed_job_num}.out")
                            if scheduler == "slurm":
                                glob_file += glob.glob(f"{jobs_folder}/logs/job_*.{failed_job_num}.err")
                            xrootdfile = None
                            if glob_file:
                                with open(glob_file[-1]) as f:
                                    c = f.readlines()
                                    for iln,ln in enumerate(c):
                                        if "OSError: XRootD error" in ln:
                                            if iln + 1 < len(c):
                                                parts = c[iln+1].strip().split()
                                                if parts:
                                                    xrootdfile = parts[-1]
                                            break
                                        if "FileNotFoundError: file not found" in ln:
                                            if iln + 3 < len(c):
                                                xrootdfile = c[iln+3].strip().strip("'")
                                            break
                                    if xrootdfile:
                                        thisxrootdsite = xrootdfile.split('/store/')[0]
                                        log_text.append( f"[b]Job {failed_job} failed[/] {failed_jobs_stats[failed_job]} times due to an XRootD error. Site: {thisxrootdsite}")
                                    else:
                                        log_text.append( f"[b]Job {failed_job} failed[/] {failed_jobs_stats[failed_job]} times. Last error:")
                                        log_text.append("\t"+ "".join(c[-3:]))
                            else:
                                log_text.append( f"Error in job {failed_job}: No .err file found")

                            if resubmit and failed_jobs_stats[failed_job] <= max_resubmit:
                                if xrootdfile:
                                    # Include the failed file in the global list so that it's not reused later
                                    if xrootdfile not in xrootdfaillist:
                                        with open(xrootdfailfile,"a") as f:
                                            f.write(xrootdfile+"\n")
                                        xrootdfaillist.append(xrootdfile)
                                        # Keep the explicit --blocklist-sites unioned in so it's
                                        # never dropped when the auto blacklist is recomputed.
                                        new_blacklist_sites = list(set(update_blacklist(xrootdfaillist,blacklist_threshold)) | explicit_blocklist)
                                        if len(new_blacklist_sites) > len(blacklist_sites):
                                            added = sorted(set(new_blacklist_sites) - set(blacklist_sites))
                                            log_text.append(f"[red][b]New blacklist sites[/]: {added}[/]")
                                            blacklist_sites = new_blacklist_sites

                                    # Move the .err file so that this xrootdfile is not marked again as an XRootD failure
                                    if glob_file:
                                        os.system(f"mv {glob_file[-1]} {jobs_folder}/logs/processedlogs")

                                    # Update the filelist in the failed job's config to exclude this failed file
                                    thisconfigfile = f"{jobs_folder}/config_{failed_job}.pkl"
                                    config = cloudpickle.load(open(thisconfigfile, "rb"))
                                    current_fileset = config.filesets
                                    new_fileset = deepcopy(current_fileset)
                                    for sample, dct in new_fileset.items():
                                        fllist = dct['files']
                                        if xrootdfile in fllist:
                                            flidx = fllist.index(xrootdfile)
                                            newfl = find_other_file(xrootdfile, sitemap,
                                                                    blocklist=blacklist_sites,
                                                                    exclude_urls=xrootdfaillist,
                                                                    fallback_redirector=redirector_fallback,
                                                                    rucio_client=rucio_client)
                                            if newfl != xrootdfile:
                                                new_fileset[sample]['files'][flidx] = newfl
                                                config.set_filesets_manually(new_fileset)
                                                cloudpickle.dump(config, open(thisconfigfile, "wb"))
                                                log_text.append(f"[b]Job {failed_job}[/]: Updated XRootD path of failed file to a new site.")
                                            else:
                                                log_text.append(f"[b]Job {failed_job}[/]: No alternative site found for {xrootdfile}. Resubmitting with the same file!")
                                        
                                    # Enforce blacklist              
                                    # Take this opportunity to replace all files in the config that are
                                    # at one of the blacklisted sites          
                                    if len(blacklist_sites) > 0:
                                        flcounter = 0
                                        samecounter = 0
                                        sitecounter = []
                                        for sample, dct in new_fileset.items():
                                            fllist = dct['files']
                                            newfllist = []
                                            for flname in fllist:
                                                thissite = flname.split("/store/")[0]
                                                if thissite in blacklist_sites:
                                                    newfl = find_other_file(flname, sitemap,
                                                                            blocklist=blacklist_sites,
                                                                            exclude_urls=xrootdfaillist,
                                                                            fallback_redirector=redirector_fallback,
                                                                            rucio_client=rucio_client)
                                                    newfllist.append(newfl)
                                                    if newfl != flname:
                                                        flcounter += 1
                                                        if thissite not in sitecounter:
                                                            sitecounter.append(thissite)
                                                    else:
                                                        samecounter += 1
                                                else:
                                                    newfllist.append(flname)
                                            
                                            new_fileset[sample]['files'] = newfllist

                                        config.set_filesets_manually(new_fileset)
                                        cloudpickle.dump(config, open(thisconfigfile, "wb"))

                                        if flcounter > 0:
                                            log_text.append(f"[b]Job {failed_job}[/]: Replaced {flcounter} files in config because they were in {len(sitecounter)} blacklisted sites: {sitecounter}.")
                                        if samecounter > 0:
                                            log_text.append(f"[red][b]Job {failed_job}[/]: Could not replace {samecounter} files in config though they were in blacklisted sites, because no alternative site was found![/]")

                                resubmit_log, resubmit_succeeded = resubmit_job(
                                    jobs_folder, failed_job, scheduler)

                                log_text.append(resubmit_log)
                                if resubmit_succeeded:
                                    os.system(f"rm {jobs_folder}/{failed_job}.failed")
                                    os.system(f"touch {jobs_folder}/{failed_job}.idle")
                                    resubmit_count += 1

                                if resubmit_count % 10 == 0:
                                    rprint(f"[green]Resubmitted {resubmit_count}/{len(failed_jobs)} jobs so far in step {step}[/]")   # Terminal output so that the user knows something's going on
                            else:
                                # Add it to the list of jobs that are definitely failed
                                definitive_failed.append(failed_job)
                    if resubmit_count > 0:
                        log_text.append(f"[red]Resubmitted {resubmit_count} failed jobs to {scheduler}[/]")

                # check in the logs for SYSTEM_PERIODIC_REMOVE
                # they are not failed but remain running/idle
                # (condor event logs only: under SLURM this block no-ops and the
                # slurm-specific scan below takes over.)
                _log_files = glob.glob(f"{jobs_folder}/logs/job_*.log") if scheduler == "condor" else []
                c = []
                if _log_files:  # no logs yet on the first iterations -> skip
                    with open(_log_files[0]) as f:
                        c = f.readlines()

                for il, line in enumerate(c):
                    if line.startswith("009"):
                        # Match with a regex the job id from this
                        # line format "005 (5189350.010.000) 11/15 21:29:13 Job was aborted
                        pattern = re.compile(r"\((\d+)\.(\d+)\.\d+\)")
                        match = pattern.search(line)
                        if match:
                            cluster_id = match.group(1)
                            job_id = int(match.group(2))    # 010 -> 10
                            job_name = f"{cluster_id}_{job_id}"

                            # If this job was already resubmitted, skip
                            if job_name in maxtimelist:
                                continue

                            thisjob = f"job_{job_id}"
                            if thisjob in running_jobs or thisjob in idle_jobs:
                                if thisjob in running_jobs:
                                    running_jobs.remove(thisjob)
                                    os.system(f"rm {jobs_folder}/{thisjob}.running")
                                
                                # Sometimes jobs which never run also get aborted; they have the idle tag
                                # but exist in the log file as and aborted job
                                if thisjob in idle_jobs:
                                    idle_jobs.remove(thisjob)
                                    os.system(f"rm {jobs_folder}/{thisjob}.idle")

                                failed_jobs.append(thisjob)                                
                                os.system(f"touch {jobs_folder}/{thisjob}.failed")

                                maxtimelist.append(job_name)
                                with open(maxtimefile,'a') as f:
                                    f.write(job_name+"\n")

                                # Modify the sub file
                                # Check if next line has SYSTEM_PERIODIC_REMOVE
                                if not "SYSTEM_PERIODIC_REMOVE" in c[il+1]:
                                    log_text.append(f"{thisjob} was aborted by condor. Check the log file for more details")
                                else:
                                    sub_file = f"{jobs_folder}/{thisjob}.sub"
                                    if recreate_queue is not None:
                                        set_queue(sub_file, recreate_queue, thisjob)
                                        next_jf = recreate_queue
                                    else:
                                        next_jf = bump_queue(sub_file, queue_shift)

                                    log_text.append(f"{thisjob} was removed by the system due to max-time reached. Marked as failed and bumped to longer condor queue: {next_jf}.")

                                    # No need to resubmit, just let the next pass handle it in its first section
                                    # os.system(f"cd {jobs_folder} && condor_submit {thisjob}.sub")
                                    # os.system(f"rm {jobs_folder}/{thisjob}.failed")
                                    # os.system(f"touch {jobs_folder}/{thisjob}.idle")
                
                # Now check jobs which were resubmitted by this script but then failed again
                # Look for "job was aborted"
                failedlogs = [] if scheduler != "condor" else \
                    os.popen(f'grep -il {jobs_folder}/logs/job_*.log -e "Job was aborted"').read().split("\n")[:-1]
                for failedlog in failedlogs:
                    # Skip the primary/original log (already handled by the
                    # SYSTEM_PERIODIC_REMOVE block above). `log_file` used to be
                    # undefined here, crashing the loop with a NameError.
                    if _log_files and _log_files[0].split("/")[-1] in failedlog:
                        continue
                    failedlogcluster = failedlog.split("/")[-1].split(".")[0]
                    jobid = None
                    try:
                        jobid = failedlog.split("/")[-1].split(".")[1]
                    except:
                        pass

                    if not jobid:
                        if failedlog not in resubmitted_and_failed:
                            # Report once, but don't keep reporting the same thing
                            log_text.append(f"[red]Detected a failed job log {failedlog} but could not determine the job id.[/]")
                            resubmitted_and_failed.append(failedlog)
                    else:
                        job_name = f"{failedlogcluster}_{jobid}"
                        if job_name in maxtimelist:
                            continue

                        thisjob = f"job_{jobid}"
                        if thisjob in running_jobs or thisjob in idle_jobs:
                            if thisjob in running_jobs:
                                running_jobs.remove(thisjob)
                                os.system(f"rm {jobs_folder}/{thisjob}.running")
                            
                            # Sometimes jobs which never run also get aborted; they have the idle tag
                            # but exist in the log file as and aborted job
                            if thisjob in idle_jobs:
                                idle_jobs.remove(thisjob)
                                os.system(f"rm {jobs_folder}/{thisjob}.idle")

                            failed_jobs.append(thisjob)                                
                            os.system(f"touch {jobs_folder}/{thisjob}.failed")

                            maxtimelist.append(job_name)
                            with open(maxtimefile,'a') as f:
                                f.write(job_name+"\n")

                            # Modify the sub file
                            # Check if log file has SYSTEM_PERIODIC_REMOVE
                            with open(failedlog,"r") as f:
                                lines = f.readlines()
                            
                            dobump = False
                            for line in lines:
                                if "SYSTEM_PERIODIC_REMOVE" in line:
                                    dobump = True
                                    break

                            if not dobump:
                                log_text.append(f"Resubmitted job, {thisjob}, was aborted [i]again[/] by condor. Check the log file for more details")
                            else:
                                sub_file = f"{jobs_folder}/{thisjob}.sub"
                                if recreate_queue is not None:
                                    set_queue(sub_file, recreate_queue, thisjob)
                                    next_jf = recreate_queue
                                else:
                                    next_jf = bump_queue(sub_file, queue_shift)

                                log_text.append(f"Resubmitted job, {thisjob}, was removed [i]again[/] by the system due to max-time reached. Marked as failed and bumped to longer condor queue: {next_jf}.")
                            
                            os.system(f"mv {failedlog} {jobs_folder}/logs/processedlogs")

                # SLURM kill-reason detection: there is no condor-style event
                # log — a job killed for its walltime or by the OOM killer just
                # leaves its .running flag behind, and slurmstepd writes the
                # reason into the job's stderr (logs/job_<slurmid>.<idx>.err).
                # Flip the flags so the resubmit section picks the job up, and
                # bump the partition/--time (timeout) or --mem (OOM).
                if scheduler == "slurm":
                    for errlog in glob.glob(f"{jobs_folder}/logs/job_*.err"):
                        parts = os.path.basename(errlog).split(".")
                        if len(parts) != 3:   # expect job_<slurmid>.<idx>.err
                            continue
                        sched_id = parts[0][len("job_"):]
                        job_idx = parts[1]
                        job_key = f"{sched_id}_{job_idx}"
                        if job_key in maxtimelist:
                            continue
                        kind = classify_slurm_log(errlog)
                        if kind is None:
                            continue
                        thisjob = f"job_{job_idx}"
                        if thisjob not in running_jobs and thisjob not in idle_jobs:
                            continue
                        if thisjob in running_jobs:
                            running_jobs.remove(thisjob)
                            os.system(f"rm {jobs_folder}/{thisjob}.running")
                        if thisjob in idle_jobs:
                            idle_jobs.remove(thisjob)
                            os.system(f"rm {jobs_folder}/{thisjob}.idle")
                        failed_jobs.append(thisjob)
                        os.system(f"touch {jobs_folder}/{thisjob}.failed")

                        maxtimelist.append(job_key)
                        with open(maxtimefile, 'a') as f:
                            f.write(job_key + "\n")

                        slurm_file = f"{jobs_folder}/{thisjob}.slurm"
                        if kind == "timeout":
                            if recreate_queue is not None:
                                slurm_queue.set_queue(slurm_file, recreate_queue, thisjob)
                                next_q = recreate_queue
                            else:
                                next_q = slurm_queue.bump_queue(slurm_file, queue_shift)
                            log_text.append(f"{thisjob} was killed by SLURM for hitting its "
                                            f"time limit. Marked as failed and moved to "
                                            f"partition: {next_q}.")
                        else:  # oom
                            new_mem = slurm_queue.bump_mem(slurm_file)
                            log_text.append(f"{thisjob} was killed by the OOM killer. Marked "
                                            f"as failed and --mem raised to {new_mem}.")
                        # Park the classified log so a stale copy can't
                        # re-trigger the flag flip after resubmission.
                        os.makedirs(f"{jobs_folder}/logs/processedlogs", exist_ok=True)
                        os.system(f"mv {errlog} {jobs_folder}/logs/processedlogs")

                if len(log_text):
                    if len(log_text) > 20:
                        log_text = log_text[-20:]
                    layout["right"].update(Panel("\n".join(log_text), title="Log"))

                if len(tot_jobs) == len(done_jobs) + len(failed_jobs):
                    rprint("[green]All jobs are completed[/]")
                    rprint(f"Now merge outputs with [yellow]merge-outputs -jc {jobs_folder}[/].")
                    break
                if once:
                    # Single pass requested: stop after one monitor/resubmit iteration.
                    break
                time.sleep(5)
        except KeyboardInterrupt:
            pass
        except Exception as e:
            # The babysitter can run for days; degrade gracefully on an unexpected
            # error (e.g. a log being written concurrently) with a clear message
            # instead of dumping a raw traceback and dying silently.
            rprint(f"[red]check-jobs stopped on an unexpected error:[/] {e!r}")
            raise

if __name__ == "__main__":
    check_jobs()
