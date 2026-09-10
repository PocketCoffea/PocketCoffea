'''Monitor current-format PocketCoffea manual jobs.'''

import fcntl
import glob
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from copy import deepcopy
from functools import wraps
from pathlib import Path

import click
import cloudpickle
import yaml
from rich import print as rprint
from rich.console import Console
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.progress import BarColumn, Progress, TextColumn
from rich.table import Table

from pocket_coffea.executors.executors_manual_jobs import render_condor_submit
from pocket_coffea.utils.htcondor_queue import next_queue
from pocket_coffea.utils.job_progress import aggregate_by_group, load_job_to_group_map, render_progress_bar
from pocket_coffea.utils.network import get_proxy_path
from pocket_coffea.utils.rucio import get_xrootd_sites_map, get_rucio_client
from pocket_coffea.utils.site_rewrite import (
    extract_failed_url, find_other_file, normalize_rse,
    rewrite_fileset_blocklist, rewrite_fileset_to_redirector,
)

LOCK_FILENAME = ".check_jobs.lock"
JOB_MARKERS = ("idle", "running", "done", "failed", "timeout")
CONDOR_REMOVAL_TIMEOUT = 10.0
CONTRACT_ERROR = (
    "This jobs directory predates the consolidated check-jobs format.\n"
    "Please resubmit it with the current PocketCoffea version."
)


def _resolve_jobs_folder(jobs_folder):
    folder = Path(jobs_folder)
    if len(os.listdir(folder)) == 1 and (folder / "job").is_dir():
        return folder / "job"
    return folder


def shlex_quote(value):
    import shlex
    return shlex.quote(value)


def _new_lock_info():
    return {
        "hostname": socket.gethostname(), "pid": os.getpid(),
        "start_time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "command_line": " ".join(shlex_quote(arg) for arg in sys.argv),
        "session_id": uuid.uuid4().hex,
    }


def acquire_check_jobs_lock(jobs_folder, ignore_lock=False):
    path = Path(jobs_folder) / LOCK_FILENAME
    info = _new_lock_info()
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    except FileExistsError:
        with path.open("r+") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX)
            existing = json.load(handle)
            rprint(f"[yellow]check-jobs is already running on {existing.get('hostname', 'unknown host')} "
                   f"(PID {existing.get('pid', 'unknown')}). Use --ignore-lock to skip this check (risky!).[/]")
            if not ignore_lock:
                return None
            handle.seek(0)
            handle.truncate()
            json.dump(info, handle)
            handle.write("\n")
            handle.flush()
            return info
    with os.fdopen(fd, "w") as handle:
        json.dump(info, handle)
        handle.write("\n")
    return info


def release_check_jobs_lock(jobs_folder, session_id):
    path = Path(jobs_folder) / LOCK_FILENAME
    try:
        with path.open("r+") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX)
            if json.load(handle).get("session_id") != session_id:
                return False
            path.unlink()
            return True
    except FileNotFoundError:
        return False


def load_current_contract(jobs_folder):
    folder = _resolve_jobs_folder(jobs_folder)
    try:
        metadata = yaml.safe_load((folder / "jobs_config.yaml").read_text()) or {}
        submission = metadata["submission"]
        if submission["format_version"] != 1:
            raise ValueError
        if submission["executor"] not in ("condor@lxplus", "condor@rubin"):
            raise ValueError
        for key in ("requires_grid_certificate", "proxy_transfer_path", "proxy_source"):
            if key not in submission:
                raise ValueError
        state_file = folder / "job_state.json"
        state = json.loads(state_file.read_text())
        if not isinstance(state, dict) or not state:
            raise ValueError
        for name in ("job.sh", "inner_run_options.yaml", "resubmit.sub"):
            if not (folder / name).is_file():
                raise ValueError
        for job, values in state.items():
            if not (folder / f"config_job_{job}.pkl").is_file():
                raise ValueError
            if not all(key in values for key in
                       ("chunksize", "request_cpus", "request_memory", "resubmissions")):
                raise ValueError
            if submission["executor"] == "condor@lxplus" and not all(
                    key in values for key in ("queue", "resources_scaled")):
                raise ValueError
        return folder, metadata, submission, state, state_file
    except (FileNotFoundError, KeyError, ValueError, json.JSONDecodeError):
        raise click.UsageError(CONTRACT_ERROR)


def _with_check_jobs_lock(function):
    @wraps(function)
    def wrapped(*args, **kwargs):
        startup_progress = Progress(
            TextColumn("Launching check-jobs"),
            BarColumn(),
            TextColumn("[progress.description]{task.description}"),
            TextColumn("{task.completed}/{task.total}"),
        )
        startup_progress.start()
        startup_task = startup_progress.add_task(
            "Validating jobs contract", total=5 if kwargs.get("resubmit") else 4)
        startup_progress.refresh()
        lock = None
        try:
            folder = _resolve_jobs_folder(kwargs["jobs_folder"])
            needs_lock = kwargs.get("resubmit") or kwargs.get("recreate") is not None
            if needs_lock:
                startup_progress.update(startup_task, description="Acquiring check-jobs lock")
                startup_progress.refresh()
                lock = acquire_check_jobs_lock(folder, kwargs.get("ignore_lock", False))
                if lock is None:
                    return None
            contract = load_current_contract(folder)
            startup_progress.advance(startup_task)
            kwargs["jobs_folder"] = folder
            if not needs_lock:
                startup_progress.update(startup_task, description="Other startup steps")
                startup_progress.refresh()
            kwargs["_startup_progress"] = (startup_progress, startup_task)
            kwargs["_startup_contract"] = contract
            return function(*args, **kwargs)
        finally:
            if lock is not None:
                release_check_jobs_lock(folder, lock["session_id"])
            startup_progress.stop()
    return wrapped


def check_jobs_logs(jobs_folder):
    folder = Path(jobs_folder)
    return tuple([p.stem for p in folder.glob(f"job_*.{marker}")]
                  for marker in JOB_MARKERS)


_CONDOR_ABORT_RE = re.compile(r"^\s*009 \((\d+)\.(\d+)\.\d+\)\s+(\d\d/\d\d \d\d:\d\d:\d\d)")


def scan_condor_log_failures(jobs_folder, log_offsets):
    folder = Path(jobs_folder)
    active = {}
    for marker in ("idle", "running"):
        for path in folder.glob(f"job_*.{marker}"):
            job = path.stem
            if any((folder / f"{job}.{term}").exists() for term in ("done", "failed", "timeout")):
                continue
            try:
                active[job] = path.stat().st_mtime
            except FileNotFoundError:
                continue
    recovered = {}
    for path in (folder / "logs").glob("job_*.log"):
        key = str(path)
        offset = log_offsets.get(key, 0)
        if offset > path.stat().st_size:
            offset = 0
        with path.open(errors="replace") as handle:
            handle.seek(offset)
            while True:
                start = handle.tell()
                line = handle.readline()
                if not line:
                    break
                match = _CONDOR_ABORT_RE.match(line)
                if not match:
                    continue
                reason = handle.readline()
                if not reason:
                    handle.seek(start)
                    break
                parts = path.stem.split(".", 1)
                job = f"job_{parts[1]}" if len(parts) == 2 else f"job_{match.group(2)}"
                if job not in active:
                    continue
                year = time.localtime(active[job]).tm_year
                event = time.mktime(time.strptime(
                    f"{year}/{match.group(3)}", "%Y/%m/%d %H:%M:%S"))
                if event > active[job] and (job not in recovered or event > recovered[job][0]):
                    recovered[job] = (event, reason)
            log_offsets[key] = handle.tell()
    return {
        job: ("timeout" if "SYSTEM_PERIODIC_REMOVE" in reason and
              "wall time exceeded" in reason.lower() else "failed")
        for job, (_, reason) in recovered.items()
        if not any((folder / f"{job}.{term}").exists()
                   for term in ("done", "failed", "timeout"))
    }


def clear_job_markers(jobs_folder, job):
    folder = Path(jobs_folder)
    for marker in JOB_MARKERS:
        (folder / f"{job}.{marker}").unlink(missing_ok=True)


def apply_condor_log_failures(jobs_folder, findings):
    for job, marker in findings.items():
        if not any((Path(jobs_folder) / f"{job}.{term}").exists()
                   for term in ("done", "failed", "timeout")):
            clear_job_markers(jobs_folder, job)
            (Path(jobs_folder) / f"{job}.{marker}").touch()


def merge_inferred_status(idle, running, done, failed, timeout, findings):
    values = [list(value) for value in (idle, running, done, failed, timeout)]
    idle, running, done, failed, timeout = values
    for job, marker in findings.items():
        if job in done or job in failed or job in timeout:
            continue
        if job in idle:
            idle.remove(job)
        if job in running:
            running.remove(job)
        (timeout if marker == "timeout" else failed).append(job)
    return idle, running, done, failed, timeout


def get_tables(tot_jobs, idle_jobs, running_jobs, done_jobs, failed_jobs,
               timeout=None, details=False):
    failed_jobs = failed_jobs + [job for job in (timeout or []) if job not in failed_jobs]
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


def condor_rm_job(job):
    idx = job.split("_", 1)[1]
    try:
        result = subprocess.run(
            ["condor_rm", "-constraint", f'regexp("config_job_{idx}\\.pkl", Args)'],
            text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False)
    except OSError as exc:
        return False, str(exc)
    return result.returncode == 0, result.stdout.strip()


def wait_for_condor_job_removal(job, timeout=CONDOR_REMOVAL_TIMEOUT, poll_interval=0.2):
    idx = job.split("_", 1)[1]
    constraint = f'regexp("config_job_{idx}\\.pkl", Args)'
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            result = subprocess.run(
                ["condor_q", "-constraint", constraint, "-af", "ClusterId"],
                text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False)
            if result.returncode == 0 and not result.stdout.strip():
                return True
        except OSError:
            pass
        time.sleep(poll_interval)
    return False


def condor_submit_job(jobs_folder, submit_file):
    try:
        result = subprocess.run(
            ["condor_submit", submit_file], cwd=str(jobs_folder),
            text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False)
    except OSError as exc:
        return False, str(exc)
    return result.returncode == 0, result.stdout.strip()


def mark_job_idle(jobs_folder, job):
    clear_job_markers(jobs_folder, job)
    (Path(jobs_folder) / f"{job}.idle").touch()


def mark_job_failed(jobs_folder, job):
    clear_job_markers(jobs_folder, job)
    (Path(jobs_folder) / f"{job}.failed").touch()


def _write_state_temp(state_file, state):
    fd, name = tempfile.mkstemp(prefix=f".{Path(state_file).name}.", suffix=".tmp",
                                dir=Path(state_file).parent)
    os.close(fd)
    path = Path(name)
    path.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")
    return path


def _atomic_copy_proxy(source, target):
    source, target = os.path.abspath(source), os.path.abspath(target)
    if source == target:
        if not os.path.exists(target):
            raise RuntimeError(f"Required proxy transfer path does not exist: {target}")
        os.chmod(target, 0o600)
        return
    parent = os.path.dirname(target) or "."
    os.makedirs(parent, exist_ok=True)
    temp = os.path.join(parent, f".{os.path.basename(target)}.{os.getpid()}.tmp")
    try:
        with open(source, "rb") as src, open(temp, "wb") as dst:
            shutil.copyfileobj(src, dst)
        os.chmod(temp, 0o600)
        os.replace(temp, target)
        os.chmod(target, 0o600)
    finally:
        Path(temp).unlink(missing_ok=True)


def prepare_proxy_for_jobs(jobs_folder):
    submission = load_current_contract(jobs_folder)[2]
    if not submission["requires_grid_certificate"]:
        return None
    path = submission["proxy_transfer_path"]
    if not path:
        raise RuntimeError("The jobs require a grid certificate but no proxy transfer path was recorded")
    if submission["proxy_source"] == "default":
        _atomic_copy_proxy(get_proxy_path(), path)
    elif submission["proxy_source"] != "explicit":
        raise RuntimeError(f"Unsupported proxy source {submission['proxy_source']!r}")
    if not os.path.exists(path):
        raise RuntimeError(f"Required proxy transfer path does not exist: {path}")
    os.chmod(path, 0o600)
    os.environ["X509_USER_PROXY"] = path
    return path


def scale_memory(memory, factor):
    match = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([A-Za-z]*)\s*", str(memory))
    if not match:
        raise ValueError(f"Cannot scale RequestMemory value: {memory!r}")
    return f"{float(match.group(1)) * factor:g}{match.group(2)}"


def candidate_state(current, executor, queue_shift, ncpu):
    candidate = deepcopy(current)
    if executor != "condor@lxplus":
        return candidate
    candidate["queue"] = next_queue(candidate["queue"], queue_shift)
    if not candidate["resources_scaled"]:
        candidate["request_cpus"] = int(candidate["request_cpus"]) * ncpu
        candidate["request_memory"] = scale_memory(candidate["request_memory"], ncpu)
        candidate["resources_scaled"] = True
    return candidate


def render_states(folder, submission, states):
    rows = []
    for job, state in sorted(states.items(), key=lambda item: int(item[0])):
        row = {"PROC": job, "CHUNKSIZE": state["chunksize"],
               "CPUS": state["request_cpus"], "MEMORY": state["request_memory"]}
        if submission["executor"] == "condor@lxplus":
            row["QUEUE"] = state["queue"]
        rows.append(row)
    return render_condor_submit(
        (Path(folder) / "resubmit.sub").read_text(), rows, submission["executor"])


def is_xrootd_exhaustion_log(out_text):
    return any(marker in out_text for marker in (
        "XRootD failure found at root://xrootd-cms.infn.it",
        "Reached the maximum number of XRootD recovery attempts"))


def latest_job_out(jobs_folder, job):
    job_num = job.split("_", 1)[1]
    paths = glob.glob(f"{jobs_folder}/logs/job_*.{job_num}.out")
    return sorted(paths, key=os.path.getmtime)[-1] if paths else None


def convert_timeout_jobs(jobs_folder, timeout_jobs, running, idle, failed,
                         queue_shift, ncpu, state, log_text, pending=None):
    _, _, submission, _, _ = load_current_contract(jobs_folder)
    pending = pending if pending is not None else {}
    for job in list(timeout_jobs):
        if job in running:
            running.remove(job)
        if job in idle:
            idle.remove(job)
        mark_job_failed(jobs_folder, job)
        failed.append(job) if job not in failed else None
        job_num = job.split("_", 1)[1]
        pending[job] = candidate_state(state[job_num], submission["executor"], queue_shift, ncpu)
        log_text.append(
            f"{job} reached the Condor time limit; preparing "
            + ("queue/resource escalation." if submission["executor"] == "condor@lxplus"
               else "a retry."))
    return len(timeout_jobs)


def submit_resubmit_jobs(jobs_folder, job_nums, state, state_file, log_text, pending=None):
    folder, _, submission, _, _ = load_current_contract(jobs_folder)
    pending = pending or {}
    states = {job: deepcopy(pending.get(f"job_{job}", state[job])) for job in job_nums}
    updated_state = deepcopy(state)
    for job, candidate in states.items():
        committed = deepcopy(candidate)
        committed["resubmissions"] = int(state[job]["resubmissions"]) + 1
        updated_state[job] = committed
    (folder / "resubmit_now.sub").write_text(render_states(folder, submission, states))
    state_temp = None
    try:
        state_temp = _write_state_temp(state_file, updated_state)
        prepare_proxy_for_jobs(folder)
    except Exception as exc:
        if state_temp:
            state_temp.unlink(missing_ok=True)
        log_text.append(f"[red]Could not prepare proxy for resubmission: {exc}[/]")
        return []
    ok, output = condor_submit_job(folder, "resubmit_now.sub")
    if not ok:
        state_temp.unlink(missing_ok=True)
        log_text.append(f"[red]Failed to resubmit jobs; state unchanged: {output}[/]")
        return []
    try:
        os.replace(state_temp, state_file)
    except Exception as exc:
        raise RuntimeError(
            "Condor accepted the replacement, but job_state.json could not be "
            "committed. Do not rerun automatic recovery until the scheduler "
            "state is inspected."
        ) from exc
    state_temp = None
    state.clear()
    state.update(updated_state)
    for job in states:
        try:
            mark_job_idle(folder, f"job_{job}")
        except Exception as exc:
            raise RuntimeError(
                f"Condor accepted the replacement for job_{job}, but local marker "
                f"bookkeeping failed; manual inspection is required: {exc}"
            ) from exc
    log_text.append(f"[green]Resubmitted {len(states)} failed jobs to condor[/]")
    return list(states)


def recreate_jobs_oneshot(jobs_folder, jobs_to_recreate, *, use_redirector=False,
                          blocklist_sites=None, recreate_queue=None, skip_bad_files=False,
                          queue_shift=1, ncpu=1, remove_running=False):
    folder, jobs_config, submission, state, state_file = load_current_contract(jobs_folder)
    result = {"requested": [], "submitted": [], "skipped": [], "failed": {}}
    if recreate_queue is not None and submission["executor"] == "condor@rubin":
        raise click.UsageError("--recreate-queue is only supported for condor@lxplus")

    if jobs_to_recreate == "auto":
        jobs = []
        for marker in ("failed", "running", "idle", "timeout"):
            jobs.extend(p.stem for p in folder.glob(f"job_*.{marker}"))
        jobs = list(dict.fromkeys(jobs))
    else:
        jobs = list(dict.fromkeys(
            item.strip() if item.strip().startswith("job_") else f"job_{item.strip()}"
            for item in jobs_to_recreate.split(",") if item.strip()))
    explicit = jobs_to_recreate != "auto"
    result["requested"] = jobs
    if not jobs:
        return result

    eligible_jobs = []
    selected_active = set()
    for job in jobs:
        if job not in jobs_config["jobs_list"]:
            result["failed"][job] = "job is not present in jobs_config.yaml"
            continue
        active = (folder / f"{job}.running").exists() or (folder / f"{job}.idle").exists()
        if active:
            selected_active.add(job)
        if active and not remove_running:
            if explicit:
                result["failed"][job] = "job is active; pass --remove-running to recreate it"
            else:
                result["skipped"].append(job)
            continue
        eligible_jobs.append(job)
    jobs = eligible_jobs
    if not jobs:
        return result

    blocklist = {normalize_rse(site) for site in (blocklist_sites or [])}
    failed_xrootd = []
    for job in jobs:
        output = latest_job_out(folder, job)
        if output and extract_failed_url(Path(output).read_text(errors="replace")):
            failed_xrootd.append(job)
    sitemap = client = None
    if (failed_xrootd or blocklist) and not use_redirector:
        prepare_proxy_for_jobs(folder)
        sitemap = get_xrootd_sites_map()
        client = get_rucio_client()

    options_path = folder / "inner_run_options.yaml"
    options_original = None
    if skip_bad_files:
        options_original = options_path.read_bytes()
        options = yaml.safe_load(options_original) or {}
        options["skip-bad-files"] = True
        temp = options_path.with_name(f".{options_path.name}.{os.getpid()}.tmp")
        temp.write_text(yaml.safe_dump(options, sort_keys=False))
        os.replace(temp, options_path)

    for job in jobs:
        if job in selected_active and (folder / f"{job}.done").exists():
            result["skipped"].append(job)
            continue
        was_active = job in selected_active
        config_temp = sub_temp = None
        backup_path = None
        state_temp = None
        scheduler_instance_removed = False
        submitted_to_scheduler = False
        try:
            fileset = deepcopy(jobs_config["jobs_list"][job]["filesets"])
            if use_redirector:
                fileset = rewrite_fileset_to_redirector(fileset)
            else:
                if job in failed_xrootd:
                    for dataset in fileset.values():
                        dataset["files"] = [
                            find_other_file(file, sitemap, blocklist=blocklist, rucio_client=client)
                            for file in dataset["files"]]
                if blocklist:
                    fileset = rewrite_fileset_blocklist(fileset, sitemap, blocklist, rucio_client=client)
            config_path = folder / f"config_{job}.pkl"
            config = cloudpickle.load(config_path.open("rb"))
            config.set_filesets_manually(fileset)
            fd, name = tempfile.mkstemp(prefix=f".{config_path.name}.", suffix=".tmp", dir=folder)
            os.close(fd)
            config_temp = Path(name)
            with config_temp.open("wb") as handle:
                cloudpickle.dump(config, handle)
            job_num = job.split("_", 1)[1]
            candidate = deepcopy(state[job_num])
            if (folder / f"{job}.timeout").exists():
                candidate = candidate_state(candidate, submission["executor"], queue_shift, ncpu)
            if recreate_queue is not None:
                candidate["queue"] = recreate_queue
            row = {"PROC": job_num, "CHUNKSIZE": candidate["chunksize"],
                   "CPUS": candidate["request_cpus"], "MEMORY": candidate["request_memory"]}
            if submission["executor"] == "condor@lxplus":
                row["QUEUE"] = candidate["queue"]
            fd, name = tempfile.mkstemp(prefix=f".{job}.", suffix=".sub", dir=folder)
            os.close(fd)
            sub_temp = Path(name)
            sub_temp.write_text(render_condor_submit(
                (folder / "resubmit.sub").read_text(), [row], submission["executor"]))
            candidate["resubmissions"] = int(state[job_num]["resubmissions"]) + 1
            updated_state = deepcopy(state)
            updated_state[job_num] = candidate
            state_temp = _write_state_temp(state_file, updated_state)
            prepare_proxy_for_jobs(folder)
            if was_active and remove_running:
                if (folder / f"{job}.done").exists():
                    result["skipped"].append(job)
                    continue
                ok, output = condor_rm_job(job)
                if not ok:
                    if (folder / f"{job}.done").exists():
                        result["skipped"].append(job)
                        continue
                    raise RuntimeError(f"condor_rm failed: {output}")
                if not wait_for_condor_job_removal(job):
                    raise RuntimeError("could not confirm Condor removal")
                scheduler_instance_removed = True
                if (folder / f"{job}.done").exists():
                    result["skipped"].append(job)
                    continue

            backup_path = config_path.with_name(f".{config_path.name}.{os.getpid()}.bak")
            os.replace(config_path, backup_path)
            os.replace(config_temp, config_path)
            ok, output = condor_submit_job(folder, sub_temp.name)
            if not ok:
                config_path.unlink(missing_ok=True)
                os.replace(backup_path, config_path)
                backup_path = None
                if scheduler_instance_removed:
                    mark_job_failed(folder, job)
                result["failed"][job] = output or "condor_submit failed"
                continue
            submitted_to_scheduler = True
            try:
                os.replace(state_temp, state_file)
            except Exception as exc:
                raise RuntimeError(
                    "Condor accepted the replacement, but job_state.json could "
                    "not be committed. Do not rerun automatic recovery until "
                    "the scheduler state is inspected."
                ) from exc
            state_temp = None
            result["submitted"].append(job)
            state.clear()
            state.update(updated_state)
            try:
                backup_path.unlink(missing_ok=True)
                backup_path = None
            except Exception as exc:
                rprint(f"[yellow]{job} was submitted, but could not remove the config backup: {exc}[/]")
            try:
                mark_job_idle(folder, job)
            except Exception as exc:
                raise RuntimeError(
                    f"Condor accepted the replacement for {job}, but local marker "
                    f"bookkeeping failed; manual inspection is required: {exc}"
                ) from exc
        except Exception as exc:
            if not submitted_to_scheduler and backup_path and backup_path.exists():
                config_path.unlink(missing_ok=True)
                os.replace(backup_path, config_path)
            if submitted_to_scheduler:
                raise
            else:
                result["failed"][job] = str(exc)
            if scheduler_instance_removed:
                mark_job_failed(folder, job)
        finally:
            if config_temp:
                config_temp.unlink(missing_ok=True)
            if sub_temp:
                sub_temp.unlink(missing_ok=True)
            if state_temp and not submitted_to_scheduler:
                state_temp.unlink(missing_ok=True)

    if options_original is not None and not result["submitted"]:
        options_path.write_bytes(options_original)

    rprint("Recreate summary:")
    rprint(f"  submitted: {len(result['submitted'])}")
    rprint(f"  skipped: {len(result['skipped'])}")
    rprint(f"  failed: {len(result['failed'])}")
    for job, message in result["failed"].items():
        rprint(f"{job}: {message}")
    return result


@click.command()
@click.option("-j", "--jobs-folder", type=str, help="Folder containing the jobs", required=True)
@click.option("-d", "--details", is_flag=True, help="Show the details of the jobs")
@click.option("-r", "--resubmit", is_flag=True, help="Resubmit the failed jobs")
@click.option("-m", "--max-resubmit", type=click.IntRange(min=0),
              help="Maximum number of resubmission", default=4)
@click.option("-q", "--queue-shift", type=click.IntRange(min=0),
              help="How many queues to bump to if a job is removed due to time limit? "
                   "E.g. 1 = bump to next queue, 2 = bump to next-to-next queue", default=1)
@click.option("-n", "--ncpu", type=click.IntRange(min=1), default=1,
              help="Multiply CPU and memory requests by this factor on first LXPLUS escalation")
@click.option("--by", "group_by", type=click.Choice(["sample", "dataset", "none"]),
              default="sample",
              help="Show a per-group progress table below the summary. Requires "
                   "jobs_config.yaml in the jobs folder (created by manual-job "
                   "executors). Pass 'none' to disable. Default: sample.")
@click.option("--recreate", type=str, default=None, help="Recreate selected jobs")
@click.option("--once", is_flag=True, default=False, help="Run one monitoring pass")
@click.option("--use-redirector", is_flag=True, default=False,
              help="Use the global XRootD redirector when recreating")
@click.option("--blocklist-sites", type=str, default=None,
              help="Comma-separated CMS/Rucio sites to block when recreating")
@click.option("--recreate-queue", type=str, default=None,
              help="Queue to use for recreated lxplus jobs")
@click.option("--skip-bad-files", is_flag=True, default=False,
              help="Enable Coffea skip-bad-files in the shared inner worker options")
@click.option("--remove-running", is_flag=True, default=False,
              help="Remove active Condor jobs before recreation")
@click.option("--ignore-lock", is_flag=True, default=False,
              help="Ignore an existing check-jobs lock (risky)")
@_with_check_jobs_lock
def check_jobs(jobs_folder, details, resubmit, max_resubmit, queue_shift, ncpu, group_by,
               recreate, once, use_redirector, blocklist_sites, recreate_queue,
               skip_bad_files, remove_running, ignore_lock, _startup_progress=None,
               _startup_contract=None):
    if _startup_contract is None:
        folder, _, submission, state, state_file = load_current_contract(jobs_folder)
    else:
        folder, _, submission, state, state_file = _startup_contract
    startup_progress, startup_task = _startup_progress or (None, None)

    def show_startup_step(description):
        if startup_progress is not None:
            startup_progress.update(startup_task, description=description)
            startup_progress.refresh()

    def advance_startup_step():
        if startup_progress is not None:
            startup_progress.advance(startup_task)

    blocklist = (
        [site.strip() for site in blocklist_sites.split(",") if site.strip()]
        if blocklist_sites else []
    )
    invalid = [site for site in blocklist if site.startswith("root://")]
    if invalid:
        raise click.BadParameter(
            "--blocklist-sites accepts CMS/Rucio site names, not XRootD prefixes"
        )
    if recreate is None and any((use_redirector, blocklist_sites, recreate_queue,
                                 skip_bad_files, remove_running)):
        raise click.UsageError("recreate-only options require --recreate")
    if recreate is not None:
        show_startup_step("Other startup steps")
        result = recreate_jobs_oneshot(
            folder, recreate, use_redirector=use_redirector,
            blocklist_sites=blocklist,
            recreate_queue=recreate_queue, skip_bad_files=skip_bad_files,
            queue_shift=queue_shift, ncpu=ncpu, remove_running=remove_running)
        if result["failed"]:
            raise click.exceptions.Exit(1)
        if not resubmit:
            advance_startup_step()
            return
        folder, _, submission, state, state_file = load_current_contract(folder)
    else:
        show_startup_step("Other startup steps")
    advance_startup_step()

    total = [f"job_{job}" for job in state]
    groups = None
    label = None
    overlap = False
    if group_by != "none":
        show_startup_step("Loading progress groups")
        sample_jobs, dataset_jobs = load_job_to_group_map(folder)
        groups, label = (sample_jobs, "sample") if group_by == "sample" else (dataset_jobs, "dataset")
        if groups:
            listed = [job for jobs in groups.values() for job in jobs]
            overlap = len(listed) != len(set(listed))
        advance_startup_step()
    else:
        show_startup_step("Other startup steps")
        advance_startup_step()
    offsets = {}
    show_startup_step("Scanning job status")
    findings = scan_condor_log_failures(folder, offsets)
    idle, running, done, failed, timeout = check_jobs_logs(folder)
    advance_startup_step()
    log_text, pending, definitive = [], {}, set()

    mutation = bool(resubmit or recreate is not None)

    def prepare_status(idle, running, done, failed, timeout, findings):
        if mutation:
            apply_condor_log_failures(folder, findings)
            idle, running, done, failed, timeout = check_jobs_logs(folder)
            convert_timeout_jobs(folder, timeout, running, idle, failed, queue_shift, ncpu,
                                 state, log_text, pending)
        else:
            idle, running, done, failed, timeout = merge_inferred_status(
                idle, running, done, failed, timeout, findings)
        resubmit_now = []
        if resubmit:
            for job in failed:
                if job in definitive:
                    continue
                number = job.split("_", 1)[1]
                attempts = int(state[number]["resubmissions"])
                output = latest_job_out(folder, job)
                out_text = Path(output).read_text(errors="replace") if output else ""
                if "Corrupt input data" in out_text:
                    log_text.append(f"{job} reported corrupt input data in its .out log.")
                if attempts >= max_resubmit:
                    definitive.add(job)
                    continue
                if (attempts >= 1 and job not in timeout and job not in pending
                        and not is_xrootd_exhaustion_log(out_text)):
                    pending[job] = candidate_state(
                        state[number], submission["executor"], queue_shift, ncpu)
                resubmit_now.append(number)
        if resubmit_now:
            successful = submit_resubmit_jobs(folder, resubmit_now, state, state_file, log_text, pending)
            for number in successful:
                job = f"job_{number}"
                failed[:] = [value for value in failed if value != job]
                if job not in idle:
                    idle.append(job)
                pending.pop(job, None)
                definitive.discard(job)
        return idle, running, done, failed, timeout

    if resubmit:
        show_startup_step("Preparing resubmissions")
    idle, running, done, failed, timeout = prepare_status(
        idle, running, done, failed, timeout, findings)
    if resubmit:
        advance_startup_step()
    if startup_progress is not None:
        startup_progress.stop()

    layout = create_layout(with_progress=groups is not None)

    def refresh(idle, running, done, failed, timeout):
        summary, _ = get_tables(total, idle, running, done, failed, timeout, details)
        if groups is None:
            layout["left"].update(Panel(summary, title="Job Status"))
        else:
            layout["summary"].update(Panel(summary, title="Job Status"))
            layout["progress"].update(Panel(
                get_progress_table(aggregate_by_group(groups, idle, running, done, failed), label, overlap)))
        layout["right"].update(Panel("\n".join(log_text[-20:]) or "No logs yet", title="Log"))

    refresh(idle, running, done, failed, timeout)
    with Live(layout, refresh_per_second=0.2, console=Console()):
        first_pass = True
        while True:
            if not first_pass:
                findings = scan_condor_log_failures(folder, offsets)
                idle, running, done, failed, timeout = check_jobs_logs(folder)
                idle, running, done, failed, timeout = prepare_status(
                    idle, running, done, failed, timeout, findings)
                refresh(idle, running, done, failed, timeout)
            first_pass = False
            terminal_failed = len(definitive) if resubmit else len(failed)
            if len(total) == len(done) + terminal_failed:
                rprint("[green]All jobs are completed[/]")
                rprint(f"Now merge outputs with [yellow]merge-outputs -jc {folder}[/].")
                break
            if once:
                break
            time.sleep(5)


if __name__ == "__main__":
    check_jobs()
