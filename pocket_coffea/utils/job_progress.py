"""Pure helpers for the `check-jobs` per-sample / per-dataset progress
breakdown. Kept dependency-free (stdlib only) so the aggregator can be
unit-tested without importing the rucio / coffea stack that
`scripts/check_jobs.py` pulls in at module load."""
import os
import yaml
from collections import defaultdict


def load_job_to_group_map(jobs_folder):
    """Read `jobs_config.yaml` (written by the manual-job executors' prepare_jobs)
    and return two reverse-index dicts:

      - `sample_to_jobs`:  {sample_name: [job_N, ...]}
      - `dataset_to_jobs`: {dataset_name: [job_N, ...]}

    Returns ``(None, None)`` when `jobs_config.yaml` is not present.

    A single job that covers multiple samples/datasets (the uniform-split
    case) appears under every group it touches; callers should warn about
    the overlap when displaying totals."""
    yaml_path = os.path.join(str(jobs_folder), "jobs_config.yaml")
    if not os.path.isfile(yaml_path):
        return None, None
    with open(yaml_path) as f:
        cfg = yaml.safe_load(f)
    sample_to_jobs = defaultdict(list)
    dataset_to_jobs = defaultdict(list)
    for job_name, job in (cfg.get("jobs_list") or {}).items():
        for ds_name, ds_entry in (job.get("filesets") or {}).items():
            sample = (ds_entry.get("metadata") or {}).get("sample")
            dataset_to_jobs[ds_name].append(job_name)
            if sample is not None:
                sample_to_jobs[sample].append(job_name)
    return dict(sample_to_jobs), dict(dataset_to_jobs)


def render_progress_bar(counts, width=30):
    """Render a stacked single-line progress bar (rich markup) showing the
    relative shares of done / running / idle / failed for a group.

    Pure string output — no rich import needed at module-load time, so this
    is testable without pulling in rich. Widths are computed proportionally
    and the rounding error is absorbed by the segments with the largest
    fractional remainders, so the bar always fills exactly `width` columns.
    An empty group (total==0) returns a dim placeholder.

    Colour key:
      - green   = done
      - magenta = running
      - blue    = idle
      - red     = failed
    """
    total = counts["total"]
    if total == 0:
        return f"[dim]{'·' * width}[/]"

    segments = [
        ("green",   counts["done"]),
        ("magenta", counts["running"]),
        ("blue",    counts["idle"]),
        ("red",     counts["failed"]),
    ]
    raw = [(color, n, width * n / total) for color, n in segments if n > 0]
    if not raw:
        return f"[dim]{'·' * width}[/]"
    # Floor each segment, then distribute leftover cells (from rounding) to
    # the segments with the largest fractional remainders so we always sum
    # exactly to `width`.
    widths = [(color, n, int(frac), frac - int(frac)) for color, n, frac in raw]
    assigned = sum(w for _, _, w, _ in widths)
    leftover = width - assigned
    order = sorted(range(len(widths)), key=lambda i: widths[i][3], reverse=True)
    final = [list(w) for w in widths]
    for i in range(leftover):
        final[order[i % len(order)]][2] += 1
    parts = [f"[{color}]{'█' * w}[/]" for color, _, w, _ in final if w > 0]
    return "".join(parts)


def aggregate_by_group(group_to_jobs, idle_jobs, running_jobs, done_jobs, failed_jobs):
    """Roll the per-status job lists up into per-group counts.

    Parameters
    ----------
    group_to_jobs : dict[str, list[str]]
        Mapping group_name -> list of job names. A given job_name may
        appear under more than one group (uniform-split case); each
        occurrence is counted independently.
    idle_jobs, running_jobs, done_jobs, failed_jobs : iterable[str]

    Returns
    -------
    dict[str, dict]
        ``{group_name: {"total", "idle", "running", "done", "failed", "pct_done"}}``
        with ``pct_done`` as a float in [0, 100]. Empty groups (total==0)
        get pct_done = 0.0.
    """
    idle = set(idle_jobs)
    running = set(running_jobs)
    done = set(done_jobs)
    failed = set(failed_jobs)
    out = {}
    for group, jobs in group_to_jobs.items():
        jobs_set = set(jobs)
        n_idle = len(jobs_set & idle)
        n_running = len(jobs_set & running)
        n_done = len(jobs_set & done)
        n_failed = len(jobs_set & failed)
        total = len(jobs_set)
        pct_done = (100.0 * n_done / total) if total else 0.0
        out[group] = {
            "total": total,
            "idle": n_idle,
            "running": n_running,
            "done": n_done,
            "failed": n_failed,
            "pct_done": pct_done,
        }
    return out
