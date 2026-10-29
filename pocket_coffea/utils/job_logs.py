"""Parse HTCondor job logs (.out / .err) of manual-job executors to find
jobs that failed because of an input file (XRootD error, missing file,
corrupt data) and which file it was.

Kept dependency-free so it can be unit-tested without rucio / coffea / rich.
"""
import os
import re

# (reason label, substring to look for — matched case-insensitively)
INPUT_FAILURE_PATTERNS = (
    ("XRootD error", "XRootD error"),
    ("File not found", "FileNotFoundError: file not found"),
    ("Corrupt input data", "Corrupt input data"),
)

_LOG_NAME_RE = re.compile(r"^job_(\d+)\.(\d+)\.(out|err)$")

# Ways the failing file shows up in a log, most specific first:
#  - coffea wraps every processing error in
#      Exception: Failed processing file: WorkItem(dataset=..., filename='root://...', ...)
#  - uproot XRootD errors end with "in file root://..."
#  - uproot FileNotFoundError prints the quoted URL a few lines below
_FILE_RES = (
    re.compile(r"filename\s*=\s*['\"]([^'\"]+)['\"]"),
    re.compile(r"in file\s+(root://\S+)"),
    re.compile(r"['\"](root://[^'\"]+)['\"]"),
)


def latest_job_logs(logs_dir, job_num):
    """Return the .out/.err paths of the most recent HTCondor cluster that
    ran job `job_num` (log names: job_<ClusterId>.<ProcId>.{out,err}).
    Only the top level of `logs_dir` is scanned, so archived logs are ignored."""
    job_num = str(job_num)
    by_cluster = {}
    if not os.path.isdir(logs_dir):
        return []
    for fname in os.listdir(logs_dir):
        m = _LOG_NAME_RE.match(fname)
        if m and m.group(2) == job_num:
            by_cluster.setdefault(int(m.group(1)), []).append(os.path.join(logs_dir, fname))
    if not by_cluster:
        return []
    return sorted(by_cluster[max(by_cluster)])


def _clean(url):
    return url.strip().strip("'\"").rstrip(",)")


def parse_input_failure(lines):
    """Look for an input-file failure in the log `lines`.

    Returns ``(reason, filename)``; ``(None, None)`` if no known pattern is found,
    ``(reason, None)`` if the error is found but no file could be extracted.
    """
    reason = None
    hit = None
    for i, ln in enumerate(lines):
        low = ln.lower()
        for label, pat in INPUT_FAILURE_PATTERNS:
            if pat.lower() in low:
                reason, hit = label, i
                break
        if reason:
            break
    if reason is None:
        return None, None

    # 1) look right after the error line (formats used by uproot)
    window = "".join(lines[hit:hit + 6])
    for rx in _FILE_RES[1:]:
        m = rx.search(window)
        if m:
            return reason, _clean(m.group(1))
    # 2) anywhere in the log: coffea WorkItem first, then any quoted / "in file" URL.
    #    Take the last occurrence, i.e. the one closest to the final error.
    text = "".join(lines)
    for rx in _FILE_RES:
        found = rx.findall(text)
        if found:
            return reason, _clean(found[-1])
    return reason, None


def find_job_input_failure(logs_dir, job_num):
    """Check the latest .out AND .err of a job.

    Returns ``(reason, filename, log_paths)``; reason is None if the job did
    not fail because of an input file. ``log_paths`` are the logs read.
    """
    paths = latest_job_logs(logs_dir, job_num)
    reason, filename = None, None
    for p in sorted(paths, key=lambda p: not p.endswith(".err")):  # .err first
        try:
            with open(p, errors="replace") as f:
                r, fl = parse_input_failure(f.readlines())
        except OSError:
            continue
        if r and (reason is None or (filename is None and fl)):
            reason, filename = r, fl
        if reason and filename:
            break
    return reason, filename, paths


def last_lines(paths, n=3):
    """Last `n` non-empty lines of the given logs (.err preferred), for display."""
    for p in sorted(paths, key=lambda p: not p.endswith(".err")):
        try:
            with open(p, errors="replace") as f:
                lines = [l for l in f.readlines() if l.strip()]
        except OSError:
            continue
        if lines:
            return "".join(lines[-n:])
    return ""
