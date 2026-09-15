"""Unit tests for the held-job detection/handling helpers used by
`pocket-coffea check-jobs`.

The helpers live in `pocket_coffea/utils/job_holds.py` precisely so they can
be tested without pulling in rucio / coffea / rich. Log snippets follow the
real HTCondor user-log format from lxplus (memory holds are Code 34).
"""
import pytest

from pocket_coffea.utils.job_holds import (
    bump_memory,
    find_held_jobs,
    parse_hold_memory_mb,
    parse_request_memory_mb,
)

SUBMIT_HOST = ("<188.185.72.155:9618?addrs=188.185.72.155-9618"
               "&alias=bigbird08.cern.ch&noUDP&sock=schedd_2725_b3aa>")

MEM_HOLD_REASON = ("Error from slot1_17@b9g12p1875.cern.ch: Job has gone over "
                   "cgroup memory limit of 12000 megabytes. Last measured usage: "
                   "23884 megabytes.  Consider resubmitting with a higher request_memory.")


def _submit(cluster, proc):
    return (f"000 ({cluster}.{proc:03d}.000) 09/01 13:38:08 Job submitted from host: {SUBMIT_HOST}\n"
            "...\n")


def _execute(cluster, proc):
    return (f"001 ({cluster}.{proc:03d}.000) 09/01 14:00:00 Job executing on host: <137.138.0.1:9618>\n"
            "...\n")


def _hold(cluster, proc, reason=MEM_HOLD_REASON):
    return (f"012 ({cluster}.{proc:03d}.000) 09/01 14:20:52 Job was held.\n"
            f"\t{reason}\n"
            "\tCode 34 Subcode 102\n"
            "...\n")


def _release(cluster, proc):
    return (f"013 ({cluster}.{proc:03d}.000) 09/01 14:30:00 Job was released.\n"
            "...\n")


def _terminate(cluster, proc):
    return (f"005 ({cluster}.{proc:03d}.000) 09/01 15:00:00 Job terminated.\n"
            "\t(1) Normal termination (return value 0)\n"
            "...\n")


@pytest.fixture
def jobs_folder(tmp_path):
    (tmp_path / "logs").mkdir()
    return tmp_path


# ----------------------- find_held_jobs -----------------------

def test_find_held_jobs_og_log(jobs_folder):
    """Original cluster log: held jobs are keyed by ProcId; released or
    terminated jobs don't show up."""
    log = "".join([
        _submit(100, 0), _submit(100, 1), _submit(100, 2), _submit(100, 3),
        _execute(100, 0), _execute(100, 1), _execute(100, 2), _execute(100, 3),
        _hold(100, 1),                       # still held
        _hold(100, 2), _release(100, 2),     # held then released -> not held
        _terminate(100, 0),
    ])
    (jobs_folder / "logs" / "job_100.log").write_text(log)

    held = find_held_jobs(str(jobs_folder))
    assert list(held) == [("100", 1, 1)]
    reason, schedd = held[("100", 1, 1)]
    assert "memory limit of 12000" in reason
    assert schedd == "bigbird08.cern.ch"


def test_find_held_jobs_resubmitted_log(jobs_folder):
    """Per-job resubmission log job_<cluster>.<jobid>.log: the job number
    comes from the filename, not the ProcId (always 0)."""
    (jobs_folder / "logs" / "job_200.42.log").write_text(
        _submit(200, 0) + _execute(200, 0) + _hold(200, 0)
    )
    held = find_held_jobs(str(jobs_folder))
    assert list(held) == [("200", 0, 42)]


def test_find_held_jobs_ignores_healthy_logs(jobs_folder):
    (jobs_folder / "logs" / "job_300.log").write_text(
        _submit(300, 0) + _execute(300, 0) + _terminate(300, 0)
    )
    assert find_held_jobs(str(jobs_folder)) == {}


# ----------------------- parsers -----------------------

def test_parse_hold_memory_mb():
    assert parse_hold_memory_mb(MEM_HOLD_REASON) == (12000, 23884)
    assert parse_hold_memory_mb("some unrelated hold reason") == (None, None)


@pytest.mark.parametrize("value,expected", [
    ("2GB", 2048.0),
    ("4000MB", 4000.0),
    ("4000", 4000.0),
    ('"7.5GB"', 7680.0),
    ("12 GB", 12288.0),
    ("garbage", None),
])
def test_parse_request_memory_mb(value, expected):
    assert parse_request_memory_mb(value) == expected


# ----------------------- bump_memory -----------------------

SUB_TEMPLATE = """Executable = job.sh
+JobFlavour = "workday"
RequestCpus = 4
RequestMemory = 2GB
queue
"""


def test_bump_memory_from_hold_reason(tmp_path):
    """Real lxplus case: RequestMemory=2GB but the enforced (per-core) limit
    was 12000 MB with 23884 MB measured -> 1.2x measured, next GB up."""
    sub = tmp_path / "job_1052.sub"
    sub.write_text(SUB_TEMPLATE)
    assert bump_memory(str(sub), limit_mb=12000, measured_mb=23884) == 29000
    assert "RequestMemory = 29000MB\n" in sub.read_text()
    # the rest of the sub file is untouched
    assert '+JobFlavour = "workday"\n' in sub.read_text()


def test_bump_memory_doubles_without_hold_details(tmp_path):
    sub = tmp_path / "job_0.sub"
    sub.write_text(SUB_TEMPLATE.replace("2GB", "10000MB"))
    assert bump_memory(str(sub)) == 20000
    assert "RequestMemory = 20000MB\n" in sub.read_text()


def test_bump_memory_adds_missing_line_before_queue(tmp_path):
    sub = tmp_path / "job_0.sub"
    sub.write_text("Executable = job.sh\nqueue\n")
    assert bump_memory(str(sub), limit_mb=12000, measured_mb=23884) == 29000
    assert sub.read_text() == "Executable = job.sh\nRequestMemory = 29000MB\nqueue\n"


def test_bump_memory_nothing_to_do(tmp_path):
    sub = tmp_path / "job_0.sub"
    sub.write_text("Executable = job.sh\nqueue\n")
    assert bump_memory(str(sub)) is None
    assert sub.read_text() == "Executable = job.sh\nqueue\n"
