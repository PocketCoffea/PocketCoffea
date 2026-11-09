import os
import time

from pocket_coffea.scripts import check_jobs as cj

XROOTD_OUT = """Runner attempt 0
OSError: XRootD error: [ERROR] Operation expired
in file root://eoscms.cern.ch//eos/cms/store/mc/X/file.root

/usr/local/bin/python: No module named pocket_coffea.scripts.rewrite_xrootd_site
Job failed
Done
"""


def test_failure_reason_logged_once_and_waits_for_fresh_logs(tmp_path):
    (tmp_path / "logs").mkdir()
    flag = tmp_path / "job_7.failed"
    flag.touch()
    out = tmp_path / "logs" / "job_100.7.out"
    out.write_text(XROOTD_OUT)
    old = time.time() - 3600
    os.utime(out, (old, old))  # log of an older attempt

    reported, waiting, log = {}, {}, []
    assert cj.log_failure_reasons(tmp_path, ["job_7"], reported, waiting, log) == {"job_7"}
    assert log == []

    os.utime(out)  # log of this attempt arrives
    assert cj.log_failure_reasons(tmp_path, ["job_7"], reported, waiting, log) == set()
    assert len(log) == 1
    assert "XRootD error root://eoscms.cern.ch//eos/cms/store/mc/X/file.root" in log[0]
    assert "No module named pocket_coffea.scripts.rewrite_xrootd_site" in log[0]

    cj.log_failure_reasons(tmp_path, ["job_7"], reported, waiting, log)
    assert len(log) == 1  # same attempt not reported again


def test_timeout_job_does_not_wait_for_logs(tmp_path):
    # A job removed at the wall time limit sends back no logs: no 600 s wait.
    (tmp_path / "logs").mkdir()
    (tmp_path / "job_9.failed").touch()
    reported, waiting, log = {}, {}, []
    assert cj.log_failure_reasons(tmp_path, ["job_9"], reported, waiting, log,
                                  timeout=["job_9"]) == set()
    assert len(log) == 1 and "Condor wall time limit" in log[0]
    # without the timeout information the same job waits for its logs
    (tmp_path / "job_8.failed").touch()
    assert cj.log_failure_reasons(tmp_path, ["job_8"], reported, waiting, log) == {"job_8"}
