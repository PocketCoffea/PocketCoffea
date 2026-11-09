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
    assert len(log) == 1 and "waiting up to" in log[0]

    os.utime(out)  # log of this attempt arrives
    assert cj.log_failure_reasons(tmp_path, ["job_7"], reported, waiting, log) == set()
    assert len(log) == 2
    assert "XRootD error root://eoscms.cern.ch//eos/cms/store/mc/X/file.root" in log[1]
    assert "No module named pocket_coffea.scripts.rewrite_xrootd_site" in log[1]

    cj.log_failure_reasons(tmp_path, ["job_7"], reported, waiting, log)
    assert len(log) == 2  # same attempt not reported again

