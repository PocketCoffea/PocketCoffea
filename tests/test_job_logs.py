import os

from pocket_coffea.utils.job_logs import find_job_input_failure, latest_job_logs


def _write(path, text, mtime):
    path.write_text(text)
    os.utime(path, (mtime, mtime))


def test_stale_logs_ignored_until_fresh_ones_arrive(tmp_path):
    # Attempt 1 (cluster 100) failed on file A long ago; its logs are still there.
    _write(tmp_path / "job_100.7.out", "XRootD error ... in file root://siteA//store/a.root\n", 1000)
    _write(tmp_path / "job_100.7.err", "", 1000)
    flag_time = 2000  # attempt 2 touched job_7.failed now, .out/.err not transferred yet

    assert latest_job_logs(tmp_path, 7) != []  # old behaviour: stale logs picked up
    assert latest_job_logs(tmp_path, 7, newer_than=flag_time) == []
    assert find_job_input_failure(tmp_path, 7, newer_than=flag_time) == (None, None, [])

    # Attempt 2 (cluster 200) logs arrive after the flag.
    _write(tmp_path / "job_200.7.out", "XRootD error ... in file root://siteB//store/b.root\n", 2001)
    _write(tmp_path / "job_200.7.err", "", 2001)
    reason, fname, paths = find_job_input_failure(tmp_path, 7, newer_than=flag_time)
    assert reason == "XRootD error"
    assert fname == "root://siteB//store/b.root"
    assert len(paths) == 2

