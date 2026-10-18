import click
import pytest

from pocket_coffea.executors.executors_lxplus import ExecutorFactoryCondorCERN
from pocket_coffea.executors.executors_manual_jobs import ExecutorFactoryManualABC
from pocket_coffea.utils.htcondor_queue import next_queue, queue_for_runtime


def _filesets():
    return {
        "fast": {
            "files": ["f0", "f1", "f2", "f3"],
            "metadata": {"nevents": 4000, "sample": "same"},
        },
        "slow": {
            "files": ["s0", "s1", "s2"],
            "metadata": {"nevents": 3000, "sample": "same"},
        },
        "missing": {
            "files": ["m0"],
            "metadata": {"nevents": 1000, "sample": "same"},
        },
    }


def _factory(tmp_path, queue="workday"):
    factory = object.__new__(ExecutorFactoryCondorCERN)
    factory.run_options = {
        "queue": queue,
        "_timeit-dir": str(tmp_path / "timeit"),
        "cores-per-worker": 1,
    }
    return factory


def _print_fixed(factory, filesets, jobs, throughputs):
    factory._print_runtime_forecast(
        filesets,
        jobs,
        throughputs,
        [factory.run_options["queue"]] * len(jobs),
        throughputs,
        None,
        False,
    )


@pytest.mark.parametrize("create_dir", [False, True])
def test_no_timeit_data_does_not_print_forecast(monkeypatch, tmp_path, capsys, create_dir):
    jobs = [{"fast": _filesets()["fast"]}]
    monkeypatch.setattr(
        ExecutorFactoryManualABC,
        "prepare_splitting",
        lambda self, filesets: jobs,
    )
    if create_dir:
        (tmp_path / "timeit").mkdir()

    assert _factory(tmp_path).prepare_splitting(_filesets()) == jobs
    assert "Expected max time per job" not in capsys.readouterr().out


def test_forecast_uses_max_files_and_sorts_missing_last(tmp_path, capsys):
    filesets = _filesets()
    jobs = [
        {"fast": {"files": ["f0", "f1"], "metadata": filesets["fast"]["metadata"]}},
        {"fast": {"files": ["f2", "f3"], "metadata": filesets["fast"]["metadata"]}},
        {"slow": {"files": ["s0", "s1"], "metadata": filesets["slow"]["metadata"]}},
        {"slow": {"files": ["s2"], "metadata": filesets["slow"]["metadata"]}},
    ]

    _print_fixed(_factory(tmp_path), filesets, jobs, {"fast": 1.0, "slow": 0.5})
    output = capsys.readouterr().out
    assert output.index("fast") < output.index("slow") < output.index("missing")
    assert "0h33m" in output
    assert "1h07m" in output
    assert "Missing" in output


def test_fixed_forecast_sums_mixed_dataset_job(monkeypatch, tmp_path, capsys):
    factory = _factory(tmp_path)
    filesets = {
        "A": {"files": ["a"], "metadata": {"nevents": 12_600}},
        "B": {"files": ["b"], "metadata": {"nevents": 12_600}},
    }
    monkeypatch.setattr(click, "confirm", lambda *args, **kwargs: True)

    _print_fixed(
        factory,
        filesets,
        [{"A": filesets["A"], "B": filesets["B"]}],
        {"A": 1.0, "B": 1.0},
    )

    output = capsys.readouterr().out
    assert "7h00m" in output
    assert "87.5%" in output


def test_unknown_queue_fails_with_timing_data(tmp_path):
    factory = _factory(tmp_path, queue="unknown")
    filesets = {"dataset": {"files": ["f"], "metadata": {"nevents": 1, "sample": "s"}}}
    try:
        _print_fixed(factory, filesets, [{"dataset": filesets["dataset"]}], {"dataset": 1.0})
    except click.ClickException as exc:
        assert "unknown LXPLUS queue" in str(exc)
    else:
        raise AssertionError("unknown queues must fail when timing data is present")


@pytest.mark.parametrize(
    "seconds, expected",
    [
        (8 * 3600 * 0.8 - 1, "workday"),
        (8 * 3600 * 0.8, "tomorrow"),
        (7 * 24 * 3600, "nextweek"),
    ],
)
def test_queue_selection_uses_strict_threshold(seconds, expected):
    assert queue_for_runtime(seconds, 80) == expected


def test_auto_forecast_uses_median_for_missing_and_shows_queue(monkeypatch, tmp_path, capsys):
    factory = _factory(tmp_path, queue="auto")
    factory.run_options["queue_time_threshold_percent"] = 80
    filesets = _filesets()
    jobs = [{"fast": filesets["fast"]}, {"missing": filesets["missing"]}]
    factory._print_runtime_forecast(
        filesets,
        jobs,
        {"fast": 1.0},
        ["microcentury", "microcentury"],
        {"fast": 1.0, "slow": 1.0, "missing": 1.0},
        [4000.0, 1000.0],
        True,
    )
    output = capsys.readouterr().out
    assert "Queue" in output
    assert "% of queue time" not in output
    assert "microcentury" in output


def test_auto_rejects_empty_timing(monkeypatch, tmp_path):
    jobs = [{"fast": _filesets()["fast"]}]
    monkeypatch.setattr(
        ExecutorFactoryManualABC,
        "prepare_splitting",
        lambda self, filesets: jobs,
    )
    factory = _factory(tmp_path, queue="auto")
    with pytest.raises(click.ClickException, match="requires at least one"):
        factory.prepare_splitting(_filesets())


def test_auto_mixed_dataset_runtime_is_summed(tmp_path):
    factory = _factory(tmp_path, queue="auto")
    filesets = {
        "fast": {"files": ["f"], "metadata": {"nevents": 1000}},
        "slow": {"files": ["s"], "metadata": {"nevents": 1000}},
    }
    jobs = [{"fast": filesets["fast"], "slow": filesets["slow"]}]
    assert factory._job_runtime_seconds(filesets, jobs, {"fast": 1.0, "slow": 2.0}) == [1500.0]


def test_queue_shift_zero_is_a_noop_and_unknown_queues_bump_to_terminal():
    assert next_queue("espresso", 0) == "espresso"
    assert next_queue("site-specific", 0) == "site-specific"
    assert next_queue("site-specific", 1) == "nextweek"


def test_runtime_forecast_scales_per_worker_throughput(tmp_path):
    filesets = {"dataset": {"files": ["f"], "metadata": {"nevents": 1000}}}
    jobs = [{"dataset": filesets["dataset"]}]
    factory = _factory(tmp_path)
    one_worker = factory._job_runtime_seconds(filesets, jobs, {"dataset": 100.0})
    factory.run_options["cores-per-worker"] = 4
    four_workers = factory._job_runtime_seconds(filesets, jobs, {"dataset": 100.0})
    assert one_worker == [10.0]
    assert four_workers == [2.5]
