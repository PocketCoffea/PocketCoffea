"""Unit tests for the per-sample splitting in `prepare_splitting`.

Exercises the two static helpers `_split_uniform` and `_split_per_sample` of
`ExecutorFactoryManualABC` (`pocket_coffea/executors/executors_manual_jobs.py`)
directly so the test does not need an HTCondor environment or a grid proxy.
"""
from collections import OrderedDict
from collections.abc import Mapping

import pytest

from pocket_coffea.executors.executors_manual_jobs import ExecutorFactoryManualABC


class _NonDictMapping(Mapping):
    """A Mapping that is NOT a dict subclass — stands in for OmegaConf's
    DictConfig so we can exercise the dict-vs-Mapping isinstance check
    without needing omegaconf in the test environment."""

    def __init__(self, data):
        self._data = dict(data)

    def __getitem__(self, key):
        return self._data[key]

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)


def _make_fileset(sample, nevents, files):
    return {
        "files": list(files),
        "metadata": {"sample": sample, "nevents": str(nevents)},
    }


def _make_filesets(entries):
    """entries: list of (dataset_name, sample, nevents, [files...])"""
    out = OrderedDict()
    for ds_name, sample, nevents, files in entries:
        out[ds_name] = _make_fileset(sample, nevents, files)
    return out


# ----------------------- per-sample dict mode -----------------------

def test_per_sample_each_job_has_single_dataset():
    """In dict mode every output job must contain files from exactly one dataset."""
    filesets = _make_filesets([
        ("TT_2018", "TT", 1_000_000, [f"tt_{i}.root" for i in range(10)]),
        ("ttH_2018", "ttH", 800_000, [f"tth_{i}.root" for i in range(8)]),
    ])
    limits = {"TT": 200_000, "ttH": 400_000, "default": 1_000_000}

    jobs, _ = ExecutorFactoryManualABC._split_per_sample(filesets, limits)

    for job in jobs:
        assert len(job) == 1, f"Job mixes datasets: {list(job)}"


def test_per_sample_respects_limit():
    """A sample with limit L and N total events should produce ~ceil(N/L) jobs."""
    filesets = _make_filesets([
        ("TT_2018", "TT", 1_000_000, [f"tt_{i}.root" for i in range(10)]),
        ("ttH_2018", "ttH", 800_000, [f"tth_{i}.root" for i in range(8)]),
    ])
    limits = {"TT": 200_000, "ttH": 400_000}

    jobs, _ = ExecutorFactoryManualABC._split_per_sample(filesets, limits)
    by_ds = {}
    for j in jobs:
        ds_name = next(iter(j))
        by_ds.setdefault(ds_name, []).append(j)

    # TT: 1M events / 200k limit, 100k per file (10 files) -> trigger every 2 files -> 5 jobs of 2 files
    assert len(by_ds["TT_2018"]) == 5
    assert all(len(j["TT_2018"]["files"]) == 2 for j in by_ds["TT_2018"])

    # ttH: 800k / 400k limit, 100k per file (8 files) -> trigger every 4 files -> 2 jobs of 4 files
    assert len(by_ds["ttH_2018"]) == 2
    assert all(len(j["ttH_2018"]["files"]) == 4 for j in by_ds["ttH_2018"])


def test_per_sample_default_fallback_used():
    """Samples not listed explicitly should fall back to 'default'."""
    filesets = _make_filesets([
        ("OtherSample_2018", "OTHER", 600_000, [f"o_{i}.root" for i in range(6)]),
    ])
    limits = {"TT": 200_000, "default": 300_000}

    jobs, _ = ExecutorFactoryManualABC._split_per_sample(filesets, limits)

    # 600k events / 300k limit, 100k per file (6 files) -> trigger every 3 files -> 2 jobs
    assert len(jobs) == 2
    assert all(len(j["OtherSample_2018"]["files"]) == 3 for j in jobs)


def test_per_sample_missing_sample_and_no_default_raises():
    filesets = _make_filesets([
        ("UnknownSample_2018", "UNKNOWN", 100_000, ["u_0.root"]),
    ])
    limits = {"TT": 200_000}   # no default and UNKNOWN absent

    with pytest.raises(Exception, match="no 'default' fallback"):
        ExecutorFactoryManualABC._split_per_sample(filesets, limits)


def test_per_sample_preserves_order():
    """Dataset order and file order within each dataset must be preserved."""
    filesets = _make_filesets([
        ("A_2018", "A", 400_000, ["a_0.root", "a_1.root", "a_2.root", "a_3.root"]),
        ("B_2018", "B", 200_000, ["b_0.root", "b_1.root"]),
    ])
    limits = {"A": 200_000, "B": 200_000}

    jobs, _ = ExecutorFactoryManualABC._split_per_sample(filesets, limits)

    # Flatten and verify everything appears in original order.
    seen_files = [f for job in jobs for ds in job.values() for f in ds["files"]]
    assert seen_files == [
        "a_0.root", "a_1.root", "a_2.root", "a_3.root",
        "b_0.root", "b_1.root",
    ]


# ----------------------- uniform (scalar) mode regression -----------------------

def test_uniform_mode_unchanged():
    """Scalar mode keeps the legacy behaviour: jobs can mix datasets."""
    filesets = _make_filesets([
        ("A_2018", "A", 300_000, ["a_0.root", "a_1.root", "a_2.root"]),
        ("B_2018", "B", 300_000, ["b_0.root", "b_1.root", "b_2.root"]),
    ])

    jobs, _ = ExecutorFactoryManualABC._split_uniform(filesets, max_events_per_job=200_000)

    # 100k per file across 6 files, threshold 200k -> trigger every 2 files
    assert len(jobs) == 3
    # The middle job is allowed to straddle dataset A and B.
    assert any(len(j) == 2 for j in jobs), "expected at least one job mixing both datasets in uniform mode"


# ----------------------- per-sample chunksize resolver -----------------------

def _make_single_sample_job(sample):
    """A 'split' (one job entry) covering one dataset of `sample`."""
    return {f"{sample}_ds": {"files": ["f.root"], "metadata": {"sample": sample, "nevents": "10"}}}


def test_chunksize_scalar_passthrough():
    assert ExecutorFactoryManualABC._resolve_chunksize_for_job(150_000, _make_single_sample_job("TT")) == 150_000
    # string from CLI pass-through is coerced to int
    assert ExecutorFactoryManualABC._resolve_chunksize_for_job("80000", _make_single_sample_job("TT")) == 80_000


def test_chunksize_dict_picks_per_sample():
    cfg = {"TT": 50_000, "ttH": 80_000, "default": 150_000}
    assert ExecutorFactoryManualABC._resolve_chunksize_for_job(cfg, _make_single_sample_job("TT")) == 50_000
    assert ExecutorFactoryManualABC._resolve_chunksize_for_job(cfg, _make_single_sample_job("ttH")) == 80_000


def test_chunksize_dict_falls_back_to_default():
    cfg = {"TT": 50_000, "default": 150_000}
    assert ExecutorFactoryManualABC._resolve_chunksize_for_job(cfg, _make_single_sample_job("Other")) == 150_000


def test_chunksize_dict_missing_sample_no_default_raises():
    cfg = {"TT": 50_000}
    with pytest.raises(Exception, match="no 'default' fallback"):
        ExecutorFactoryManualABC._resolve_chunksize_for_job(cfg, _make_single_sample_job("ttH"))


def test_chunksize_dict_rejects_mixed_sample_job():
    cfg = {"TT": 50_000, "ttH": 80_000, "default": 150_000}
    mixed_split = {
        "TT_2018": {"files": ["t.root"], "metadata": {"sample": "TT", "nevents": "10"}},
        "ttH_2018": {"files": ["h.root"], "metadata": {"sample": "ttH", "nevents": "10"}},
    }
    with pytest.raises(Exception, match="multiple samples"):
        ExecutorFactoryManualABC._resolve_chunksize_for_job(cfg, mixed_split)


def test_chunksize_validate_keys_warns_on_unknown(capsys):
    filesets = _make_filesets([("TT_2018", "TT", 10, ["f.root"])])
    cfg = {"TT": 50_000, "Typo": 99_999, "default": 150_000}
    ExecutorFactoryManualABC._validate_chunksize_keys(cfg, filesets)
    captured = capsys.readouterr().out
    assert "Typo" in captured
    assert "WARNING" in captured


# ----- regression: OmegaConf DictConfig-like Mapping must be accepted -----

def test_chunksize_resolver_accepts_non_dict_mapping():
    """OmegaConf's DictConfig (delivered by --custom-run-options YAML) is a
    Mapping but not a dict; the resolver must accept any Mapping."""
    cfg = _NonDictMapping({"TT": 50_000, "default": 150_000})
    assert ExecutorFactoryManualABC._resolve_chunksize_for_job(
        cfg, _make_single_sample_job("TT")
    ) == 50_000
    assert ExecutorFactoryManualABC._resolve_chunksize_for_job(
        cfg, _make_single_sample_job("Other")
    ) == 150_000


def test_per_sample_split_accepts_non_dict_mapping():
    """Same regression for max-events-per-job: _split_per_sample must accept
    any Mapping, not only plain dict."""
    filesets = _make_filesets([
        ("TT_2018", "TT", 1_000_000, [f"tt_{i}.root" for i in range(10)]),
    ])
    limits = _NonDictMapping({"TT": 200_000, "default": 500_000})

    jobs, _ = ExecutorFactoryManualABC._split_per_sample(filesets, limits)
    assert len(jobs) == 5
    assert all(len(j["TT_2018"]["files"]) == 2 for j in jobs)


def test_adaptive_targets_give_fast_samples_fewer_jobs_and_slow_samples_more_jobs():
    filesets = _make_filesets([
        ("fast_ds", "same", 1_000, [f"fast_{i}.root" for i in range(10)]),
        ("slow_ds", "same", 1_000, [f"slow_{i}.root" for i in range(10)]),
    ])

    targets = ExecutorFactoryManualABC._adaptive_job_targets(
        filesets, {"fast_ds": 100.0, "slow_ds": 10.0}, scaleout=11
    )

    assert targets == {"fast_ds": 1, "slow_ds": 10}


def test_adaptive_limits_use_median_for_unknown_samples():
    filesets = _make_filesets([
        ("known_ds", "same", 100, [f"known_{i}.root" for i in range(10)]),
        ("new_ds", "same", 100, [f"new_{i}.root" for i in range(10)]),
    ])

    targets = ExecutorFactoryManualABC._adaptive_job_targets(
        filesets, {"known_ds": 20.0, "other": 10.0}, scaleout=2
    )

    assert targets == {"known_ds": 1, "new_ds": 2}


def test_adaptive_dataset_limits_split_each_dataset_independently():
    filesets = _make_filesets([
        ("fast_ds", "same", 1_000, [f"fast_{i}.root" for i in range(10)]),
        ("slow_ds", "same", 1_000, [f"slow_{i}.root" for i in range(10)]),
    ])
    targets = {"fast_ds": 2, "slow_ds": 4}

    jobs, _ = ExecutorFactoryManualABC._split_per_dataset(filesets, targets)

    assert [next(iter(job)) for job in jobs] == [
        "fast_ds", "fast_ds", "slow_ds", "slow_ds", "slow_ds", "slow_ds"
    ]


def test_adaptive_target_can_exceed_global_scaleout():
    filesets = _make_filesets([
        (f"ds_{i}", f"sample_{i}", 1_000, [f"file_{i}_{j}.root" for j in range(4)])
        for i in range(3)
    ])
    targets = ExecutorFactoryManualABC._adaptive_job_targets(
        filesets, {name: 10.0 for name in filesets}, scaleout=2
    )

    assert targets == {name: 1 for name in filesets}
    jobs, _ = ExecutorFactoryManualABC._split_per_dataset(filesets, targets)
    assert len(jobs) == 3


def test_adaptive_target_is_clamped_to_available_files():
    filesets = _make_filesets([
        ("small_ds", "small", 1_000, [f"small_{i}.root" for i in range(3)]),
    ])
    targets = ExecutorFactoryManualABC._adaptive_job_targets(
        filesets, {"small_ds": 1.0}, scaleout=100
    )

    assert targets == {"small_ds": 3}


def test_per_dataset_split_preserves_files_and_balances_group_sizes():
    files = [f"file_{i}.root" for i in range(5)]
    filesets = _make_filesets([("dataset", "sample", 500, files)])

    jobs, _ = ExecutorFactoryManualABC._split_per_dataset(filesets, {"dataset": 2})
    groups = [job["dataset"]["files"] for job in jobs]

    assert [file for group in groups for file in group] == files
    assert sorted(map(len, groups)) == [2, 3]


def test_prepare_adaptive_splitting_uses_exact_dataset_keys(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "pocket_coffea.executors.executors_manual_jobs.load_sample_throughputs",
        lambda _: {"fast_ds": 100.0, "slow_ds": 10.0},
    )
    factory = _ManualFactory(_manual_options(
        tmp_path, scaleout=11, **{"_timeit-dir": str(tmp_path / "timeit")}
    ), str(tmp_path))
    filesets = _make_filesets([
        ("fast_ds", "same", 1_000, [f"fast_{i}.root" for i in range(10)]),
        ("slow_ds", "same", 1_000, [f"slow_{i}.root" for i in range(10)]),
    ])

    jobs = factory.prepare_splitting(filesets)

    assert [len(job[next(iter(job))]["files"]) for job in jobs] == [10] + [1] * 10


def test_timeit_keeps_one_job_per_dataset_and_all_selected_files(tmp_path):
    factory = _ManualFactory(_manual_options(
        tmp_path, timeit=True, scaleout=99, **{"_timeit-dir": str(tmp_path / "timeit")}
    ), str(tmp_path))
    filesets = _make_filesets([
        ("fast_ds", "same", 1_000, ["fast_0.root", "fast_1.root"]),
        ("slow_ds", "same", 2_000, ["slow_0.root", "slow_1.root", "slow_2.root"]),
    ])

    jobs = factory.prepare_splitting(filesets)

    assert len(jobs) == 2
    assert [next(iter(job)) for job in jobs] == ["fast_ds", "slow_ds"]
    assert [len(job[dataset]["files"]) for job, dataset in zip(jobs, ("fast_ds", "slow_ds"))] == [2, 3]


def test_validate_chunksize_keys_accepts_non_dict_mapping(capsys):
    filesets = _make_filesets([("TT_2018", "TT", 10, ["f.root"])])
    cfg = _NonDictMapping({"TT": 50_000, "Typo": 99_999, "default": 150_000})
    ExecutorFactoryManualABC._validate_chunksize_keys(cfg, filesets)
    captured = capsys.readouterr().out
    assert "Typo" in captured


def test_chunksize_mapping_prefers_exact_dataset_key():
    job = _make_single_sample_job("TT")
    dataset = next(iter(job))
    assert ExecutorFactoryManualABC._resolve_chunksize_for_job(
        {dataset: 25_000, "TT": 50_000, "default": 150_000}, job
    ) == 25_000


class _ManualFactory(ExecutorFactoryManualABC):
    def get(self):
        return None

    def prepare_jobs(self, splits):
        raise AssertionError("recreation must not split jobs")

    def submit_jobs(self, jobs):
        raise AssertionError("recreation must not submit through the normal path")

def _manual_options(tmp_path, **extra):
    options = {
        "ignore-grid-certificate": True,
        "jobs-dir": str(tmp_path),
        "job-name": "job",
    }
    options.update(extra)
    return options


def test_manual_factory_rejects_existing_directory_for_new_submission(tmp_path):
    (tmp_path / "job").mkdir()
    with pytest.raises(SystemExit):
        _ManualFactory(_manual_options(tmp_path), str(tmp_path))
