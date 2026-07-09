"""Offline tests for the one-shot `check-jobs --recreate` pass.

These exercise `pocket_coffea.scripts.check_jobs.recreate_jobs_oneshot` against a
synthetic jobs_dir (jobs_config.yaml + config_job_i.pkl + job_i.sub + flag files),
with all network / condor side effects stubbed:

- `get_xrootd_sites_map` and `get_rucio_client` are monkeypatched,
- `site_rewrite._query_replicas` returns a controllable LFN -> [sites] table,
- `dry_run=True` skips the real `condor_submit` (one test uses a fake os.system
  to check the submit/flag-flip commands instead).

Importing check_jobs pulls in pocket_coffea.utils.rucio (imports the rucio
package); stub it when unavailable so this stays offline.
"""
import sys
import types

import pytest
import yaml
import cloudpickle


def _ensure_rucio_importable():
    try:
        import rucio.client  # noqa: F401
        import rucio.common.client  # noqa: F401
        return
    except (ImportError, OSError):
        rucio = types.ModuleType("rucio")
        client = types.ModuleType("rucio.client")
        client.Client = object
        common = types.ModuleType("rucio.common")
        common_client = types.ModuleType("rucio.common.client")
        common_client.detect_client_location = lambda *a, **k: {}
        rucio.client = client
        rucio.common = common
        common.client = common_client
        sys.modules.setdefault("rucio", rucio)
        sys.modules.setdefault("rucio.client", client)
        sys.modules.setdefault("rucio.common", common)
        sys.modules.setdefault("rucio.common.client", common_client)


_ensure_rucio_importable()

from pocket_coffea.scripts import check_jobs  # noqa: E402
from pocket_coffea.utils import site_rewrite  # noqa: E402


SITEA = "root://siteA.example//"
SITEC = "root://siteC.example//"
SITEMAP = {"T2_X_SITEA": SITEA, "T2_X_SITEC": SITEC}
REDIR = site_rewrite.GLOBAL_XROOTD_REDIRECTOR


class FakeConfigurator:
    """Minimal stand-in for pocket_coffea Configurator (no coffea needed)."""

    def __init__(self, filesets):
        self.filesets = filesets

    def set_filesets_manually(self, filesets):
        self.filesets = filesets


def _fs(files):
    return {"datasetA": {"files": list(files), "metadata": {"sample": "A", "nevents": "1"}}}


PLACEHOLDER_FS = {"__placeholder__": {"files": [], "metadata": {}}}


def _make_jobs_dir(tmp_path, jobs):
    """Build a synthetic jobs_dir.

    `jobs` maps job_name -> {"filesets": <fileset>, "flag": failed|running|idle|None,
    "flavour": <+JobFlavour>}. The per-job pickle is seeded with a *placeholder*
    fileset (not the real one) so a passing assertion on the reloaded pickle proves
    the recreate pass sourced the fileset from jobs_config.yaml and persisted it.
    """
    d = tmp_path / "job"
    d.mkdir()
    (d / "logs").mkdir()
    jobs_list = {}
    for jn, spec in jobs.items():
        cfg = d / f"config_{jn}.pkl"
        with open(cfg, "wb") as f:
            cloudpickle.dump(FakeConfigurator(dict(PLACEHOLDER_FS)), f)
        jobs_list[jn] = {
            "filesets": spec["filesets"],
            "config_file": str(cfg),
            "output_file": str(d / f"output_{jn}.coffea"),
        }
        flavour = spec.get("flavour", "espresso")
        (d / f"{jn}.sub").write_text(
            "executable = job.sh\n"
            f'+JobFlavour="{flavour}"\n'
            f"transfer_input_files = {cfg}\n"
            "queue\n"
        )
        flag = spec.get("flag")
        if flag:
            (d / f"{jn}.{flag}").write_text("")
    (d / "jobs_config.yaml").write_text(yaml.safe_dump({
        "job_name": "job",
        "job_dir": str(d),
        "jobs_list": jobs_list,
    }))
    # job.sh with the `--chunksize $3` anchor the skip-bad-files patcher looks for
    (d / "job.sh").write_text(
        "pocket-coffea run --cfg $2 -o output --executor iterative --chunksize $3\n"
    )
    return d


def _load_files(cfg_path):
    with open(cfg_path, "rb") as f:
        return cloudpickle.load(f).filesets["datasetA"]["files"]


@pytest.fixture
def replicas(monkeypatch):
    """Stub the sitemap / rucio client / replica lookup. Returns the mutable
    LFN -> [site names] table used by find_other_file."""
    monkeypatch.setattr(check_jobs, "get_xrootd_sites_map", lambda: SITEMAP)
    monkeypatch.setattr(check_jobs, "get_rucio_client", lambda: None)
    table = {}

    def fake_query(lfn, client=None, scope="cms"):
        return list(table.get(lfn, []))

    monkeypatch.setattr(site_rewrite, "_query_replicas", fake_query)
    return table


def test_recreate_redirector_rewrites_all_files(tmp_path, replicas):
    f = SITEA + "/store/data/foo.root"
    d = _make_jobs_dir(tmp_path, {"job_0": {"filesets": _fs([f]), "flag": "failed"}})
    check_jobs.recreate_jobs_oneshot(d, "0", use_redirector=True, dry_run=True)
    assert _load_files(d / "config_job_0.pkl") == [REDIR + "store/data/foo.root"]


def test_recreate_queue_forced(tmp_path, replicas):
    f = SITEA + "/store/data/foo.root"
    d = _make_jobs_dir(tmp_path, {"job_0": {"filesets": _fs([f]), "flag": "failed", "flavour": "espresso"}})
    check_jobs.recreate_jobs_oneshot(d, "0", use_redirector=True,
                                     recreate_queue="longlunch", dry_run=True)
    assert '+JobFlavour="longlunch"' in (d / "job_0.sub").read_text()


def test_recreate_running_job_gets_queue_bump(tmp_path, replicas):
    f = SITEA + "/store/data/foo.root"
    d = _make_jobs_dir(tmp_path, {"job_0": {"filesets": _fs([f]), "flag": "running", "flavour": "espresso"}})
    check_jobs.recreate_jobs_oneshot(d, "auto", use_redirector=True, dry_run=True)
    # running job with no explicit --recreate-queue -> implicit one-step bump
    assert '+JobFlavour="microcentury"' in (d / "job_0.sub").read_text()


def test_recreate_blocklist_rewrites_to_alt_site(tmp_path, replicas):
    f = SITEA + "/store/data/foo.root"
    replicas["/store/data/foo.root"] = ["T2_X_SITEA", "T2_X_SITEC"]
    d = _make_jobs_dir(tmp_path, {"job_0": {"filesets": _fs([f]), "flag": "failed"}})
    check_jobs.recreate_jobs_oneshot(d, "0", blocklist_sites={"T2_X_SITEA"}, dry_run=True)
    assert _load_files(d / "config_job_0.pkl") == [SITEC + "/store/data/foo.root"]


def test_recreate_redirector_precedence_over_blocklist(tmp_path, replicas):
    # both set -> redirector wins, no Rucio lookup happens (empty replica table).
    f = SITEA + "/store/data/foo.root"
    d = _make_jobs_dir(tmp_path, {"job_0": {"filesets": _fs([f]), "flag": "failed"}})
    check_jobs.recreate_jobs_oneshot(d, "0", use_redirector=True,
                                     blocklist_sites={"T2_X_SITEA"}, dry_run=True)
    assert _load_files(d / "config_job_0.pkl") == [REDIR + "store/data/foo.root"]


def test_recreate_selector_only_touches_selected_jobs(tmp_path, replicas):
    fA = SITEA + "/store/data/a.root"
    fB = SITEA + "/store/data/b.root"
    d = _make_jobs_dir(tmp_path, {
        "job_0": {"filesets": _fs([fA]), "flag": "failed"},
        "job_1": {"filesets": _fs([fB]), "flag": "failed"},
    })
    check_jobs.recreate_jobs_oneshot(d, "0", use_redirector=True, dry_run=True)
    assert _load_files(d / "config_job_0.pkl") == [REDIR + "store/data/a.root"]
    # job_1 was not selected -> its pickle keeps the seeded placeholder fileset
    with open(d / "config_job_1.pkl", "rb") as fh:
        assert cloudpickle.load(fh).filesets == PLACEHOLDER_FS


def test_recreate_selector_accepts_job_prefix_and_bare_ids(tmp_path, replicas):
    fA = SITEA + "/store/data/a.root"
    fB = SITEA + "/store/data/b.root"
    d = _make_jobs_dir(tmp_path, {
        "job_0": {"filesets": _fs([fA]), "flag": "failed"},
        "job_1": {"filesets": _fs([fB]), "flag": "failed"},
    })
    check_jobs.recreate_jobs_oneshot(d, "0,job_1", use_redirector=True, dry_run=True)
    assert _load_files(d / "config_job_0.pkl") == [REDIR + "store/data/a.root"]
    assert _load_files(d / "config_job_1.pkl") == [REDIR + "store/data/b.root"]


def test_recreate_skip_bad_files_materializes_inner_yaml(tmp_path, replicas):
    pytest.importorskip("coffea")  # write_inner_run_options lives next to the executor
    f = SITEA + "/store/data/foo.root"
    d = _make_jobs_dir(tmp_path, {"job_0": {"filesets": _fs([f]), "flag": "failed"}})
    check_jobs.recreate_jobs_oneshot(d, "0", use_redirector=True,
                                     skip_bad_files=True, dry_run=True)
    inner = d / "inner_run_options.yaml"
    assert inner.exists()
    assert yaml.safe_load(inner.read_text()).get("skip-bad-files") is True
    assert "inner_run_options.yaml" in (d / "job.sh").read_text()
    assert "inner_run_options.yaml" in (d / "job_0.sub").read_text()


def test_recreate_dry_run_false_submits_and_flips_flags(tmp_path, replicas, monkeypatch):
    f = SITEA + "/store/data/foo.root"
    d = _make_jobs_dir(tmp_path, {"job_0": {"filesets": _fs([f]), "flag": "failed"}})
    calls = []
    monkeypatch.setattr(check_jobs.os, "system", lambda cmd: calls.append(cmd) or 0)
    check_jobs.recreate_jobs_oneshot(d, "0", use_redirector=True, dry_run=False)
    joined = "\n".join(calls)
    assert "condor_submit job_0.sub" in joined
    assert "touch" in joined and "job_0.idle" in joined
    assert "rm" in joined and "job_0.failed" in joined


def test_recreate_missing_jobs_config_is_graceful(tmp_path, replicas):
    d = tmp_path / "job"
    d.mkdir()
    # no jobs_config.yaml -> should return without raising
    check_jobs.recreate_jobs_oneshot(d, "0", use_redirector=True, dry_run=True)
