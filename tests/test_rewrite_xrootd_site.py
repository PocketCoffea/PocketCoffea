import json
from collections import OrderedDict

from pocket_coffea.scripts import rewrite_xrootd_site as helper


SITEA_PREFIX = "root://siteA.example//"
SITEB_PREFIX = "root://siteB.example//"
SITEC_PREFIX = "root://siteC.example//"


def _fileset(files_by_sample):
    fs = OrderedDict()
    for sample, files in files_by_sample:
        fs[sample] = {"files": list(files), "metadata": {"nevents": "1"}}
    return fs


def _dump_config(tmp_path, filesets):
    config_path = tmp_path / "fileset_job_0.json"
    config_path.write_text(json.dumps(filesets))
    return config_path


def _load_filesets(config_path):
    return json.loads(config_path.read_text())


def _patch_rewrite_deps(monkeypatch):
    monkeypatch.setattr(helper, "get_xrootd_sites_map", lambda: {
        "T2_X_SITEA": SITEA_PREFIX,
        "T2_X_SITEB": SITEB_PREFIX,
        "T2_X_SITEC": SITEC_PREFIX,
    })
    monkeypatch.setattr(helper, "get_rucio_client", lambda: object())

    def fake_find_other_file(filepath, sitemap, blocklist=None, rucio_client=None):
        return filepath.replace(SITEA_PREFIX, SITEC_PREFIX)

    monkeypatch.setattr(helper, "find_other_file", fake_find_other_file)


def test_no_xrootd_string_returns_no_xrootd_and_preserves_config(tmp_path, monkeypatch, capsys):
    _patch_rewrite_deps(monkeypatch)
    filesets = _fileset([("sampleA", [SITEA_PREFIX + "/store/data/a.root"])])
    config_path = _dump_config(tmp_path, filesets)
    log_path = tmp_path / "job_0.out"
    log_path.write_text("ValueError: something unrelated\n")

    rc = helper.rewrite_config_from_log(config_path, log_path)
    output = capsys.readouterr().out

    assert rc == helper.EXIT_NO_XROOTD
    assert output == ""
    assert _load_filesets(config_path)["sampleA"]["files"] == filesets["sampleA"]["files"]


def test_xrootd_failure_rewrites_all_files_from_failed_site(tmp_path, monkeypatch):
    _patch_rewrite_deps(monkeypatch)
    filesets = _fileset([
        ("sampleA", [
            SITEA_PREFIX + "/store/data/a.root",
            SITEB_PREFIX + "/store/data/b.root",
        ]),
        ("sampleB", [SITEA_PREFIX + "/store/data/c.root"]),
    ])
    config_path = _dump_config(tmp_path, filesets)
    log_path = tmp_path / "job_0.out"
    log_path.write_text(f"OSError: XRootD error\n  file: {SITEA_PREFIX}/store/data/a.root\n")

    rc = helper.rewrite_config_from_log(config_path, log_path)
    out = _load_filesets(config_path)

    assert rc == helper.EXIT_REWRITTEN
    assert list(out.keys()) == ["sampleA", "sampleB"]
    assert out["sampleA"]["files"] == [
        SITEC_PREFIX + "/store/data/a.root",
        SITEB_PREFIX + "/store/data/b.root",
    ]
    assert out["sampleB"]["files"] == [SITEC_PREFIX + "/store/data/c.root"]


def test_xrootd_failure_with_no_url_change_returns_no_change(tmp_path, monkeypatch):
    _patch_rewrite_deps(monkeypatch)
    monkeypatch.setattr(helper, "find_other_file", lambda filepath, sitemap, blocklist=None, rucio_client=None: filepath)
    filesets = _fileset([("sampleA", [SITEA_PREFIX + "/store/data/a.root"])])
    config_path = _dump_config(tmp_path, filesets)
    log_path = tmp_path / "job_0.out"
    log_path.write_text(f"received 0 bytes from XRootDSource while reading {SITEA_PREFIX}/store/data/a.root\n")

    rc = helper.rewrite_config_from_log(config_path, log_path)

    assert rc == helper.EXIT_NO_CHANGE
    assert _load_filesets(config_path)["sampleA"]["files"] == filesets["sampleA"]["files"]


def test_file_not_found_traceback_url_is_detected(tmp_path, monkeypatch):
    _patch_rewrite_deps(monkeypatch)
    filesets = _fileset([("sampleA", [SITEA_PREFIX + "/store/data/a.root"])])
    config_path = _dump_config(tmp_path, filesets)
    log_path = tmp_path / "job_0.out"
    log_path.write_text(
        "FileNotFoundError: file not found\n"
        "traceback line\n"
        "traceback line\n"
        f"'{SITEA_PREFIX}/store/data/a.root'\n"
    )

    rc = helper.rewrite_config_from_log(config_path, log_path)

    assert rc == helper.EXIT_REWRITTEN
    assert _load_filesets(config_path)["sampleA"]["files"] == [SITEC_PREFIX + "/store/data/a.root"]


def test_corrupt_input_data_uses_coffea_workitem_url(tmp_path, monkeypatch):
    # Real log shape: the skim probe prints an unrelated EOS URL above, the lzma
    # traceback has no URL, coffea names the input file ~18 lines below.
    _patch_rewrite_deps(monkeypatch)
    filesets = _fileset([("sampleA", [SITEA_PREFIX + "/store/data/a.root"])])
    config_path = _dump_config(tmp_path, filesets)
    log_path = tmp_path / "job_0.out"
    log_path.write_text(
        "read_skimmed_file: cannot read root://eoscms.cern.ch//eos/cms/store/group/skim.root\n"
        + "traceback line\n" * 5
        + "_lzma.LZMAError: Corrupt input data\n"
        + "traceback line\n" * 17
        + f"Exception: Failed processing file: WorkItem(dataset='sampleA', "
          f"filename='{SITEA_PREFIX}/store/data/a.root', treename='Events')\n"
    )

    rc = helper.rewrite_config_from_log(config_path, log_path)

    assert rc == helper.EXIT_REWRITTEN
    assert _load_filesets(config_path)["sampleA"]["files"] == [SITEC_PREFIX + "/store/data/a.root"]


def test_failed_sites_file_blocklists_previous_sites_and_reaches_redirector(tmp_path, monkeypatch):
    from pocket_coffea.utils.site_rewrite import GLOBAL_XROOTD_REDIRECTOR

    monkeypatch.setattr(helper, "get_xrootd_sites_map", lambda: {
        "T2_X_SITEA": SITEA_PREFIX,
        "T2_X_SITEB": SITEB_PREFIX,
    })
    monkeypatch.setattr(helper, "get_rucio_client", lambda: object())

    def fake_find_other_file(filepath, sitemap, blocklist=None, rucio_client=None):
        assert blocklist == {"T2_X_SITEA", "T2_X_SITEB"}
        return GLOBAL_XROOTD_REDIRECTOR + "store/" + filepath.split("/store/", 1)[1]

    monkeypatch.setattr(helper, "find_other_file", fake_find_other_file)
    filesets = _fileset([("sampleA", [SITEB_PREFIX + "/store/data/a.root"])])
    config_path = _dump_config(tmp_path, filesets)
    log_path = tmp_path / "job_0.out"
    log_path.write_text(f"OSError: XRootD error\n  file: {SITEB_PREFIX}/store/data/a.root\n")
    failed_sites = tmp_path / "failed_sites.txt"
    failed_sites.write_text("T2_X_SITEA\n")

    rc = helper.rewrite_config_from_log(config_path, log_path, failed_sites)

    assert rc == helper.EXIT_REWRITTEN
    assert _load_filesets(config_path)["sampleA"]["files"] == [
        GLOBAL_XROOTD_REDIRECTOR + "store/data/a.root"
    ]
    assert set(failed_sites.read_text().splitlines()) == {"T2_X_SITEA", "T2_X_SITEB"}
