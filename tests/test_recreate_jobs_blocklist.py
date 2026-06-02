"""Tests for the blocklist-driven rewrite used by --recreate-jobs.

These exercise the pure helper `rewrite_fileset_blocklist` in
`pocket_coffea.utils.site_rewrite`, which is reused by both the lxplus and
rubin manual-job executors. The DAS query inside `find_other_file` is
stubbed out so the tests are self-contained and don't pull in dask.
"""
from collections import OrderedDict

import pytest

from pocket_coffea.utils import site_rewrite as ex


SITEA_PREFIX = "root://siteA.example//"
SITEB_PREFIX = "root://siteB.example//"
SITEC_PREFIX = "root://siteC.example//"

SITEMAP = {
    "T2_X_SITEA": SITEA_PREFIX,
    "T2_X_SITEB": SITEB_PREFIX,
    "T2_X_SITEC": SITEC_PREFIX,
}


@pytest.fixture
def das_sites(monkeypatch):
    """Patch the rucio replica lookup in `_query_replicas` with a
    controllable mapping from LFN -> list of site names that hold the file.
    Named after the old DAS-based lookup; today the production code uses
    `rucio.Client.list_replicas`."""
    table = {}

    def fake_query(lfn, client=None, scope="cms"):
        return list(table.get(lfn, []))

    monkeypatch.setattr(ex, "_query_replicas", fake_query)
    return table


def _fileset(files_by_sample):
    """Build a fileset OrderedDict so we can assert order preservation."""
    fs = OrderedDict()
    for sample, files in files_by_sample:
        fs[sample] = {"files": list(files), "metadata": {"nevents": "1"}}
    return fs


def test_alt_site_replacement(das_sites):
    """A file at a blocklisted site is rewritten to a non-blocklisted alt."""
    f = SITEA_PREFIX + "/store/data/foo.root"
    das_sites["/store/data/foo.root"] = ["T2_X_SITEA", "T2_X_SITEC"]
    fileset = _fileset([("sampleA", [f])])

    out = ex.rewrite_fileset_blocklist(fileset, SITEMAP, blocklist={"T2_X_SITEA"})

    assert out["sampleA"]["files"] == [SITEC_PREFIX + "/store/data/foo.root"]


def test_fallback_to_global_redirector(das_sites):
    """If DAS has no non-blocklisted alternative, fall back to the global xrd redirector."""
    f = SITEA_PREFIX + "/store/data/foo.root"
    das_sites["/store/data/foo.root"] = ["T2_X_SITEA"]  # only blocklisted replicas
    fileset = _fileset([("sampleA", [f])])

    out = ex.rewrite_fileset_blocklist(fileset, SITEMAP, blocklist={"T2_X_SITEA"})

    expected = ex.GLOBAL_XROOTD_REDIRECTOR + "store/data/foo.root"
    assert out["sampleA"]["files"] == [expected]


def test_non_blocklisted_file_untouched(das_sites):
    """A file at a site that is not blocklisted must be left exactly as-is."""
    f = SITEB_PREFIX + "/store/data/bar.root"
    # No DAS entry needed — find_other_file should not be called for this file.
    fileset = _fileset([("sampleB", [f])])

    out = ex.rewrite_fileset_blocklist(fileset, SITEMAP, blocklist={"T2_X_SITEA"})

    assert out["sampleB"]["files"] == [f]


def test_order_preservation(das_sites):
    """File order within a dataset and dataset order across the fileset must be preserved."""
    files_a = [
        SITEA_PREFIX + "/store/data/a1.root",
        SITEB_PREFIX + "/store/data/a2.root",
        SITEA_PREFIX + "/store/data/a3.root",
    ]
    files_b = [SITEA_PREFIX + "/store/data/b1.root"]
    das_sites["/store/data/a1.root"] = ["T2_X_SITEC"]
    das_sites["/store/data/a3.root"] = ["T2_X_SITEC"]
    das_sites["/store/data/b1.root"] = ["T2_X_SITEC"]
    fileset = _fileset([("sampleA", files_a), ("sampleB", files_b)])

    out = ex.rewrite_fileset_blocklist(fileset, SITEMAP, blocklist={"T2_X_SITEA"})

    assert list(out.keys()) == ["sampleA", "sampleB"]
    assert out["sampleA"]["files"] == [
        SITEC_PREFIX + "/store/data/a1.root",
        SITEB_PREFIX + "/store/data/a2.root",       # untouched site, untouched URL
        SITEC_PREFIX + "/store/data/a3.root",
    ]
    assert out["sampleB"]["files"] == [SITEC_PREFIX + "/store/data/b1.root"]


def test_empty_blocklist_is_noop(das_sites):
    f = SITEA_PREFIX + "/store/data/foo.root"
    fileset = _fileset([("sampleA", [f])])

    out = ex.rewrite_fileset_blocklist(fileset, SITEMAP, blocklist=set())

    assert out is fileset or out["sampleA"]["files"] == [f]


# ----------------------- rewrite_fileset_to_redirector -----------------------

def test_redirector_rewrites_all_files_no_rucio(das_sites):
    """rewrite_fileset_to_redirector must NOT call _query_replicas: it just
    swaps the redirector prefix on every file. Empty das_sites table proves
    no lookup happens (otherwise files at non-mapped sites would error)."""
    f1 = SITEA_PREFIX + "/store/data/a.root"
    f2 = SITEB_PREFIX + "/store/data/b.root"
    fileset = _fileset([("sampleA", [f1, f2])])

    out = ex.rewrite_fileset_to_redirector(fileset)

    assert out["sampleA"]["files"] == [
        ex.GLOBAL_XROOTD_REDIRECTOR + "store/data/a.root",
        ex.GLOBAL_XROOTD_REDIRECTOR + "store/data/b.root",
    ]


def test_redirector_accepts_custom_url():
    custom = "root://my-redirector.example//"
    f = SITEA_PREFIX + "/store/data/foo.root"
    out = ex.rewrite_fileset_to_redirector(_fileset([("s", [f])]), redirector=custom)
    assert out["s"]["files"] == [custom + "store/data/foo.root"]


def test_redirector_preserves_order_across_datasets():
    f1 = SITEA_PREFIX + "/store/data/a.root"
    f2 = SITEB_PREFIX + "/store/data/b.root"
    f3 = SITEA_PREFIX + "/store/data/c.root"
    fileset = _fileset([("A", [f1, f2]), ("B", [f3])])

    out = ex.rewrite_fileset_to_redirector(fileset)

    assert list(out.keys()) == ["A", "B"]
    assert out["A"]["files"] == [
        ex.GLOBAL_XROOTD_REDIRECTOR + "store/data/a.root",
        ex.GLOBAL_XROOTD_REDIRECTOR + "store/data/b.root",
    ]
    assert out["B"]["files"] == [ex.GLOBAL_XROOTD_REDIRECTOR + "store/data/c.root"]


def test_redirector_leaves_non_lfn_urls_unchanged():
    """Files without a /store/ segment can't be safely rewritten — leave them."""
    weird = "file:///local/path/foo.root"
    fileset = _fileset([("s", [weird])])
    out = ex.rewrite_fileset_to_redirector(fileset)
    assert out["s"]["files"] == [weird]
