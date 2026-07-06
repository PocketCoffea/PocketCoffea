"""Offline test that dataset selection does not share one metadata dict across datasets.

A wildcard query selects many datasets with a single `metadata` dict; do_save later
mutates each dataset's metadata in place (e.g. das_names). If the same dict object is
shared, every dataset ends up with the last one's provenance.

`do_select` itself uses no network, but importing dataset_query pulls in
`pocket_coffea.utils.rucio`, which imports the `rucio` package at module load. When
rucio is unavailable we install lightweight stubs so this stays a genuinely offline
test (the real package is used whenever it is importable).
"""
import sys
import types


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

from pocket_coffea.scripts.dataset.dataset_query import DataDiscoveryCLI  # noqa: E402


def _cli_with_query(query_list):
    # Bypass __init__ (which sets up rucio); we only exercise do_select's bookkeeping.
    cli = DataDiscoveryCLI.__new__(DataDiscoveryCLI)
    cli.last_query_list = list(query_list)
    cli.selected_datasets = []
    cli.selected_datasets_metadata = []
    return cli


def test_do_select_metadata_is_per_dataset_copy():
    cli = _cli_with_query(["/DAS/A/NANOAODSIM", "/DAS/B/NANOAODSIM"])
    shared_meta = {"year": "2018", "isMC": "True", "das_names": ["placeholder"]}

    cli.do_select(selection="all", metadata=shared_meta)

    assert len(cli.selected_datasets_metadata) == 2
    m0, m1 = cli.selected_datasets_metadata
    # The stored dicts must be independent objects (and independent of the input).
    assert m0 is not m1
    assert m0 is not shared_meta

    # Mutating one dataset's metadata (as do_save does) must not touch the other's.
    m0["das_names"] = ["/DAS/A/NANOAODSIM"]
    assert m1["das_names"] == ["placeholder"]
