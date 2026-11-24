"""Offline test that ROOT export separates datasets by year (no EOS needed).

Previously the per-year export summed over ALL datasets of a sample regardless of
year, writing the identical all-years total into every year directory.
"""
import hist
import uproot
from coffea.util import save

from pocket_coffea.utils.export import (
    save_histogram_to_root,
    export_coffea_output_to_root,
)


def _hist(cat, n_entries):
    h = hist.Hist(
        hist.axis.StrCategory([cat], name="cat", growth=True),
        hist.axis.Regular(4, 0.0, 4.0, name="x"),
    )
    h.fill(cat=cat, x=[0.5] * n_entries)
    return h


def test_save_histogram_filters_by_year(tmp_path):
    hist_dict = {"ttbb": {"ttbb_2017": _hist("baseline", 10), "ttbb_2018": _hist("baseline", 3)}}
    out = tmp_path / "x.root"
    save_histogram_to_root(hist_dict, "2018", "baseline", out, datasets={"ttbb_2018"})
    with uproot.open(out) as f:
        assert f["ttbb"].values().sum() == 3  # only 2018, not 13


def test_export_coffea_output_separates_years(tmp_path):
    output = {
        "variables": {
            "myvar": {"ttbb": {"ttbb_2017": _hist("baseline", 10), "ttbb_2018": _hist("baseline", 3)}}
        },
        "datasets_metadata": {
            "by_dataset": {"ttbb_2017": {"year": "2017"}, "ttbb_2018": {"year": "2018"}}
        },
    }
    coffea_file = tmp_path / "out.coffea"
    save(output, coffea_file)
    root_dir = tmp_path / "root"

    export_coffea_output_to_root(
        coffea_file, root_dir, ["myvar"], ["baseline"], ["2017", "2018"]
    )

    with uproot.open(root_dir / "2017" / "baseline" / "myvar.root") as f:
        v2017 = f["ttbb"].values().sum()
    with uproot.open(root_dir / "2018" / "baseline" / "myvar.root") as f:
        v2018 = f["ttbb"].values().sum()

    assert v2017 == 10
    assert v2018 == 3  # not 13 -> the two years are genuinely separated
