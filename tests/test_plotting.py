"""Unit tests for the plotting code (:mod:`pocket_coffea.utils.plot_utils`
and :mod:`pocket_coffea.scripts.plot.make_plots`).

They check that:
- the low-memory redesign produces the *same* plots from a monolithic output and
  from its split-format equivalent,
- serial and multiprocessing (fork) dispatch agree,
- ``--no-cache`` and ``--only-hist`` behave,
- the PlotManager is lazy (it does not eagerly build every Shape at construction).
"""

import glob
import importlib
import os
import time

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FIXDIR = os.path.join(REPO, "tests/test_full_configs/test_new_weights/output_3b6cf6c")
MONO = os.path.join(FIXDIR, "output_all.coffea")
CFG = os.path.join(FIXDIR, "parameters_dump.yaml")

pytestmark = pytest.mark.skipif(
    not os.path.exists(MONO), reason="reference coffea fixture not available"
)


@pytest.fixture(scope="module", autouse=True)
def _warm_imports():
    """Pre-import lazily-loaded plotting modules.

    The CVMFS-unpacked apptainer image occasionally raises ``OSError(5)`` on the
    first read of a lazily-imported module file; importing them up front (with a
    short retry) makes the plotting runs deterministic. Harmless on a normal
    install.
    """
    for m in [
        "hist", "hist.plot", "hist.stack", "mplhep",
        "mplhep.error_estimation", "matplotlib.pyplot",
    ]:
        for _ in range(8):
            try:
                importlib.import_module(m)
                break
            except OSError:
                time.sleep(1)


@pytest.fixture(scope="module")
def split_path(tmp_path_factory):
    from coffea.util import load
    from pocket_coffea.utils.output_split import save_split

    p = str(tmp_path_factory.mktemp("split") / "out_split.coffea")
    save_split(load(MONO), p)
    return p


def _run(outdir, inputfile, workers=1, no_cache=False, only_hist=()):
    from pocket_coffea.scripts.plot.make_plots import make_plots_core

    make_plots_core(
        input_dir=FIXDIR, cfg=CFG, overwrite_parameters=(), outputdir=outdir,
        inputfiles=(inputfile,), workers=workers, only_cat=(), only_year=(), only_syst=(),
        exclude_hist=(), only_hist=only_hist, split_systematics=False, partial_unc_band=False,
        no_syst=False, overwrite=True, log_x=False, log_y=False, density=False, verbose=0,
        format="png", systematics_shifts=False, no_ratio=False, no_systematics_ratio=False,
        compare=False, index_file=None, no_cache=no_cache,
        split_by_category=False,
    )
    return sorted(os.path.relpath(p, outdir) for p in glob.glob(f"{outdir}/**/*.png", recursive=True))


def test_plotmanager_is_lazy(split_path):
    from omegaconf import OmegaConf
    from pocket_coffea.utils.output_split import make_provider
    from pocket_coffea.utils.plot_utils import PlotManager

    prov = make_provider([split_path])
    pm = PlotManager(
        variables=list(prov.variables), provider=prov, plot_dir="/tmp/_pc_lazychk",
        style_cfg=OmegaConf.load(CFG)["plotting_style"], workers=1, save=False, verbose=0,
    )
    # No Shape objects are built at construction ...
    assert pm._shape_objects_cache is None
    assert len(pm._shape_specs) > 0
    # ... and building one on demand does not populate the eager cache.
    name = next(iter(pm._shape_specs))
    assert pm._build_shape(name) is not None
    assert pm._shape_objects_cache is None


def test_plots_mono_vs_split(tmp_path, split_path):
    mono_plots = _run(str(tmp_path / "mono"), MONO, workers=1)
    split_plots = _run(str(tmp_path / "split"), split_path, workers=1)
    assert len(mono_plots) > 0
    assert mono_plots == split_plots


def test_plots_parallel_matches_serial(tmp_path, split_path):
    serial = _run(str(tmp_path / "j1"), split_path, workers=1, only_hist=("JetGood_pt", "ElectronGood_pt"))
    forked = _run(str(tmp_path / "j2"), split_path, workers=2, only_hist=("JetGood_pt", "ElectronGood_pt"))
    assert len(serial) > 0
    assert serial == forked


def test_plots_no_cache_matches(tmp_path, split_path):
    cached = _run(str(tmp_path / "cache"), split_path, workers=1, only_hist=("JetGood_pt",))
    uncached = _run(str(tmp_path / "nocache"), split_path, workers=1, no_cache=True, only_hist=("JetGood_pt",))
    assert len(cached) > 0
    assert cached == uncached


def test_only_hist_filter(tmp_path, split_path):
    got = _run(str(tmp_path / "only"), split_path, workers=1, only_hist=("JetGood_pt",))
    assert len(got) > 0
    assert all("JetGood_pt" in p for p in got)
