"""Split of one MC sample between the years that use the same MC files (parameter `mc_split`).
Synthetic events, no network. Also runs as a script: python tests/test_mc_split.py"""
from types import SimpleNamespace

import awkward as ak
import numpy as np
from omegaconf import OmegaConf

from pocket_coffea.lib.mc_split import event_uniform, mc_split_mask
from pocket_coffea.workflows.base import BaseProcessorABC

N = 200_000
YEARS = ["2025", "2026"]
LUMI = {"2024": 100.0, "2025": 110.0, "2026": 26.0, "2027": 64.0}


def make_params(mc_split):
    return OmegaConf.create(
        {"mc_split": mc_split, "lumi": {"picobarns": {y: {"tot": v} for y, v in LUMI.items()}}}
    )


def make_events(n=N):
    rng = np.random.default_rng(1)
    return ak.Array({
        "event": rng.integers(0, 2**40, n),
        "luminosityBlock": rng.integers(1, 5000, n),
        "genWeight": rng.choice([-1.0, 1.0], n, p=[0.1, 0.9]) * 2.0,
    })


def test_event_uniform():
    events = make_events()
    u = event_uniform(events.event, events.luminosityBlock)
    assert np.all((u >= 0) & (u < 1))
    assert np.array_equal(u, event_uniform(events.event, events.luminosityBlock))  # deterministic
    # independent of the position of the event in the chunk
    assert np.array_equal(u[::-1], event_uniform(events.event[::-1], events.luminosityBlock[::-1]))
    counts = np.histogram(u, bins=20, range=(0, 1))[0]
    assert np.all(abs(counts - N / 20) < 5 * np.sqrt(N / 20))  # uniform


def test_mc_split_mask():
    events = make_events()
    params = make_params([["2022", "2023"], YEARS + ["2027"]])
    group = YEARS + ["2027"]
    masks = [mc_split_mask(events, y, params) for y in group]
    tot = sum(LUMI[y] for y in group)
    for y, m in zip(group, masks):
        assert abs(np.mean(m) - LUMI[y] / tot) < 0.005, (y, np.mean(m))
    assert np.all(np.sum(masks, axis=0) == 1)  # disjoint and complete
    # years in no group, and the default (no mc_split): no split
    assert mc_split_mask(events, "2024", params) is None
    assert mc_split_mask(events, "2025", make_params([])) is None
    assert mc_split_mask(events, "2025", OmegaConf.create({})) is None
    # years written as integers in the yaml file
    assert np.array_equal(mc_split_mask(events, "2025", make_params([[2025, 2026, 2027]])), masks[0])
    try:
        mc_split_mask(events, "2025", make_params([YEARS, ["2025", "2027"]]))
    except ValueError:
        pass
    else:
        raise AssertionError("a year in two groups must raise")


class _Processor(BaseProcessorABC):
    '''The base process() up to the skim, without a Configurator.'''

    def __init__(self, params, year, isMC=True, isSkim=False):
        self.params, self._meta = params, (year, isMC, isSkim)
        self.cfg = SimpleNamespace(save_skimmed_files=None)
        self.workflow_options = {}
        self.output_format = {"sum_genweights": {}, "sum_signOf_genweights": {},
                              "cutflow": {"initial": {}}, "processing_time": {}}

    def load_metadata(self):
        self._dataset = "ds"
        self._year, self._isMC, self._isSkim = self._meta

    def skim_events(self):
        self.has_events = False  # stop after the normalization counters

    def apply_object_preselection(self, variation):
        pass

    def count_objects(self, variation):
        pass


def test_processor_normalization():
    events = make_events()
    events["skimRescaleGenWeight"] = np.ones(N)
    params = make_params([YEARS])
    sumw = ak.sum(events.genWeight)
    kept, sumw_years = [], 0.0
    for year in YEARS:
        proc = _Processor(params, year)
        out = proc.process(events)
        mask = mc_split_mask(events, year, params)
        assert len(proc.events) == np.sum(mask)
        # normalization counters: the ones of the kept events
        assert out["sum_genweights"]["ds"] == ak.sum(events.genWeight[mask])
        assert out["sum_signOf_genweights"]["ds"] == ak.sum(np.sign(events.genWeight[mask]))
        assert out["cutflow"]["initial"]["ds"] == N  # events read from the file
        kept.append(set(zip(proc.events.event.tolist(), proc.events.luminosityBlock.tolist())))
        sumw_years += out["sum_genweights"]["ds"]
    assert not set.intersection(*kept)
    assert set.union(*kept) == set(zip(events.event.tolist(), events.luminosityBlock.tolist()))
    assert np.isclose(sumw_years, sumw)

    # no change: data, a year in no group, no mc_split
    for proc in [_Processor(params, "2025", isMC=False), _Processor(params, "2024"),
                 _Processor(make_params([]), "2025")]:
        out = proc.process(events)
        assert len(proc.events) == N
        if proc._isMC:
            assert out["sum_genweights"]["ds"] == sumw

    # skim made with the same split: all the events are of this year, no change
    mask = mc_split_mask(events, "2025", params)
    proc = _Processor(params, "2025", isSkim=True)
    out = proc.process(events[mask])
    assert len(proc.events) == np.sum(mask)
    assert out["sum_genweights"]["ds"] == ak.sum(events.genWeight[mask])
    # skim made without the split: the normalization from the skim metadata would be wrong
    try:
        _Processor(params, "2025", isSkim=True).process(events)
    except Exception as e:
        assert "mc_split" in str(e)
    else:
        raise AssertionError("mc_split on a skim with events of the other years must raise")


if __name__ == "__main__":
    test_event_uniform()
    test_mc_split_mask()
    test_processor_normalization()
    print("all ok")
