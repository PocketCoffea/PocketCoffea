import awkward as ak

from workflows.base import ttHbbBaseProcessor

class MEMStudiesProcessor(ttHbbBaseProcessor):
    def __init__(self, year='2017', cfg='test.py', hist_dir='histograms/', hist2d =False, DNN=False) -> None:
        super().__init__(year=year, cfg=cfg, hist_dir=hist_dir, hist2d=hist2d, DNN=DNN)

    def parton_matching(self, events):
        JetIDs = [5]
        bquarks = ak.zeros_like(selev.LHEPart.pdgId == 5, dtype=bool)

    def process_extra(self, events: ak.Array) -> ak.Array:
        print("We are executing the processor MEMStudies!!!")
