import awkward as ak

from workflows.base import ttHbbBaseProcessor

class MEMStudiesProcessor(ttHbbBaseProcessor):
    def __init__(self, year='2017', cfg='test.py', hist_dir='histograms/', hist2d =False, DNN=False) -> None:
        super().__init__(year=year, cfg=cfg, hist_dir=hist_dir, hist2d=hist2d, DNN=DNN)

    def parton_matching(self, events):
        isHiggs = events.GenPart.pdgId == 25
        isHard = events.GenPart.hasFlags(['isHardProcess'])
        hasTwoChildren = ak.num(events.GenPart.children, axis=2) == 2
        higgs = events.GenPart[isHiggs & isHard & hasTwoChildren]
        bquarks = higgs.children 
        print(bquarks.pdgId)

    def process_extra(self, events: ak.Array) -> ak.Array:
        print(events.metadata["dataset"])
        self.parton_matching(events)
