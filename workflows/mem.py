import awkward as ak

from workflows.base import ttHbbBaseProcessor

class MEMStudiesProcessor(ttHbbBaseProcessor):
    def __init__(self, year='2017', cfg='test.py', hist_dir='histograms/', hist2d =False, DNN=False) -> None:
        super().__init__(year=year, cfg=cfg, hist_dir=hist_dir, hist2d=hist2d, DNN=DNN)
        self.bquark_hists = [histname for histname in self._hist_dict.keys() if 'parton' in histname]

    def parton_matching(self, events: ak.Array) -> ak.Array:
        if events.metadata["dataset"] == 'ttHTobb':
            isHiggs = events.GenPart.pdgId == 25
            isHard = events.GenPart.hasFlags(['fromHardProcess'])
            hasTwoChildren = ak.num(events.GenPart.childrenIdxG, axis=2) == 2
            higgs = events.GenPart[isHiggs & isHard & hasTwoChildren]
            bquarks = ak.flatten(higgs.children, axis=2)
            b = bquarks[ak.argsort(bquarks.pdgId)][:,1]
            bbar = bquarks[ak.argsort(bquarks.pdgId)][:,0]

            for (i, parton) in enumerate([b, bbar]):
                deltaR = parton.delta_r(events.Jet)
                hasJet = ak.sum(ak.mask(deltaR, deltaR<1),axis=-1) > 0
                sortedR = ak.argsort(ak.mask(deltaR, deltaR<1))
                if i == 0:
                    jets_matched = ak.unflatten( ak.mask( ak.firsts(events.Jet[sortedR]), hasJet ), 1 )
                    idx_matched = ak.unflatten( ak.mask( ak.firsts(sortedR), hasJet ), 1 )
                else:
                    jets_matched = ak.concatenate( ( jets_matched, ak.unflatten( ak.mask( ak.firsts(events.Jet[sortedR]), hasJet ), 1 ) ), axis=1 )
                    idx_matched = ak.concatenate( ( idx_matched, ak.unflatten( ak.firsts(sortedR), 1 ) ), axis=1 )

            events["BQuark"] = bquarks
            hasMatchedJets = ~ak.any(ak.is_none(jets_matched, axis=1), axis=1)
            #isUnique = ak.fill_none(~(idx_matched[:,0] == idx_matched[:,1]), True)
            isUnique = ak.fill_none(~(idx_matched[:,0] == idx_matched[:,1]), True)
            print("jets_matched", jets_matched)
            print("idx_matched", idx_matched)
            print("hasMatchedJets", hasMatchedJets)
            print("isUnique", isUnique)
            print("jets_matched[hasMatchedJets & ~isUnique]", jets_matched[hasMatchedJets & ~isUnique])
            print("idx_matched[hasMatchedJets & ~isUnique]", idx_matched[hasMatchedJets & ~isUnique])
            #print(bquarks.delta_r(events.BJetGood))

    def fill_histograms_extra(self, histname, cut, events):
        if histname in self.bquark_hists:
            parton = events.BQuark
            weight = ak.flatten(self.weights.weight() * ak.Array(ak.ones_like(parton.pt) * self._cuts.all(*self._selections[cut])))
            fields = {k: ak.flatten(ak.fill_none(parton[k], -9999)) for k in h.fields if k in dir(parton)}

    def process_extra(self, events: ak.Array) -> ak.Array:
        print(events.metadata["dataset"])
        self.parton_matching(events)
