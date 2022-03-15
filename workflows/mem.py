import awkward as ak
from coffea import hist

from workflows.base import ttHbbBaseProcessor

class MEMStudiesProcessor(ttHbbBaseProcessor):
    def __init__(self, year='2017', cfg='test.py', hist_dir='histograms/', hist2d =False, DNN=False) -> None:
        super().__init__(year=year, cfg=cfg, hist_dir=hist_dir, hist2d=hist2d, DNN=DNN)
        self.bquark_hists = [histname for histname in self._hist_dict.keys() if 'bquark' in histname and not histname in self.nobj_hists]

    def parton_matching(self) -> ak.Array:
        # Select b-quarks at LHE level
        isOutgoing = self.events.LHEPart.status == 1
        isB = abs(self.events.LHEPart.pdgId) == 5
        bquarks = self.events.LHEPart[isB & isOutgoing]

        # Select b-quarks at Gen level, coming from H->bb decay
        if self.events.metadata["dataset"] == 'ttHTobb':
            isHiggs = self.events.GenPart.pdgId == 25
            isHard = self.events.GenPart.hasFlags(['fromHardProcess'])
            hasTwoChildren = ak.num(self.events.GenPart.childrenIdxG, axis=2) == 2
            higgs = self.events.GenPart[isHiggs & isHard & hasTwoChildren]
            bquarks = ak.concatenate( (bquarks, ak.flatten(higgs.children, axis=2)), axis=1 )
            # Sort b-quarks by pt
            bquarks = ak.with_name(bquarks[ak.argsort(bquarks.pt)], name='PtEtaPhiMCandidate')

        # Compute deltaR(b, jet) and save the nearest jet (deltaR matching)
        Nbmax = ak.max(ak.num(bquarks))
        bquarks = ak.pad_none(bquarks, Nbmax)
        deltaR = ak.flatten(bquarks.metric_table(self.events.JetGood), axis=2)
        idx_pairs_sorted = ak.argsort(deltaR, axis=1)
        pairs = ak.argcartesian([bquarks, self.events.JetGood])
        pairs_sorted = pairs[idx_pairs_sorted]
        idx_bquarks, idx_JetGood = ak.unzip(pairs_sorted)

        hasMatch = ak.zeros_like(idx_JetGood)
        for idx_bquark in range(Nbmax):
        #for idx_pair in range(ak.max(ak.num(idx_bquarks))):
            idx_bquark = idx_bquarks[idx_pair]
            print(idx_bquark, self.events.metadata["dataset"], hasMatch)
            print(idx_bquarks)
            #if idx_bquark == 0:
            if idx_bquark == 0:
                matchedJet     = ak.unflatten( ak.firsts(self.events.JetGood[idx_JetGood[( (idx_bquarks == idx_bquarks[:,idx_bquark]) & ~hasMatch )]]), 1 )
                idx_matchedJet = ak.unflatten( ak.firsts(idx_JetGood[( (idx_bquarks == idx_bquarks[:,idx_bquark]) & ~hasMatch)]), 1 )
                dr_matchedJet  = ak.unflatten( ak.firsts(deltaR[idx_pairs_sorted[( (idx_bquarks == idx_bquarks[:,idx_bquark]) & ~hasMatch)]]), 1 )
            else:
                matchedJet     = ak.concatenate( (matchedJet, ak.unflatten( ak.firsts(self.events.JetGood[idx_JetGood[( (idx_bquarks == idx_bquarks[:,idx_bquark]) & ~hasMatch )]]), 1 ) ), axis=1 )
                idx_matchedJet = ak.concatenate( (idx_matchedJet, ak.unflatten( ak.firsts(idx_JetGood[( (idx_bquarks == idx_bquarks[:,idx_bquark]) & ~hasMatch)]), 1 )), axis=1 )
                dr_matchedJet  = ak.concatenate( (dr_matchedJet, ak.unflatten( ak.firsts(deltaR[idx_pairs_sorted[( (idx_bquarks == idx_bquarks[:,idx_bquark]) & ~hasMatch)]]), 1 ) ), axis=1 )
            hasMatch = hasMatch | (idx_matchedJet == idx_matchedJet[:,idx_bquark])
        self.events["BQuark"] = ak.with_field(bquarks, matchedJet, "JetGoodMatched")
        self.events["BQuark"] = ak.with_field(self.events.BQuark, dr_matchedJet, "drMatchedJet")
        self.events["BQuark"] = ak.with_field(self.events.BQuark, ak.num(bquarks), "Nb")
        print("deltaR", deltaR)
        print("self.events.BQuark.JetGoodMatched", self.events.BQuark.JetGoodMatched)

    def count_objects_extra(self):
        self.events["nbquark"] = ak.count(self.events.BQuark.pt, axis=1)

    def fill_histograms_extra(self, output):
        for histname, h in output.items():
            if type(h) is not hist.Hist: continue
            if histname not in self.bquark_hists: continue
            for cut in self._selections.keys():
                if histname in self.bquark_hists:
                    parton = self.events.BQuark
                    weight = ak.flatten(self.weights.weight() * ak.Array(ak.fill_none(ak.ones_like(parton.pt), 0) * self._cuts.all(*self._selections[cut])))
                    fields = {k: ak.flatten(ak.fill_none(parton[k], -9999)) for k in h.fields if k in dir(parton)}
                    h.fill(dataset=self.events.metadata["dataset"], cut=cut, year=self._year, **fields, weight=weight)
            output[histname] = h
        return output

    def process_extra(self) -> ak.Array:
        self.parton_matching()
        self.count_objects_extra()
        print(self.events.nbquark)
