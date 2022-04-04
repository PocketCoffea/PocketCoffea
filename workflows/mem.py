import sys
import awkward as ak
from coffea import hist

from workflows.base import ttHbbBaseProcessor
from lib.fill import fill_histograms_object

class MEMStudiesProcessor(ttHbbBaseProcessor):
    def __init__(self, cfg='test.py') -> None:
        super().__init__(cfg=cfg)
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
            bquarks = ak.with_name(bquarks[ak.argsort(bquarks.pt, ascending=False)], name='PtEtaPhiMCandidate')

        # Compute deltaR(b, jet) and save the nearest jet (deltaR matching)
        deltaR = ak.flatten(bquarks.metric_table(self.events.JetGood), axis=2)
        idx_pairs_sorted = ak.argsort(deltaR, axis=1)
        pairs = ak.argcartesian([bquarks, self.events.JetGood])
        pairs_sorted = pairs[idx_pairs_sorted]
        idx_bquarks, idx_JetGood = ak.unzip(pairs_sorted)

        hasMatch = ak.zeros_like(idx_JetGood, dtype=bool)
        Npairmax = ak.max(ak.num(idx_bquarks))
        # Loop over the (parton, jet) pairs
        for idx_pair in range(Npairmax):
            idx_bquark = ak.pad_none(idx_bquarks, Npairmax)[:,idx_pair]
            idx_match_candidates = idx_JetGood[ak.fill_none( (idx_bquarks == idx_bquark) & ~hasMatch, False)]
            idx_pair_candidates  = ak.local_index(idx_JetGood)[ak.fill_none( (idx_bquarks == idx_bquark) & ~hasMatch, False)]
            if idx_pair == 0:
                idx_matchedJet    = ak.unflatten( ak.firsts(idx_match_candidates), 1 )
                idx_matchedParton = ak.unflatten( idx_bquark, 1 )
                idx_matchedPair   = ak.unflatten( ak.firsts(idx_pair_candidates), 1 )
            else:
                # If the partons are matched in all events or the number of jets is smaller than the number of partons, stop iterating
                if ak.all( ( (ak.count(idx_matchedJet, axis=1) == ak.count(bquarks.pt, axis=1)) | (ak.count(self.events.JetGood.pt, axis=1) < ak.count(bquarks.pt, axis=1) ) ) ): break
                idx_matchedJet    = ak.concatenate( (idx_matchedJet, ak.unflatten( ak.firsts(idx_match_candidates), 1 ) ), axis=1 )
                idx_matchedParton = ak.concatenate( (idx_matchedParton, ak.unflatten( idx_bquark, 1 )), axis=1)
                idx_matchedPair   = ak.concatenate( (idx_matchedPair, ak.unflatten( ak.firsts(idx_pair_candidates), 1 ) ), axis=1 )
            # The mask `hasMatch` masks to False the 
            hasMatch = hasMatch | ak.fill_none(idx_JetGood == ak.fill_none(ak.firsts(idx_match_candidates), -99), False) | ak.fill_none(idx_bquarks == ak.fill_none(idx_bquark, -99), False)

        # The invalid jet matches result in a None value. Only non-None values are selected.
        idx_matchedParton = idx_matchedParton[~ak.is_none(idx_matchedJet, axis=1)]        
        idx_matchedJet = idx_matchedJet[~ak.is_none(idx_matchedJet, axis=1)]
        dr_matchedJet = deltaR[idx_pairs_sorted][~ak.is_none(idx_matchedPair, axis=1)]
        idx_matchedPair = idx_matchedPair[~ak.is_none(idx_matchedPair, axis=1)]
        matchedJet    = self.events.JetGood[idx_matchedJet]
        matchedParton = bquarks[idx_matchedParton]
        print("matchedJet", matchedJet)
        hasMatchedPartons = ak.count(idx_matchedParton, axis=1) == ak.count(bquarks.pt, axis=1)
        print(hasMatchedPartons)
        # Compute efficiency of parton matching for different cuts
        for cut in self._selections.keys():
            print(self.events.metadata["dataset"], cut, "matched partons =", round(100*ak.sum(hasMatchedPartons[self._cuts.all(*self._selections[cut])])/ak.size(hasMatchedPartons[self._cuts.all(*self._selections[cut])]), 2), "%")
        self.events["BQuark"] = bquarks
        self.events["JetGoodMatched"] = matchedJet
        self.events["BQuarkMatched"] = matchedParton
        self.events["BQuarkMatched"] = ak.with_field(self.events.BQuarkMatched, dr_matchedJet, "drMatchedJet")
        print("deltaR", dr_matchedJet)

    def count_bquarks(self):
        self.events["nbquark"] = ak.count(self.events.BQuark.pt, axis=1)

    def fill_histograms(self):
        super().fill_histograms()
        fill_histograms_object(self, self.events.BQuarkMatched, self.bquark_hists)

    def fill_histograms_extra(self):
        for histname, h in self.output.items():
            if type(h) is not hist.Hist: continue
            if histname not in self.bquark_hists: continue
            for cut in self._selections.keys():
                if histname in self.bquark_hists:
                    parton = self.events.BQuarkMatched
                    weight = ak.flatten(self.weights.weight() * ak.Array(ak.fill_none(ak.ones_like(parton.pt), 0) * self._cuts.all(*self._selections[cut])))
                    fields = {k: ak.flatten(ak.fill_none(parton[k], -9999)) for k in h.fields if k in dir(parton)}
                    h.fill(dataset=self._sample, cut=cut, year=self._year, **fields, weight=weight)
            self.output[histname] = h

    def process_extra(self) -> ak.Array:
        self.parton_matching()
        self.count_bquarks()
        print(self.events.nbquark)
