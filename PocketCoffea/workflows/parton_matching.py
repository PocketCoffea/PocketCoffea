import sys
import awkward as ak
from coffea import hist
import numba

from .base import ttHbbBaseProcessor
from ..lib.fill import fill_histograms_object

@numba.jit
def get_matching_pairs_indices(idx_quark, idx_jets, builder, builder2):
    for ev_q, ev_j in zip(idx_quark, idx_jets):
        builder.begin_list()
        builder2.begin_list()
        q_done = []
        j_done = []
        for i, (q,j) in enumerate(zip(ev_q, ev_j)):
            if q not in q_done:
                if j not in j_done:
                    builder.append(i)
                    q_done.append(q)
                    j_done.append(j)
                else: 
                    builder2.append(i)
        builder.end_list()
        builder2.end_list()
    return builder, builder2

class PartonMatchingProcessor(ttHbbBaseProcessor):
    def __init__(self,cfg) -> None:
        super().__init__(cfg=cfg)
        self.parton_hists = [histname for histname in self._hist_dict.keys() if 'parton' in histname and not histname in self.nobj_hists]
        self.dr_min = 0.4
               
    def parton_matching(self) -> ak.Array:
        # Selects quarks at LHE level
        isOutgoing = self.events.LHEPart.status == 1
        isB = abs(self.events.LHEPart.pdgId) == 5
        #bquarks = self.events.LHEPart[isB & isOutgoing]
        quarks = self.events.LHEPart[isOutgoing]
        
        # Select b-quarks at Gen level, coming from H->bb decay
        if self.events.metadata["sample"] == 'ttHTobb':
            isHiggs = self.events.GenPart.pdgId == 25
            isHard = self.events.GenPart.hasFlags(['fromHardProcess'])
            hasTwoChildren = ak.num(self.events.GenPart.childrenIdxG, axis=2) == 2
            higgs = self.events.GenPart[isHiggs & isHard & hasTwoChildren]
            quarks = ak.concatenate( (quarks, ak.flatten(higgs.children, axis=2)), axis=1 )
            # Sort b-quarks by pt
            quarks = ak.with_name(quarks[ak.argsort(quarks.pt, ascending=False)], name='PtEtaPhiMCandidate')

        # Compute deltaR(quark, jet) and save the nearest jet (deltaR matching)
        deltaR = ak.flatten(quarks.metric_table(self.events.JetGood), axis=2)
        # keeping only the pairs with a deltaR min
        maskDR = deltaR < self.dr_min
        deltaRcut = deltaR[maskDR]
        idx_pairs_sorted = ak.argsort(deltaRcut, axis=1)
        pairs = ak.argcartesian([quarks, self.events.JetGood])[maskDR]
        pairs_sorted = pairs[idx_pairs_sorted]
        idx_quarks, idx_jets = ak.unzip(pairs_sorted)
        
        _idx_matched_pairs, _idx_missed_pairs = get_matching_pairs_indices(idx_quarks, idx_jets, ak.ArrayBuilder(), ak.ArrayBuilder())
        idx_matched_pairs = _idx_matched_pairs.snapshot()
        idx_missed_pairs = _idx_missed_pairs.snapshot()
        # The invalid jet matches result in a None value. Only non-None values are selected.
        matched_quarks = quarks[idx_quarks[idx_matched_pairs]]
        matched_jets = self.events.JetGood[idx_jets[idx_matched_pairs]]
        deltaR_matched = deltaRcut[idx_matched_pairs]

        self.events["Parton"] = quarks
        self.events["PartonMatched"] = ak.with_field(matched_quarks, deltaR_matched, "dRMatchedJet" )
        self.events["JetGoodMatched"] = matched_jets

    def count_partons(self):
        self.events["nparton"] = ak.count(self.events.Parton.pt, axis=1)
        self.events["nparton_matched"] = ak.num(self.events.PartonMatched, axis=1)

    def fill_histograms_extra(self):
        fill_histograms_object(self, self.events.PartonMatched, self.parton_hists)

    def process_extra_after_presel(self) -> ak.Array:
        self.parton_matching()
        self.count_partons()

