import sys
import awkward as ak
from coffea import hist
import numba

from .base import ttHbbBaseProcessor
from ..lib.fill import fill_histograms_object
from ..lib.deltaR_matching import object_matching

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

        # Calling our general object_matching function.
        # The output is an awkward array with the shape of the second argument and None where there is no matching.
        # So, calling like this, we will get out an array of matched_quarks with the dimension of the JetGood. 
        matched_quarks, matched_jets, deltaR_matched = object_matching(quarks, self.events.JetGood, dr_min=self.dr_min)
        
        self.events["Parton"] = quarks
        self.events["PartonMatched"] = ak.with_field(matched_quarks, deltaR_matched, "dRMatchedJet" )
        self.events["JetGoodMatched"] = matched_jets
        
    def count_partons(self):
        self.events["nparton"] = ak.count(self.events.Parton.pt, axis=1)
        self.events["nparton_matched"] = ak.count(self.events.PartonMatched.pt, axis=1)

    def fill_histograms_extra(self):
        fill_histograms_object(self, self.events.Parton, self.parton_hists)
        # Fill the parton matching 2D plot
        # hmatched = self.get_histogram("hist2d_Njet_Nparton_matched")
        # for category, cuts in self._categories.items():
        #     weight = self._cuts_masks.all(*cuts)
        #     hmatched.fill(sample=self._sample, cat=category, year=self._year, 
        #                   x=self.events.njet, y=self.events.nparton_matched, weight=weight)
        
    def process_extra_after_presel(self) -> ak.Array:
        self.parton_matching()
        self.count_partons()

