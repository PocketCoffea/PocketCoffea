import sys
import awkward as ak
from coffea import hist
import numba

from .base import ttHbbBaseProcessor
from ..lib.fill import fill_column_accumulator
from ..lib.deltaR_matching import object_matching

class PartonMatchingProcessor(ttHbbBaseProcessor):
    def __init__(self,cfg) -> None:
        super().__init__(cfg=cfg)
        self.dr_min = self.cfg.workflow_extra_options["parton_jet_min_dR"]


        # Defining the column accumulators for parton and jet pts,
        self.add_column_accumulator("parton_pt", categories=['4j'], store_size=True)
        self.add_column_accumulator("parton_pdgId", categories=['4j'], store_size=True)
        self.add_column_accumulator("jet_pt", categories=['4j'], store_size=True)
        self.add_column_accumulator("jet_eta", categories=['4j'], store_size=True)
        self.add_column_accumulator("parton_jet_dR", categories=['4j'],store_size=True)
        self.add_column_accumulator("jet_btag", categories=['4j'], store_size=True)
               
    def do_parton_matching(self) -> ak.Array:
        # Selects quarks at LHE level
        isOutgoing = self.events.LHEPart.status == 1
        isParton = (abs(self.events.LHEPart.pdgId) < 6) | \
                           (self.events.LHEPart.pdgId == 21) 
        quarks = self.events.LHEPart[isOutgoing & isParton]
        
        # Select b-quarks at Gen level, coming from H->bb decay
        if self.events.metadata["sample"] == 'ttHTobb':
            higgs = self.events.GenPart[(self.events.GenPart.pdgId == 25) & (self.events.GenPart.hasFlags(['fromHardProcess']))]
            higgs =  higgs[ak.num(higgs.childrenIdxG, axis=2) == 2]
            higgs_partons = ak.with_field(ak.flatten(higgs.children, axis=2), 25, "from_part")
            quarks = ak.concatenate( (quarks, higgs_partons) , axis=1 )
            # Sort b-quarks by pt
            quarks = ak.with_name(quarks[ak.argsort(quarks.pt, ascending=False)], name='PtEtaPhiMCandidate')

        # Calling our general object_matching function.
        # The output is an awkward array with the shape of the second argument and None where there is no matching.
        # So, calling like this, we will get out an array of matched_quarks with the dimension of the JetGood. 
        matched_quarks, matched_jets, deltaR_matched = object_matching(quarks, self.events.JetGood, dr_min=self.dr_min)
        
        self.events["Parton"] = quarks
        self.events["PartonMatched"] = ak.with_field(matched_quarks, deltaR_matched, "dRMatchedJet" )
        self.events["JetGoodMatched"] = matched_jets
        self.matched_partons_mask = ~ak.is_none(self.events.JetGoodMatched, axis=1)
        
    def count_partons(self):
        self.events["nParton"] = ak.count(self.events.Parton.pt, axis=1)
        self.events["nPartonMatched"] = ak.count(self.events.PartonMatched.pt, axis=1)

        
    def process_extra_after_presel(self) -> ak.Array:
        self.do_parton_matching()
        self.count_partons()

        
        fill_column_accumulator(self.output["columns"], self._sample, "parton_pt", ["4j"],
                                self.events.PartonMatched.pt,
                                save_size = True)
        
        fill_column_accumulator(self.output["columns"], self._sample, "parton_pdgId", ["4j"],
                                self.events.PartonMatched.pdgId,
                                save_size = True)

        fill_column_accumulator(self.output["columns"], self._sample, "jet_pt", ["4j"],
                                self.events.JetGoodMatched.pt,
                                save_size = True)

        fill_column_accumulator(self.output["columns"], self._sample, "jet_eta", ["4j"],
                                self.events.JetGoodMatched.eta,
                                save_size = True)
        
        fill_column_accumulator(self.output["columns"], self._sample, "parton_jet_dR", ["4j"],
                                self.events.PartonMatched.dRMatchedJet,
                                save_size = True)

        fill_column_accumulator(self.output["columns"], self._sample, "jet_btag", ["4j"],
                                self.events.JetGoodMatched.btagDeepFlavB,
                                save_size = True)


