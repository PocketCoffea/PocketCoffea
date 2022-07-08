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
        more_histos = {}
        more_histos["hist_ptComparison_parton_matching"] = hist.Hist("hist_parton_matching", self._sample_axis, self._cat_axis,
                                        self._year_axis,
                                        hist.Bin("jet_pt", "Jet pT", 40, 0, 300),
                                        hist.Bin("parton_pt","Parton pt", 40, 0, 300),
                                        hist.Bin("jet_eta","Jet Eta", 5, -2.4, 2.4),
                                        )
        more_histos["hist_deltaR_parton_matching"] = hist.Hist("hist_deltaR_parton_jet", self._sample_axis, self._cat_axis,
                                      self._year_axis,
                                      hist.Bin("deltaR", "deltaR", 50, 0, 0.4))
        #adding the histograms to the accumulator
        self.add_additional_histograms(more_histos)
        
               
    def do_parton_matching(self) -> ak.Array:
        # Selects quarks at LHE level
        isOutgoing = self.events.LHEPart.status == 1
        isB = abs(self.events.LHEPart.pdgId) == 5
        #bquarks = self.events.LHEPart[isB & isOutgoing]
        quarks = self.events.LHEPart[isOutgoing]
        
        # Select b-quarks at Gen level, coming from H->bb decay
        if self.events.metadata["sample"] == 'ttHTobb':
            higgs = self.events.GenPart[(self.events.GenPart.pdgId == 25) & (self.events.GenPart.hasFlags(['fromHardProcess']))]
            higgs =  higgs[ak.num(higgs.childrenIdxG, axis=2) == 2]
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
        self.matched_partons_mask = ~ak.is_none(self.events.JetGoodMatched, axis=1)
        
    def count_partons(self):
        self.events["nparton"] = ak.count(self.events.Parton.pt, axis=1)
        self.events["nparton_matched"] = ak.count(self.events.PartonMatched.pt, axis=1)

    def fill_histograms_extra(self):
        fill_histograms_object(self, self.events.Parton, self.parton_hists)
        # Fill the parton matching 2D plot
        hjetpartontotal = self.get_histogram("hist2d_Njet_Nparton_total")
        hmatched = self.get_histogram("hist2d_Njet_Nparton_matched")
        hppmatched = self.get_histogram("hist2d_Nparton_Nparton_matched")
        for category, cuts in self._categories.items():
            weight = self._cuts_masks.all(*cuts)
            hmatched.fill(sample=self._sample, cat=category, year=self._year, 
                           Njet=self.events.njet, Nparton=self.events.nparton_matched, weight=weight)
            hjetpartontotal.fill(sample=self._sample, cat=category, year=self._year, 
                           Njet=self.events.njet, Nparton=self.events.nparton, weight=weight)
            hppmatched.fill(sample=self._sample, cat=category, year=self._year, 
                            Nparton=self.events.nparton,  Nparton_matched=self.events.nparton_matched, weight=weight)

        hpts = self.get_histogram("hist_ptComparison_parton_matching")
        hdeltaR = self.get_histogram("hist_deltaR_parton_matching")
        # Extracting arrays , they don't depend on the cut
        jet_pt=ak.flatten(self.events.JetGoodMatched.pt[self.matched_partons_mask])
        parton_pt=ak.flatten(self.events.PartonMatched.pt[self.matched_partons_mask])
        jet_eta=ak.flatten(self.events.JetGoodMatched.eta[self.matched_partons_mask])
        delta_R = ak.flatten(self.events.PartonMatched.dRMatchedJet[self.matched_partons_mask])
        for category, cuts in self._categories.items():
            weight = ak.flatten( (ak.ones_like(self.events.JetGoodMatched.pt)[self.matched_partons_mask]) * self._cuts_masks.all(*cuts))
            hpts.fill(sample=self._sample, cat=category, year=self._year, 
                     jet_pt=jet_pt, parton_pt=parton_pt, jet_eta=jet_eta, weight=weight)
            hdeltaR.fill(sample=self._sample, cat=category, year=self._year, 
                      deltaR=delta_R, weight=weight)
        
        
    def process_extra_after_presel(self) -> ak.Array:
        self.do_parton_matching()
        self.count_partons()

