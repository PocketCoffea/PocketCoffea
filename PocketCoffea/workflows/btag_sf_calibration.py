import sys
import awkward as ak
from coffea import hist
import numba

from .base import ttHbbBaseProcessor
from ..lib.fill import fill_histograms_object
from ..lib.deltaR_matching import object_matching

class BtagSFCalibration(ttHbbBaseProcessor):
    def __init__(self,cfg) -> None:
        super().__init__(cfg=cfg)

    def fill_histograms_extra(self):

        # Compute the Ht of jets
        Ht_jets = ak.sum(abs(self.events.JetGood.pt), axis=1)
        
        # Fill the parton matching 2D plot
        h2_njet_Ht = self.get_histogram("hist2d_Njet_Ht")
        h_Ht = self.get_histogram("hist_Ht")
        for category, cuts in self._categories.items():
            weight = self.get_weight(category) * ak.ones_like(Ht_jets) * self._cuts_masks.all(*cuts)
            h2_njet_Ht.fill(sample=self._sample, cat=category, year=self._year, 
                           Njet=self.events.njet, Ht=Ht_jets,  weight=weight)
            h_Ht.fill(sample=self._sample, cat=category, year=self._year, 
                           Ht=Ht_jets,  weight=weight)
        
        
