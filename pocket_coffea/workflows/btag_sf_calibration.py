import sys
import awkward as ak
from coffea import hist
import numba

from .ttHbb_base_processor import ttHbbBaseProcessor
from ..lib.fill import fill_histograms_object
from ..lib.deltaR_matching import object_matching

class BtagSFCalibration(ttHbbBaseProcessor):
    def __init__(self,cfg) -> None:
        super().__init__(cfg=cfg)


        
        
