import awkward as ak

from pocket_coffea.workflows.base import BaseProcessorABC


class PtRegrProcessor(BaseProcessorABC):
    def __init__(self, cfg) -> None:
        super().__init__(cfg=cfg)

    def process_extra_after_skim(self):
        # Create extra Jet collections for testing
        self.events["JetPtReg"] = ak.copy(self.events["Jet"])
        self.events["JetPtRegPlusNeutrino"] = ak.copy(self.events["Jet"])

    def apply_object_preselection(self, variation):
        self.events["JetGood"]=self.events["Jet"]
        
    def count_objects(self, variation):
        self.events["nJetGood"] = ak.num(self.events.JetGood, axis=1)

