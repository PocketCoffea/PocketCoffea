import awkward as ak

from pocket_coffea.workflows.base import BaseProcessorABC


class BasicProcessor(BaseProcessorABC):
    def __init__(self, cfg) -> None:
        super().__init__(cfg=cfg)

    def apply_object_preselection(self, variation):
        self.events["JetGood"]=self.events["Jet"]
        
    def count_objects(self, variation):
        self.events["nJetGood"] = ak.num(self.events.JetGood, axis=1)

    def process_extra_after_presel(self, variation):  # -> ak.Array:
        variation = variation
