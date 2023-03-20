import sys
import awkward as ak

from .tthbb_base_processor import ttHbbBaseProcessor
from ..lib.hist_manager import Axis


class semileptonicTriggerProcessor(ttHbbBaseProcessor):
    def __init__(self, cfg) -> None:
        super().__init__(cfg=cfg)

        self.output_format["trigger_efficiency"] = {cat: {} for cat in self._categories}

    def define_custom_axes_extra(self):
        # Add 'era' axis for data samples
        if not self._isMC:
            self.custom_axes += [
                Axis(
                    coll="metadata",
                    field="era",
                    name="era",
                    bins=sorted(set(self.cfg.workflow_options["eras"])),
                    type="strcat",
                    growth=False,
                    label="Era",
                )
            ]

    # def postprocess(self, accumulator):
    #    super().postprocess(accumulator=accumulator)

    # den_mc = sum(accumulator["sumw"]["inclusive"].values())
    # den_data = accumulator["cutflow"]["inclusive"]["DATA_SingleMuon"]
    # for category, cuts in self._categories.items():
    # num_mc = sum(accumulator["sumw"][category].values())
    # num_data = accumulator["cutflow"][category]["DATA_SingleMuon"]
    # eff_mc = num_mc / den_mc
    # eff_data = num_data / den_data
    # accumulator["trigger_efficiency"][category] = {}
    # accumulator["trigger_efficiency"][category]["mc"] = eff_mc
    # accumulator["trigger_efficiency"][category]["data"] = eff_data
    # accumulator["trigger_efficiency"][category]["sf"] = eff_data / eff_mc

    # return accumulator
