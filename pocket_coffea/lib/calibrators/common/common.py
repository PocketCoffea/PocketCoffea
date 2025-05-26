from ..calibrator import Calibrator
import numpy as np
import awkward as ak
import cachetools
from pocket_coffea.lib.jets import jet_correction, met_correction_after_jec, load_jet_factory

class JetsCalibrator(Calibrator):
    def __init__(self, params, metadata, jme_factory, **kwargs):
        super().__init__(params, metadata)
        self.jme_factory = jme_factory
        self.jet_calib_param = self.params.jets_calibration
        self.caches = [] 
        self._year = self.metadata["year"]
        self.jets_calibrated = {}

    def initialize(self, events):
        # Load the calibration of each jet type requested by the parameters
        for jet_type, jet_coll_name in self.jet_calib_param.collection[self._year].items():
            cache = cachetools.Cache(np.inf)
            self.caches.append(cache)
            self.jets_calibrated[jet_coll_name] = jet_correction(
                params=self.params,
                events=events,
                jets=events[jet_coll_name],
                factory=self.jme_factory,
                jet_type = jet_type,
                chunk_metadata={
                    "year": self.metadata["year"],
                    "isMC": self.metadata["isMC"],
                    "era": self.metadata["era"] if "era" in self.metadata else None,
                },
                cache=cache
            )
        # Prepare the list of available variations
        # For this we just read from the parameters
        available_jet_variations = []
        for jet_type in self.jet_calib_param.collection[self._year].keys():
            if jet_type in self.jet_calib_param.variations[self._year]:
                # If the jet type has variations, we add them to the list
                # of variations available for this calibrator
                for variation in self.jet_calib_param.variations[self._year][jet_type]:
                    available_jet_variations +=[
                        f"{jet_type}_{variation}Up",
                        f"{jet_type}_{variation}Down"
                    ]
                    # we want to vary independently each jet type
        self._variations = list(sorted(set(available_jet_variations)))  # remove duplicates

    def calibrate(self, events, orig_colls, variation):
        # The values have been already calculated in the initialize method
        # We just need to apply the corrections to the events
        out = {}
        if variation == "nominal" or variation not in self._variations:
            # If the variation is nominal or not in the list of variations, we return the nominal values
            for jet_coll_name, jets in self.jets_calibrated.items():
                out[jet_coll_name] = jets
        else:
            # get the jet type from the variation name
            jet_type = variation.split("_")[0]
            if jet_type not in self.jet_calib_param.collection[self._year]:
                raise ValueError(f"Jet type {jet_type} not found in the parameters for year {self._year}.")
            # get the variation type from the variation name
            if variation.endswith("Up"):
                variation_type = variation.split("_")[1][:-2]  # remove 'Up'
                direction = "up"
            elif variation.endswith("Down"):
                variation_type = variation.split("_")[1][:-4]  # remove 'Down'
                direction = "down"
            else:
                raise ValueError(f"JET Variation {variation} is not recognized. It should end with 'Up' or 'Down'.")
           
            # get the jet collection name from the parameters
            jet_coll_name = self.jet_calib_param.collection[self._year][jet_type]
            if jet_coll_name not in self.jets_calibrated:
                raise ValueError(f"Jet collection {jet_coll_name} not found in the calibrated jets.")
            # Apply the variation to the jets
            if direction == "up":
                out[jet_coll_name] = self.jets_calibrated[jet_coll_name][variation_type].up
            elif direction == "down":
                out[jet_coll_name] = self.jets_calibrated[jet_coll_name][variation_type].down
            
        return out


class METCalibrator(Calibrator):
    def __init__(self, params, metadata):
        super().__init__(params, metadata)
        # initialize variations

    def initialize(self, events):
        # initialize the calibrator
        pass

    def calibrate(self, events, orig_colls, variation):
        pass


class ElectronsCalibrator(Calibrator):
    def __init__(self, params, metadata):
        super().__init__(params, metadata)
        # initialize variations

    def initialize(self, events):
        # initialize the calibrator
        pass

    def calibrate(self, events, variation):
        pass

class MuonsCalibrator(Calibrator):
    def __init__(self, params, metadata):
        super().__init__(params, metadata)
        # initialize variations

    def initialize(self, events):
        # initialize the calibrator
        pass

    def calibrate(self, events, variation):
        pass


default_calibrators_sequence = [
]