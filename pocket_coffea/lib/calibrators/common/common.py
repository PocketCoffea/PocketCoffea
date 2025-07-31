from ..calibrator import Calibrator
import numpy as np
import awkward as ak
import cachetools
from pocket_coffea.lib.jets import jet_correction, met_correction_after_jec, load_jet_factory
from .pnet_regression import PNetRegressionCalibrator

class JetsCalibrator(Calibrator):
    """
    """
    name = "jet_calibration"
    has_variations = True
    isMC_only = False
    calibrated_collections = ["Jet", "FatJet"]


    def __init__(self, params, metadata, jme_factory, **kwargs):
        super().__init__(params, metadata)
        self.jme_factory = jme_factory
        self.jet_calib_param = self.params.jets_calibration
        self.caches = [] 
        self._year = self.metadata["year"]
        self.jets_calibrated = {}
        self.jets_calibrated_types = [ ]

    def initialize(self, events):
        # Load the calibration of each jet type requested by the parameters
        for jet_type, jet_coll_name in self.jet_calib_param.collection[self._year].items():
            # Check if the collection is enables in the parameters
            if self.metadata["isMC"]:
                if self.jet_calib_param.apply_jec_MC[self._year][jet_type] == False:
                    # If the collection is not enabled, we skip it
                    continue
            else:
                if self.jet_calib_param.apply_jec_Data[self._year][jet_type] == False:
                    # If the collection is not enabled, we skip it
                    continue
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
            # Add to the list of the types calibrated
            self.jets_calibrated_types.append(jet_type)

        # Prepare the list of available variations
        # For this we just read from the parameters
        available_jet_variations = []
        for jet_type in self.jet_calib_param.collection[self._year].keys():
            if jet_type not in self.jets_calibrated_types:
                # If the jet type is not calibrated, we skip it
                continue
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


    def calibrate(self, events, orig_colls, variation, already_applied_calibrators=None):
        # The values have been already calculated in the initialize method
        # We just need to apply the corrections to the events
        out = {}
        if variation == "nominal" or variation not in self._variations:
            # If the variation is nominal or not in the list of variations, we return the nominal values
            for jet_coll_name, jets in self.jets_calibrated.items():
                out[jet_coll_name] = jets
        else:
            # get the jet type from the variation name
            variation_parts = variation.split("_")
            jet_type = variation_parts[0]
            if jet_type not in self.jet_calib_param.collection[self._year]:
                raise ValueError(f"Jet type {jet_type} not found in the parameters for year {self._year}.")
            # get the variation type from the variation name
            if variation.endswith("Up"):
                variation_type = "_".join(variation_parts[1:])[:-2]  # remove 'Up'
                direction = "up"
            elif variation.endswith("Down"):
                variation_type = "_".join(variation_parts[1:])[:-4]  # remove 'Down'
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

###########################################
class METCalibrator(Calibrator):

    name = "met_rescaling"
    has_variations = False
    isMC_only = False
    '''
    The MET calibrator applies the JEC to the MET collection.'''
    def __init__(self, params, metadata, **kwargs):
        super().__init__(params, metadata)
        jet_calib_param = params.jets_calibration
        self.met_calib_active = jet_calib_param.rescale_MET[metadata["year"]]
        self.met_branch = jet_calib_param.rescale_MET_branch[metadata["year"]]
        self.calibrated_collections = [f"{self.met_branch}.pt", f"{self.met_branch}.phi"]

    def initialize(self, events):
        pass

    def calibrate(self, events, orig_colls, variation, already_applied_calibrators=None):
        '''The MET calibrator applies the difference from the uncalibrated Jets and the calibrated Jets after JEC to the MET collection.
        In case the Jets in the nano are already calibrated, the delta will be 0 and the MET will not be changed.'''
        # we can check if the Jets calibrator has been applied
        if "jet_calibration" not in already_applied_calibrators:
            raise ValueError("Jets calibrator must be applied before the MET calibrator.")
        if "Jet" not in orig_colls:
            # this means that the jets calibration has been skipped
            # we just return the MET as is
            return {}
        
        # Get the uncalibrated and calibrated jets
        uncalibrated_jets = orig_colls["Jet"]
        calibrated_jets = events["Jet"]
        new_MET = met_correction_after_jec(events, self.met_branch, uncalibrated_jets, calibrated_jets)
        print("Old met:", events[self.met_branch]["pt"])
        # Return the new MET collection
        print("New met:", new_MET["pt"])
        return {f"{self.met_branch}.pt" : new_MET["pt"],
                f"{self.met_branch}.phi" : new_MET["phi"]}

##############################################
class ElectronsScaleCalibrator(Calibrator):
    def __init__(self, params, metadata):
        super().__init__(params, metadata, **kwargs)
        # initialize variations

    def initialize(self, events):
        # initialize the calibrator
        pass

    def calibrate(self, events, variation):
        pass

class MuonsCalibrator(Calibrator):
    def __init__(self, params, metadata, **kwargs):
        super().__init__(params, metadata, **kwargs)
        # initialize variations

    def initialize(self, events):
        # initialize the calibrator
        pass

    def calibrate(self, events, variation):
        pass

#########################################3
default_calibrators_sequence = [
    JetsCalibrator, METCalibrator
]