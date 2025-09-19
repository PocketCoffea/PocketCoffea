from ..calibrator import Calibrator
import numpy as np
import awkward as ak
import cachetools
from pocket_coffea.lib.jets import jet_correction, met_correction_after_jec, load_jet_factory
from pocket_coffea.lib.leptons import get_ele_scaled, get_ele_smeared

class JetsCalibrator(Calibrator):
    """
    This calibator applies the JEC to the jets and their uncertainties. 
    The set of calibrations to be applied is defined in the parameters file under the 
    `jets_calibration.collection` section.
    All the jet types that have apply_jec_MC or apply_jec_Data set to True will be calibrated.
    If the pT regression is requested for a jet type, it should be done by the JetsPtRegressionCalibrator, 
    this calibrator will raise an exception if configured to apply pT regression.
    """
    
    name = "jet_calibration"
    has_variations = True
    isMC_only = False

    def __init__(self, params, metadata, do_variations, jme_factory, **kwargs):
        super().__init__(params, metadata, do_variations=True, **kwargs)
        self.jme_factory = jme_factory
        self._year = metadata["year"]
        self.jet_calib_param = self.params.jets_calibration
        self.caches = [] 
        self.jets_calibrated = {}
        self.jets_calibrated_types = []
        # It is filled dynamically in the initialize method
        self.calibrated_collections = []

    def initialize(self, events):
        # Load the calibration of each jet type requested by the parameters
        for jet_type, jet_coll_name in self.jet_calib_param.collection[self.year].items():
            # Check if the collection is enables in the parameters
            if self.isMC:
                if (self.jet_calib_param.apply_jec_MC[self.year][jet_type] == False):
                    # If the collection is not enabled, we skip it
                    continue
            else:
                if self.jet_calib_param.apply_jec_Data[self.year][jet_type] == False:
                    # If the collection is not enabled, we skip it
                    continue

            if jet_coll_name in self.jets_calibrated:
                # If the collection is already calibrated with another jet_type, raise an error for misconfiguration
                raise ValueError(f"Jet collection {jet_coll_name} is already calibrated with another jet type. " +
                                 f"Current jet type: {jet_type}. Previous jet types: {self.jets_calibrated[jet_coll_name]}")

            # Check the Pt regression is not requested for this jet type 
            # and in that case send a warning and skim them
            if self.isMC and self.jet_calib_param.apply_pt_regr_MC[self.year][jet_type]:
                print(f"WARNING: Jet type {jet_type} is requested to be calibrated with pT regression: " +
                                    "skipped by JetCalibrator. Please activate the JetsPtRegressionCalibrator.")
                continue
            if not self.isMC and self.jet_calib_param.apply_pt_regr_Data[self.year][jet_type]:
                print(f"WARNING: Jet type {jet_type} is requested to be calibrated with pT regression: " +
                                    "skipped by JetCalibrator. Please activate the JetsPtRegressionCalibrator.")
                continue

            # register the collection as calibrated by this calibrator
            self.calibrated_collections.append(jet_coll_name)

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
        for jet_type in self.jet_calib_param.collection[self.year].keys():
            if jet_type not in self.jets_calibrated_types:
                # If the jet type is not calibrated, we skip it
                continue
            if jet_type in self.jet_calib_param.variations[self.year]:
                # If the jet type has variations, we add them to the list
                # of variations available for this calibrator
                for variation in self.jet_calib_param.variations[self.year][jet_type]:
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
            if jet_type not in self.jet_calib_param.collection[self.year]:
                raise ValueError(f"Jet type {jet_type} not found in the parameters for year {self.year}.")
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
            jet_coll_name = self.jet_calib_param.collection[self.year][jet_type]
            if jet_coll_name not in self.jets_calibrated:
                raise ValueError(f"Jet collection {jet_coll_name} not found in the calibrated jets.")
            # Apply the variation to the jets
            if direction == "up":
                out[jet_coll_name] = self.jets_calibrated[jet_coll_name][variation_type].up
            elif direction == "down":
                out[jet_coll_name] = self.jets_calibrated[jet_coll_name][variation_type].down
            
        return out

class JetsPtRegressionCalibrator(JetsCalibrator):
    """
    This calibrator applied the Pt regression from PNet and UParTAK4 to the jets, before
    applying dedicated JEC calibrators.

    It is a subclass of JetsCalibrator, so it can be used in the same way.
    The jet_calibation.yaml parameters configures the regression and JEC to be applied. 
    Dedicated jet_types are used to distinguish between the different regression algorithms.

    All the jets type with "Regression" in their name will be calibrated by this calibrator, the 
    others will be ignored (and should be calibrated by the JetsCalibrator).

    The do_variations flag is passed to the base class, but it is not used in this calibrator,
    as the variations are always computed by the coffea JEC evaluator.
    """
    name = "jet_calibration_with_pt_regression"
    has_variations = True
    isMC_only = False

    def __init__(self, params, metadata, do_variations, jme_factory, **kwargs):
        super().__init__(params, metadata, do_variations, jme_factory, **kwargs)
        # It is filled dynamically in the initialize method depending on the parameters
        self.calibrated_collections = []
  
    def initialize(self, events):
        # Load the calibration of each jet type requested by the parameters
        for jet_type, jet_coll_name in self.jet_calib_param.collection[self.year].items():
            
            # check if the jet regression is requested, if not skip it
            if self.isMC:
                if not self.jet_calib_param.apply_pt_regr_MC[self.year][jet_type]:
                    # If the collection is not enabled, we skip it
                    continue    
            else:
                if not self.jet_calib_param.apply_pt_regr_Data[self.year][jet_type]:
                    # If the collection is not enabled, we skip it
                    continue

            if jet_coll_name in self.jets_calibrated:
                # If the collection is already calibrated with another jet_type, raise an error for misconfiguration
                raise ValueError(f"Jet collection {jet_coll_name} is already calibrated with another jet type. " +
                                 f"Current jet type: {jet_type}. Previous jet types: {self.jets_calibrated[jet_coll_name]}")
            # Check if the JEC application is requested for this jet type: it should! 
            # in case it is not raise an error as this jets should ne calibated after the regression
            # Check if the collection is enables in the parameters
            if self.isMC and self.jet_calib_param.apply_jec_MC[self.year][jet_type] == False:
                raise ValueError(f"Jet type {jet_type} is requested to be calibrated with Pt regression" + 
                                  " but the JEC application is not configured. Please check the parameters." +
                                   " In case you only want to apply the JEC and not the regression, use JetsCalibrator instead.")
            if not self.isMC and self.jet_calib_param.apply_jec_Data[self.year][jet_type] == False:
                raise ValueError(f"Jet type {jet_type} is requested to be calibrated with Pt regression" + 
                                  " but the JEC application is not configured. Please check the parameters." +
                                   " In case you only want to apply the JEC and not the regression, use JetsCalibrator instead.")

            self.calibrated_collections.append(jet_coll_name)

            regression_params = None
            # Get the regression parameters by collection
            if hasattr(self.params, 'object_preselection') and hasattr(self.params.object_preselection, jet_coll_name):
                jet_params = self.params.object_preselection[jet_coll_name]
                if hasattr(jet_params, 'regression'):
                    regression_params = jet_params.regression

            # Apply the regression to the jets before the JEC
            jets_regressed, reg_mask = self.apply_regression(events[jet_coll_name], jet_type, regression_params)

            cache = cachetools.Cache(np.inf)
            self.caches.append(cache)
            self.jets_calibrated[jet_coll_name] = jet_correction(
                params=self.params,
                events=events,
                jets=jets_regressed[reg_mask],  # passing the regressed jets
                factory=self.jme_factory,
                jet_type=jet_type,
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
        for jet_type in self.jet_calib_param.collection[self.year].keys():
            if jet_type not in self.jets_calibrated_types:
                # If the jet type is not calibrated, we skip it
                continue
            if jet_type in self.jet_calib_param.variations[self.year]:
                # If the jet type has variations, we add them to the list
                # of variations available for this calibrator
                for variation in self.jet_calib_param.variations[self.year][jet_type]:
                    available_jet_variations +=[
                        f"{jet_type}_{variation}Up",
                        f"{jet_type}_{variation}Down"
                    ]
                    # we want to vary independently each jet type
        self._variations = list(sorted(set(available_jet_variations)))  # remove duplicates

    def apply_regression(self, jets, jet_type, regression_params=None):
        """
        Apply PNet regression to jets.
        
        Args:
            jets: Jets collection to apply regression on
            
        Returns:
            Dictionary with calibrated jet collection # TODO: change
        """
        # Apply regression only to specific jet types (AK4PFPuppi, AK4PFchs)
        # This check should ideally be done based on jet type parameter, but for now
        # we assume it's applied to the main Jet collection

        # Flatten jets for easier processing
        j_flat, nj = ak.flatten(jets), ak.num(jets)

        if "PNet" in jet_type:
            # Use PNet regression
            pt_raw_corr='PNetRegPtRawCorr'
            pt_raw_corr_neutrino='PNetRegPtRawCorrNeutrino'
            btag_b='btagPNetB'
            btag_cvl='btagPNetCvL'
            do_plus_neutrino = "PlusNeutrino" in jet_type
        elif "UParTAK4" in jet_type:
            # Use UParTAK4 regression
            pt_raw_corr='UParTAK4RegPtRawCorr'
            pt_raw_corr_neutrino='UParTAK4RegPtRawCorrNeutrino'
            btag_b='btagUParTAK4B'
            btag_cvl='btagUParTAK4CvL'
            do_plus_neutrino = "PlusNeutrino" in jet_type
        else:
            raise ValueError(f"Regression algorithm {jet_type} is not supported."+
                             " Supported algorithms are: PNet, UParTAK4.")

        # Check if required fields exist
        required_fields = ['rawFactor', pt_raw_corr, pt_raw_corr_neutrino, btag_b, btag_cvl]
        missing_fields = [field for field in required_fields if field not in j_flat.fields]

        if missing_fields:
            # If required fields are missing, raise an error
            raise ValueError(f"Missing required fields for regression: {', '.join(missing_fields)}. " +
                             "Please ensure the jets collection contains the necessary fields for regression.")

        # Obtain the regressed PT and Mass
        reg_j_pt = (
            j_flat["pt"]
            * (1 - j_flat["rawFactor"])
            * j_flat[pt_raw_corr]
            * (
                j_flat[pt_raw_corr_neutrino] if do_plus_neutrino else 1
            )
        )
        reg_j_mass = (
            j_flat["mass"]
            * (1 - j_flat["rawFactor"])
            * j_flat[pt_raw_corr]
            * (
                j_flat[pt_raw_corr_neutrino] if do_plus_neutrino else 1
            )
        )

        if regression_params is None:
            reg_mask = ak.ones_like(reg_j_pt, dtype=bool)
        else:   
            # If regression parameters are provided, we use them
            # to select the jets to apply the regression to
            cut_btagB=getattr(regression_params, 'cut_btagB', -1.)
            cut_btagCvL=getattr(regression_params, 'cut_btagCvL', -1.)
        
            # Select which jets to apply the regression to (cuts are provided via parameter yaml)
            reg_mask = (j_flat[btag_b] >= cut_btagB) | (j_flat[btag_cvl] >= cut_btagCvL)

        # Apply regression only to jets where the regression is not 0
        reg_mask = (reg_mask) & (j_flat[pt_raw_corr] != 0) & (j_flat[pt_raw_corr_neutrino] != 0)

        # WARNING: Keeping both regressed and not regressed jets
        # can lead to issues if the regression is applied on only
        # part of the jets because the JEC should be applied ONLY
        # to the jets that have the regression applied.
        # This is why we throw away the jets that do not have the regression applied

        # An alternative is to apply the regression only to the jets that have the regression applied
        # but then the JEC would be wrong for these jets
        # Apply regression where mask is True, keep original values otherwise
        # new_j_pt_flat = ak.where(reg_mask, reg_j_pt, j_flat['pt'])
        new_j_pt_flat = ak.mask(reg_j_pt, reg_mask)
        new_j_pt = ak.unflatten(new_j_pt_flat, nj)

        # new_j_mass_flat = ak.where(reg_mask, reg_j_mass, j_flat['mass'])
        new_j_mass_flat = ak.mask(reg_j_mass, reg_mask)
        new_j_mass = ak.unflatten(new_j_mass_flat, nj)

        # Update the raw factor to 0 for the jets where regression is applied
        # because the regressed pt is the new pt raw
        # new_raw_factor_flat = ak.where(reg_mask, 0, j_flat['rawFactor'])
        new_raw_factor_flat = ak.mask(ak.zeros_like(j_flat['rawFactor']), reg_mask)
        new_raw_factor = ak.unflatten(new_raw_factor_flat, nj)

        # Replace the PT and Mass variables in the original jets collection
        reg_mask_unflatten=ak.unflatten(reg_mask, nj)
        jets_regressed=ak.mask(jets, reg_mask_unflatten)
        jets_regressed = ak.with_field(jets_regressed, new_j_pt, 'pt')
        jets_regressed = ak.with_field(jets_regressed, new_j_mass, 'mass')
        jets_regressed = ak.with_field(jets_regressed, new_raw_factor, 'rawFactor')

        return jets_regressed, reg_mask_unflatten


###########################################
class METCalibrator(Calibrator):

    name = "met_rescaling"
    has_variations = False
    isMC_only = False
    '''
    The MET calibrator applies the JEC to the MET collection.'''
    def __init__(self, params, metadata, do_variations=True, **kwargs):
        super().__init__(params, metadata, do_variations, **kwargs)
        jet_calib_param = self.params.jets_calibration
        self.met_calib_cfg = jet_calib_param.rescale_MET_config[self.year]
        self.met_calib_active = self.met_calib_cfg.apply
        self.met_branch = self.met_calib_cfg.MET_collection
        self.calibrated_collections = [f"{self.met_branch}.pt", f"{self.met_branch}.phi"]
        self.jet_collection = self.met_calib_cfg.Jet_collection
       
    def initialize(self, events):
        pass

    def calibrate(self, events, orig_colls, variation, already_applied_calibrators=None):
        '''The MET calibrator applies the difference from the uncalibrated Jets and the calibrated Jets after JEC to the MET collection.
        In case the Jets in the nano are already calibrated, the delta will be 0 and the MET will not be changed.'''
        if not self.met_calib_active:
            return {}
        # we can check if the Jets calibrator has been applied
        if "jet_calibration" not in already_applied_calibrators:
            raise ValueError("Jets calibrator must be applied before the MET calibrator.")
        if self.jet_collection not in orig_colls:
            # this means that the jets calibration has been skipped
            # we just return the MET as is
            return {}
        
        # Get the uncalibrated and calibrated jets
        uncalibrated_jets = orig_colls[self.jet_collection]
        calibrated_jets = events[self.jet_collection]
        new_MET = met_correction_after_jec(events, self.met_branch, uncalibrated_jets, calibrated_jets)
        # Return the new MET collection
        return {f"{self.met_branch}.pt" : new_MET["pt"],
                f"{self.met_branch}.phi" : new_MET["phi"]}

##############################################
class ElectronsScaleCalibrator(Calibrator):

    name = "electron_scale_and_smearing"
    has_variations = True
    isMC_only = False
    calibrated_collections = ["Electron.pt", "Electron.pt_original"]

    def __init__(self, params, metadata, do_variations=True, **kwargs):
        super().__init__(params, metadata, do_variations, **kwargs)
        self.ss_params = self.params.lepton_scale_factors.electron_sf.scale_and_smearing
        if not self.ss_params.apply[self.year]:
            # If the scale and smearing is not applied, we do not need to initialize the calibrator
            self.ssfile = None
            self._variations = []
            self.calibrated_collections = []
            self.enabled = False
            return
        self.enabled = True
        self.ssfile = self.ss_params.correctionlib_config[self.year]["file"]
        self.correction_name = self.ss_params.correctionlib_config[self.year]["correction_name"]
        if self.isMC:
            self._variations = ["ele_scaleUp", "ele_scaleDown", "ele_smearUp", "ele_smearDown"]
        else:
            self._variations = []
        

    def initialize(self, events):
        # initialize the calibrator
        if not self.enabled:
            return
        seed = abs(hash(events.metadata['fileuuid'])+events.metadata['entrystart'])
        self.electrons = ak.with_field(events.Electron,
                                       events["Electron"]["deltaEtaSC"] + events["Electron"]["eta"],
                                       "etaSC")
        self.electrons = ak.with_field(self.electrons, self.electrons["pt"], "pt_original")
        if self.isMC:
            # If the events are MC, we apply smearing
            self.smeared_pt = get_ele_smeared(self.electrons, self.ssfile, self.correction_name.smear,
                                              isMC=True, only_nominal=False, seed=seed)
            # Also get the scale variations, without scaling the nominal
            self.scaled_pt = get_ele_scaled(self.electrons, self.ssfile, self.correction_name.scale,
                                            isMC=True, runNr=events["run"])
            # TODO: check what happens with the run number and MC
        else:
            # If the events are data, we apply only scaling
            self.scaled_pt = get_ele_scaled(self.electrons, self.ssfile, self.correction_name.scale,
                                            isMC=False, runNr=events["run"])


    def calibrate(self, events, orig_colls, variation, already_applied_calibrators=None):
        if not self.enabled:
            return {}
        if self.isMC:
            if variation == "nominal" or variation not in self._variations:
                # If the variation is nominal or not handled by this calibrator
                # we return the original pt
                return {"Electron.pt": self.smeared_pt["nominal"],
                        "Electron.pt_original": self.electrons["pt_original"]}
            elif variation == "ele_scaleUp":
                return {"Electron.pt": self.scaled_pt["up"],
                        "Electron.pt_original": self.electrons["pt_original"]}
            elif variation == "ele_scaleDown":
                return {"Electron.pt": self.scaled_pt["down"],
                        "Electron.pt_original": self.electrons["pt_original"]}
            elif variation == "ele_smearUp":
                return {"Electron.pt": self.smeared_pt["up"],
                        "Electron.pt_original": self.electrons["pt_original"]}
            elif variation == "ele_smearDown":
                return {"Electron.pt": self.smeared_pt["down"],
                        "Electron.pt_original": self.electrons["pt_original"]}
        else:
            # If the events are data, we do not apply smearing
            return {"Electron.pt": self.scaled_pt["nominal"],
                    "Electron.pt_original": self.electrons["pt_original"]}

#####################################################
class MuonsCalibrator(Calibrator):
    def __init__(self, params, metadata, do_variations=True, **kwargs):
        super().__init__(params, metadata, do_variations, **kwargs)
        # initialize variations

    def initialize(self, events):
        # initialize the calibrator
        pass

    def calibrate( events, orig_colls, variation, already_applied_calibrators=None):
        pass

#########################################
default_calibrators_sequence = [
    JetsCalibrator, METCalibrator, ElectronsScaleCalibrator
]