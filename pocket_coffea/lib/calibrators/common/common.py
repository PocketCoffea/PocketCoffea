from ..calibrator import Calibrator
import numpy as np
import awkward as ak
import cachetools
from pocket_coffea.lib.jets import met_correction_after_jec, jet_correction_corrlib, msoftdrop_correction
from pocket_coffea.lib.leptons import (
    get_ele_scaled, 
    get_ele_smeared, 
    get_ele_scaled_etdependent, 
    get_ele_smeared_etdependent
    )
from pocket_coffea.utils.utils import get_random_seed
import copy
from omegaconf import OmegaConf
from pocket_coffea.lib.muon_scale_and_resolution import pt_scale, pt_resol, pt_scale_var, pt_resol_var
import correctionlib

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

    def __init__(self, params, metadata, do_variations, **kwargs):
        super().__init__(params, metadata, do_variations, **kwargs)
        self._year = metadata["year"]
        self.jet_calib_param = self.params.jets_calibration
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

            
            # Check if the pt regression is requested, if not skip it
            if ((self.isMC and self.jet_calib_param.apply_pt_regr_MC[self.year][jet_type]) 
                    or
               (not self.isMC and self.jet_calib_param.apply_pt_regr_Data[self.year][jet_type])):
                # Get the regression parameters by collection if they are present
                regression_params = OmegaConf.select(self.params,
                                                     "object_preselection." + jet_coll_name + ".regression")
                
                # Apply the regression to the jets before the JEC
                # I'm not 100% sure a softcopy is needed here, but the apply_regression method modifies the jets
                # in place, so to be safe we make a copy. This is a soft copy, so the array data is not copied.
                # This just makes sure that the original events[jet_coll_name] is not modified.
                jets_regressed, reg_mask = self.apply_regression(copy.copy(events[jet_coll_name]), 
                                                                 jet_type, regression_params)
                # replacing the collecation in place, so that the JEC is applied to the regressed jets
                events[jet_coll_name] = jets_regressed[reg_mask]


            # register the collection as calibrated by this calibrator
            self.calibrated_collections.append(jet_coll_name)

            corrected_jets = jet_correction_corrlib(
                calib_params=self.jet_calib_param.jet_types[jet_type][self._year],
                variations=self.jet_calib_param.variations[jet_type][self._year],
                events=events,
                jet_type = jet_type,
                jet_coll_name=jet_coll_name,
                chunk_metadata={
                    "year": self._year,
                    "isMC": self.metadata["isMC"],
                    "era": self.metadata["era"] if "era" in self.metadata else None,
                },
                jec_syst=self.do_variations,
                apply_jer=self.jet_calib_param.apply_jer_MC[self.year][jet_type] if self.isMC else False,
            )
            # update the rawFactor of the corrected jets
            self.jets_calibrated[jet_coll_name] = ak.with_field(corrected_jets, 
                                                                1 - corrected_jets.pt_raw / corrected_jets.pt, 
                                                                "rawFactor")
            # Add to the list of the types calibrated
            self.jets_calibrated_types.append(jet_type)

        # Prepare the list of available variations
        # For this we just read from the parameters
        available_jet_variations = []
        for jet_type in self.jet_calib_param.collection[self.year].keys():
            if jet_type not in self.jets_calibrated_types:
                # If the jet type is not calibrated, we skip it
                continue
            if jet_type in self.jet_calib_param.variations:
                if self.year not in self.jet_calib_param.variations[jet_type]:
                    continue
                # If the jet type has variations, we add them to the list
                # of variations available for this calibrator
                for variation in self.jet_calib_param.variations[jet_type][self.year]:
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
        elif "UParTAK4V1" in jet_type:
            # Use UParTAK4V1 regression
            pt_raw_corr='UParTAK4V1RegPtRawCorr'
            pt_raw_corr_neutrino='UParTAK4V1RegPtRawCorrNeutrino'
            btag_b='btagUParTAK4B'
            btag_cvl='btagUParTAK4CvL'
            do_plus_neutrino = "PlusNeutrino" in jet_typec
        elif "UParTAK4" in jet_type:
            # Use UParTAK4 regression
            pt_raw_corr='UParTAK4RegPtRawCorr'
            pt_raw_corr_neutrino='UParTAK4RegPtRawCorrNeutrino'
            btag_b='btagUParTAK4B'
            btag_cvl='btagUParTAK4CvL'
            do_plus_neutrino = "PlusNeutrino" in jet_type
        else:
            raise ValueError(f"Regression algorithm {jet_type} is not supported."+
                             " Supported algorithms are: PNet, UParTAK4, UParTAK4V1.")

        # Check if required fields exist
        required_fields = ['rawFactor', pt_raw_corr, pt_raw_corr_neutrino, btag_b, btag_cvl]
        missing_fields = [field for field in required_fields if field not in j_flat.fields]

        if missing_fields:
            # If required fields are missing, raise an error
            raise ValueError(f"Missing required fields for regression: {', '.join(missing_fields)}. " +
                             "Please ensure the jets collection contains the necessary fields for regression.")

        # Get the regression factor
        if "PNet" in jet_type:
            reg_j_factor = j_flat[pt_raw_corr]
            if do_plus_neutrino:
                reg_j_factor = reg_j_factor * j_flat[pt_raw_corr_neutrino]
        elif "UParTAK4" in jet_type:
            if do_plus_neutrino:
                reg_j_factor = j_flat[pt_raw_corr_neutrino]
            else:                
                reg_j_factor = j_flat[pt_raw_corr]

        # Obtain the regressed PT and Mass
        reg_j_pt = (
            j_flat["pt"]
            * (1 - j_flat["rawFactor"])
            * reg_j_factor
        )
        reg_j_mass = (
            j_flat["mass"]
            * (1 - j_flat["rawFactor"])
            * reg_j_factor
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

        new_j_pt_flat = ak.mask(reg_j_pt, reg_mask)
        new_j_pt = ak.unflatten(new_j_pt_flat, nj)

        new_j_mass_flat = ak.mask(reg_j_mass, reg_mask)
        new_j_mass = ak.unflatten(new_j_mass_flat, nj)

        # Update the raw factor to 0 for the jets where regression is applied
        # because the REGRESSED PT IS THE NEW PT RAW of the jet_regressed collection
        new_raw_factor_flat = ak.mask(ak.zeros_like(j_flat['rawFactor']), reg_mask)
        new_raw_factor = ak.unflatten(new_raw_factor_flat, nj)

        # Replace the PT and Mass variables in the original jets collection
        reg_mask_unflatten = ak.unflatten(reg_mask, nj)
        jets_regressed = ak.mask(jets, reg_mask_unflatten)
        jets_regressed = ak.with_field(jets_regressed, new_j_pt, 'pt')
        jets_regressed = ak.with_field(jets_regressed, new_j_mass, 'mass')
        jets_regressed = ak.with_field(jets_regressed, new_raw_factor, 'rawFactor')

        return jets_regressed, reg_mask_unflatten

    def calibrate(self, events, orig_colls, variation, already_applied_calibrators=None):
        # The values have been already calculated in the initialize method
        # We just need to apply the corrections to the events
        out = {}
        for jet_coll_name, jets in self.jets_calibrated.items():
            # Creating a soft copy of the jets to avoid modifying the original one
            # stored in the calibrator when replacing the pt correctly.
            # In practice this is not using more memory, it is just making sure that changes of
            # pointers in the out dict do not affect the calibrator internal state.
            # N.B: we don't just replace the pt and mass in the jets from events
            # because we want to use the collection initialized in the calibrator. 
            out[jet_coll_name] = copy.copy(jets)

        if variation == "nominal" or variation not in self._variations:
            # For nominal and unrelated variation return the nominal
            # If the variation is nominal or not in the list of variations, we return the nominal values
            return out
        
        # Otherwise, adapt pt and mass according to calibrations:
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
            out[jet_coll_name]["pt"] = self.jets_calibrated[jet_coll_name][f"pt_{variation_type}_up"]
            out[jet_coll_name]["mass"] = self.jets_calibrated[jet_coll_name][f"mass_{variation_type}_up"]
        elif direction == "down":
            # print(type(out[jet_coll_name]["pt"]))
            out[jet_coll_name]["pt"] = self.jets_calibrated[jet_coll_name][f"pt_{variation_type}_down"]
            out[jet_coll_name]["mass"] = self.jets_calibrated[jet_coll_name][f"mass_{variation_type}_down"]

        # Need to reorder the jet collection by pt after the variation
        if self.jet_calib_param.sort_by_pt[self._year][jet_type]:
            sorted_indices = ak.argsort(out[jet_coll_name]["pt"], axis=1, ascending=False)
            out[jet_coll_name] = out[jet_coll_name][sorted_indices]
   
        return out



class JetsSoftdropMassCalibrator(Calibrator):
    """
    This calibator applies the JEC to the softdrop mass of AK8 jets.
    The set of calibrations to be applied is defined in the parameters file under the 
    `jets_calibration.collection` section.
    All the jet types that have apply_jec_MC or apply_jec_Data set to True will be calibrated.
    If the pT regression is requested for a jet type, it should be done by the JetsPtRegressionCalibrator, 
    this calibrator will raise an exception if configured to apply pT regression.
    """
    
    name = "msoftdrop_calibration"
    has_variations = True
    isMC_only = False

    def __init__(self, params, metadata, do_variations, **kwargs):
        super().__init__(params, metadata, do_variations, **kwargs)
        self._year = metadata["year"]
        self.jet_calib_param = self.params.jets_calibration
        self.jets_calibrated = {}
        self.jets_calibrated_types = []
        # It is filled dynamically in the initialize method
        self.calibrated_collections = []

    def initialize(self, events):

        # Load the calibration of each jet type requested by the parameters
        for jet_type, jet_coll_name in self.jet_calib_param.collection[self.year].items():
            # Calibrate only AK8 jets
            if jet_type in ["AK8PFPuppi"]:
                # Define the subjet type for the correction of subjets
                if self.year in ["2016_preVFP", "2016_postVFP", "2017", "2018"]:
                    subjet_type = "AK4PFchs"
                else:
                    subjet_type = "AK4PFPuppi"
            else:
                print("WARNING: JetsSoftdropMassCalibrator only supports AK8PFPuppi jets for softdrop mass calibration." +
                      f" Jet type {jet_type} will be skipped.")
                continue


            # Check if the collection is enables in the parameters
            if self.isMC:
                if (self.jet_calib_param.apply_jec_msoftdrop_MC[self.year][jet_type] == False):
                    # If the collection is not enabled, we skip it
                    continue
            else:
                if self.jet_calib_param.apply_jec_msoftdrop_Data[self.year][jet_type] == False:
                    # If the collection is not enabled, we skip it
                    continue

            # register the collection as calibrated by this calibrator
            self.calibrated_collections.append(jet_coll_name)

            # N.B.: since the correction is applied to the subjets of the AK8 jet, the parameters for AK4 jets are passed
            self.jets_calibrated[jet_coll_name] = msoftdrop_correction(
                calib_params=self.jet_calib_param.jet_types[subjet_type][self._year],
                variations=self.jet_calib_param.variations[subjet_type][self._year],
                events=events,
                subjet_type = subjet_type,   # AK4PFPuppi (approximation: the subjets are corrected with AK4 jet corrections)
                jet_coll_name=jet_coll_name, # AK8PFPuppi
                chunk_metadata={
                    "year": self._year,
                    "isMC": self.metadata["isMC"],
                    "era": self.metadata["era"] if "era" in self.metadata else None,
                },
                jec_syst=self.do_variations
            )
            # Add to the list of the types calibrated
            self.jets_calibrated_types.append(jet_type)

        assert len(self.jets_calibrated_types) > 0, "No jet types were calibrated in JetsSoftdropMassCalibrator. Please check the configuration."

        # Prepare the list of available variations
        # For this we just read from the parameters
        available_jet_variations = []

        # N.B.: JES variations are not yet implemented for msoftdrop
        #for jet_type in self.jet_calib_param.collection[self.year].keys():
        #    if jet_type in ["AK8PFPuppi"]:
        #        # Define the subjet type for the correction of subjets
        #        if self.year in ["2016_preVFP", "2016_postVFP", "2017", "2018"]:
        #            subjet_type = "AK4PFchs"
        #        else:
        #            subjet_type = "AK4PFPuppi"
        #    else:
        #        continue
        #    if subjet_type not in self.jets_calibrated_types:
        #        # If the jet type is not calibrated, we skip it
        #        continue
        #    if subjet_type in self.jet_calib_param.variations:
        #        if self.year not in self.jet_calib_param.variations[jet_type]:
        #            continue
        #        # If the jet type has variations, we add them to the list
        #        # of variations available for this calibrator
        #        for variation in self.jet_calib_param.variations[jet_type][self.year]:
        #            if "JER" in variation:
        #                # Softdrop mass variations do not include JER variations
        #                continue
        #            available_jet_variations +=[
        #                f"{subjet_type}_{variation}Up",
        #                f"{subjet_type}_{variation}Down"
        #            ]
        #            # we want to vary independently each jet type
        self._variations = list(sorted(set(available_jet_variations)))  # remove duplicates


    def calibrate(self, events, orig_colls, variation, already_applied_calibrators=None):
        # The values have been already calculated in the initialize method
        # We just need to apply the corrections to the events
        out = {}
        for jet_coll_name, jets in self.jets_calibrated.items():
            if not jet_coll_name == "FatJet":
                continue
            # Creating a soft copy of the jets to avoid modifying the original one 
            # stored in the calibrator when replaing the pt correctly
            # N.B: we don't just replace the pt and mass in the jets from events
            # because we want to use the collection initialized in the calibrator. 
            out[jet_coll_name] = copy.copy(jets)

        if variation == "nominal" or variation not in self._variations:
            # For nominal and unrelated variation return the nominal
            # If the variation is nominal or not in the list of variations, we return the nominal values
            return out
        
        # Otherwise, adapt pt and mass according to calibrations:
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
            out[jet_coll_name]["msoftdrop"] = self.jets_calibrated[jet_coll_name][f"msoftdrop_{variation_type}_up"]
        elif direction == "down":
            out[jet_coll_name]["msoftdrop"] = self.jets_calibrated[jet_coll_name][f"msoftdrop_{variation_type}_down"]
   
        return out

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
        if ("jet_calibration" not in already_applied_calibrators) and ("jet_calibration_corrlib" not in already_applied_calibrators):
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
    calibrated_collections = ["Electron.pt", "Electron.pt_original", "Electron.energyErr"]

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
        self.et_dependent = self.ss_params.correctionlib_config[self.year].get("et_dependent", False)
        if self.isMC:
            self._variations = ["ele_scaleUp", "ele_scaleDown", "ele_smearUp", "ele_smearDown"]
        else:
            self._variations = []
        

    def initialize(self, events):
        # initialize the calibrator
        if not self.enabled:
            return
        seed = get_random_seed(events.metadata, salt="ElectronScaleCalibrator")
        self.electrons = ak.with_field(events.Electron,
                                       events["Electron"]["deltaEtaSC"] + events["Electron"]["eta"],
                                       "etaSC")
        self.electrons = ak.with_field(self.electrons, self.electrons["pt"], "pt_original")
        if self.isMC:
            # If the events are MC, we apply smearing
            if self.et_dependent:
                self.smeared_pt = get_ele_smeared_etdependent(self.electrons, self.ssfile, 
                                                             self.correction_name.smear, 
                                                             isMC=True, only_nominal=False, seed=seed)
                self.scaled_pt = get_ele_scaled_etdependent(self.electrons, self.ssfile, 
                                                           self.correction_name.scale, self.correction_name.smear,
                                                           isMC=True, runNr=events["run"], year=self.year)
            else:
                self.smeared_pt = get_ele_smeared(self.electrons, self.ssfile, self.correction_name.smear,
                                                isMC=True, only_nominal=False, seed=seed)
                # Also get the scale variations, without scaling the nominal
                self.scaled_pt = get_ele_scaled(self.electrons, self.ssfile, self.correction_name.scale,
                                                isMC=True, runNr=events["run"])
          
        else:
            # If the events are data, we apply only scaling
            if self.et_dependent:
                self.scaled_pt = get_ele_scaled_etdependent(self.electrons, self.ssfile, 
                                                           self.correction_name.scale, self.correction_name.smear,
                                                           isMC=False, runNr=events["run"], year=self.year)
            else:
                self.scaled_pt = get_ele_scaled(self.electrons, self.ssfile, self.correction_name.scale,
                                            isMC=False, runNr=events["run"])


    def calibrate(self, events, orig_colls, variation, already_applied_calibrators=None):
        if not self.enabled:
            return {}
        if self.et_dependent:
            if self.isMC:
                if variation == "nominal" or variation not in self._variations:
                    # If the variation is nominal or not handled by this calibrator
                    # we return the original pt
                    return {"Electron.pt": self.smeared_pt["pt"]["nominal"],
                            "Electron.pt_original": self.electrons["pt_original"],
                            "Electron.energyErr": self.smeared_pt["energyErr"]["nominal"]}
                elif variation == "ele_scaleUp":
                    return {"Electron.pt": self.scaled_pt["pt"]["up"],
                            "Electron.pt_original": self.electrons["pt_original"],
                            "Electron.energyErr": self.scaled_pt["energyErr"]["up"]}
                elif variation == "ele_scaleDown":
                    return {"Electron.pt": self.scaled_pt["pt"]["down"],
                            "Electron.pt_original": self.electrons["pt_original"],
                            "Electron.energyErr": self.scaled_pt["energyErr"]["down"]}
                elif variation == "ele_smearUp":
                    return {"Electron.pt": self.smeared_pt["pt"]["up"],
                            "Electron.pt_original": self.electrons["pt_original"],
                            "Electron.energyErr": self.smeared_pt["energyErr"]["up"]}
                elif variation == "ele_smearDown":
                    return {"Electron.pt": self.smeared_pt["pt"]["down"],
                            "Electron.pt_original": self.electrons["pt_original"],
                            "Electron.energyErr": self.smeared_pt["energyErr"]["down"]}
            else:
                # If the events are data, we do not apply smearing
                return {"Electron.pt": self.scaled_pt["pt"]["nominal"],
                        "Electron.pt_original": self.electrons["pt_original"],
                        "Electron.energyErr": self.scaled_pt["energyErr"]["nominal"]}
        else:  # older, non pt dependent corrections, energyErr not available
            if self.isMC:
                if variation == "nominal" or variation not in self._variations:
                    # If the variation is nominal or not handled by this calibrator
                    # we return the original pt
                    return {"Electron.pt": self.smeared_pt["pt"]["nominal"],
                            "Electron.pt_original": self.electrons["pt_original"]}
                elif variation == "ele_scaleUp":
                    return {"Electron.pt": self.scaled_pt["pt"]["up"],
                            "Electron.pt_original": self.electrons["pt_original"]}
                elif variation == "ele_scaleDown":
                    return {"Electron.pt": self.scaled_pt["pt"]["down"],
                            "Electron.pt_original": self.electrons["pt_original"]}
                elif variation == "ele_smearUp":
                    return {"Electron.pt": self.smeared_pt["pt"]["up"],
                            "Electron.pt_original": self.electrons["pt_original"]}
                elif variation == "ele_smearDown":
                    return {"Electron.pt": self.smeared_pt["pt"]["down"],
                            "Electron.pt_original": self.electrons["pt_original"]}
            else:
                # If the events are data, we do not apply smearing
                return {"Electron.pt": self.scaled_pt["pt"]["nominal"],
                        "Electron.pt_original": self.electrons["pt_original"]}

class MuonsCalibrator(Calibrator):

    name = "muons_scale_and_resolution"
    has_variations = True
    isMC_only = False
    calibrated_collections = ["Muon.pt", "Muon.pt_original", "Muon.energyErr"]


    def __init__(self, params, metadata, do_variations=True, **kwargs):
        super().__init__(params, metadata, do_variations, **kwargs)

        self._year = metadata["year"]
        self.isMC = metadata["isMC"]
        self.mscare_params = self.params.lepton_scale_factors.muon_sf.scale_and_resolution

        if not self.mscare_params.apply[self._year]:
            self.enabled = False
            self._variations = []
            self.calibrated_collections = []
            return

        self.enabled = True
        self.cset = correctionlib.CorrectionSet.from_file(
            self.mscare_params.correctionlib_config[self._year]["file"]
        )

        # Define variations
        if self.isMC:
            self._variations = [
                "muon_scaleUp",
                "muon_scaleDown",
                "muon_smearUp",
                "muon_smearDown",
            ]
        else:
            self._variations = []
        
        # Storage for all nominal + variations
        self.cache = {}

    def initialize(self, events):
        if not self.enabled:
            return

        mu = events.Muon
        
        self.muons = ak.with_field(mu, mu.pt, "pt_original")
        self.muons = ak.with_field(self.muons, ak.zeros_like(mu.pt), "energyErr")  

        # Save raw pt
        pt_raw = mu.pt
        self.cache["pt_raw"] = pt_raw

        flag = 0 if self.isMC else 1

        # Nominal scale → pt_scaled
        pt_scaled = pt_scale(
            flag, mu.pt, mu.eta, mu.phi, mu.charge,
            self.cset, nested=True
        )

        # Smearing or not
        if self.isMC:
            pt_corr = pt_resol(
                pt_scaled, mu.eta, mu.phi, mu.nTrackerLayers,
                events.event, events.luminosityBlock,
                self.cset, nested=True,
                rnd_gen="np" # ← ROOT-FREE
            )
        else:
            pt_corr = pt_scaled
            
        self.pt_corr = pt_corr

        if self.isMC:
            smear_up  = pt_resol_var(pt_scaled, pt_corr, mu.eta, "up", self.cset, nested=True)
            smear_down = pt_resol_var(pt_scaled, pt_corr, mu.eta, "dn", self.cset, nested=True)
        else:
            smear_up = smear_down = pt_scaled

        # like Electron: smeared_pt["pt"]["nominal"] etc.
        self.smeared_pt = {
            "pt": {
                "nominal": pt_corr,
                "up": smear_up,
                "down": smear_down,
            },
            "energyErr": {
                "nominal": ak.zeros_like(pt_raw),
                "up": ak.zeros_like(pt_raw),
                "down": ak.zeros_like(pt_raw),
            }
        }

        if self.isMC:
            scale_up = pt_scale_var(pt_corr, mu.eta, mu.phi, mu.charge, "up", self.cset, nested=True)
            scale_down = pt_scale_var(pt_corr, mu.eta, mu.phi, mu.charge, "dn", self.cset, nested=True)
        else:
            scale_up = scale_down = pt_scaled

        self.scaled_pt = {
            "pt": {
                "nominal": pt_scaled,
                "up": scale_up,
                "down": scale_down,
            },
            "energyErr": {
                "nominal": ak.zeros_like(pt_raw),
                "up": ak.zeros_like(pt_raw),
                "down": ak.zeros_like(pt_raw),
            }
        }
       

    def calibrate(self, events, orig_colls, variation, already_applied_calibrators=None):

        if not self.enabled:
            return {}


        # ---- NOMINAL ----
        if variation == "nominal" or variation not in self._variations:
            return {
                "Muon.pt": self.smeared_pt["pt"]["nominal"],
                "Muon.pt_original": self.muons["pt_original"],
                "Muon.energyErr": self.smeared_pt["energyErr"]["nominal"],
            }

        # ---- SCALE UP ----
        if variation == "muon_scaleUp":
            return {
                "Muon.pt": self.scaled_pt["pt"]["up"],
                "Muon.pt_original": self.muons["pt_original"],
                "Muon.energyErr": self.scaled_pt["energyErr"]["up"],
            }

        # ---- SCALE DOWN ----
        if variation == "muon_scaleDown":
            return {
                "Muon.pt": self.scaled_pt["pt"]["down"],
                "Muon.pt_original": self.muons["pt_original"],
                "Muon.energyErr": self.scaled_pt["energyErr"]["down"],
            }

        # ---- SMEAR UP ----
        if variation == "muon_smearUp":
            return {
                "Muon.pt": self.smeared_pt["pt"]["up"],
                "Muon.pt_original": self.muons["pt_original"],
                "Muon.energyErr": self.smeared_pt["energyErr"]["up"],
            }

        # ---- SMEAR DOWN ----
        if variation == "muon_smearDown":
            return {
                "Muon.pt": self.smeared_pt["pt"]["down"],
                "Muon.pt_original": self.muons["pt_original"],
                "Muon.energyErr": self.smeared_pt["energyErr"]["down"],
            }


#########################################
default_calibrators_sequence = [
    JetsCalibrator, METCalibrator, ElectronsScaleCalibrator, MuonsCalibrator,
]
