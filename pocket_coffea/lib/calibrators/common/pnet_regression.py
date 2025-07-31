from ..calibrator import Calibrator
import numpy as np
import awkward as ak


class PNetRegressionCalibrator(Calibrator):
    """
    Calibrator that applies PNet jet regression to jets.
    
    This calibrator applies ParticleNet regression corrections to jet pT and mass
    for jets that pass certain b-tagging and c-tagging criteria.
    """
    
    name = "pnet_regression"
    has_variations = False
    isMC_only = False
    calibrated_collections = ["Jet"]

    def __init__(self, params, metadata, **kwargs):
        super().__init__(params, metadata)
        if metadata is not None:
            self._year = metadata.get("year", None)
        else:
            self._year = None
        
        # Check if regression is enabled and configured
        if params is not None and hasattr(params, 'object_preselection') and hasattr(params.object_preselection, 'Jet'):
            jet_params = params.object_preselection.Jet
            if hasattr(jet_params, 'regression') and jet_params.regression.get('do', False):
                self.regression_params = jet_params.regression
            else:
                self.regression_params = None
        else:
            self.regression_params = None

    def initialize(self, events):
        """
        Initialize the calibrator. No specific initialization needed for regression.
        """
        # No variations for this calibrator
        self._variations = []

    def calibrate(self, events, events_original_collections, variation):
        """
        Apply PNet regression to jets.
        
        Args:
            events: Events object containing jet collections
            events_original_collections: Dictionary of original collections
            variation: Variation name (not used for this calibrator)
            
        Returns:
            Dictionary with calibrated jet collection
        """
        out = {}
        
        # Skip if regression parameters are not configured
        if self.regression_params is None:
            out["Jet"] = events.Jet
            return out
        
        # Get the original jets
        jets = events.Jet
        
        # Apply regression only to specific jet types (AK4PFPuppi, AK4PFchs)
        # This check should ideally be done based on jet type parameter, but for now
        # we assume it's applied to the main Jet collection
        
        # Flatten jets for easier processing
        j_flat, nj = ak.flatten(jets), ak.num(jets)
        
        # Check if required fields exist
        required_fields = ['rawFactor', 'PNetRegPtRawCorr', 'PNetRegPtRawCorrNeutrino', 'btagPNetB', 'btagPNetCvL']
        missing_fields = [field for field in required_fields if field not in j_flat.fields]
        
        if missing_fields:
            # If required fields are missing, return original jets without regression
            out["Jet"] = jets
            return out
        
        # Obtain the regressed PT and Mass
        reg_j_pt = j_flat['pt'] * (1 - j_flat['rawFactor']) * j_flat['PNetRegPtRawCorr'] * j_flat['PNetRegPtRawCorrNeutrino']
        reg_j_mass = j_flat['mass'] * (1 - j_flat['rawFactor']) * j_flat['PNetRegPtRawCorr'] * j_flat['PNetRegPtRawCorrNeutrino']
        
        # Select which jets to apply the regression to (cuts are provided via parameter yaml)
        cuts = self.regression_params
        reg_mask = (j_flat['btagPNetB'] >= cuts['cut_btagB']) | (j_flat['btagPNetCvL'] >= cuts['cut_btagCvL'])
        
        # Apply regression where mask is True, keep original values otherwise
        new_j_pt_flat = ak.where(reg_mask, reg_j_pt, j_flat['pt'])
        new_j_pt = ak.unflatten(new_j_pt_flat, nj)
        
        new_j_mass_flat = ak.where(reg_mask, reg_j_mass, j_flat['mass'])
        new_j_mass = ak.unflatten(new_j_mass_flat, nj)
        
        # Replace the PT and Mass variables in the original jets collection
        jets_regressed = ak.with_field(jets, new_j_pt, 'pt')
        jets_regressed = ak.with_field(jets_regressed, new_j_mass, 'mass')
        
        out["Jet"] = jets_regressed
        return out
