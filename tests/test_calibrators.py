
import pytest
import os
from pocket_coffea.lib.calibrators.common.common import (
    JetsCalibrator, 
    METCalibrator, 
    ElectronsScaleCalibrator, 
    MuonsCalibrator,
    JetsSoftdropMassCalibrator)
from pocket_coffea.lib.calibrators.calibrator import Calibrator
from pocket_coffea.lib.calibrators.calibrators_manager import CalibratorsManager

from pocket_coffea.parameters import defaults
import awkward as ak
import numpy as np
from tests.utils import events, events_run3


@pytest.fixture(scope="module")
def params():
    params = defaults.get_default_parameters()
    # Adding triggers
    params["shape_variations"] = {
        "test_obj_calibrator": {
            "2018": [
                "test_obj_variation_1Up",
                "test_obj_variation_1Down",
                "test_obj_variation_2Up",
                "test_obj_variation_2Down"
            ]
        }
    }
    return params

def test_calibrator_mechanism(events, params):
    class MyCalibrator(Calibrator):
        name = 'test_obj_calibrator'
        has_variations = True
        isMC_only = True
        calibrated_collections = ["Electron.pt"]

        def initialize(self, events):
            # Create some custom electron pt variation
            # initialize variations
            self._variations = [
                "test_obj_variation_1Up",
                "test_obj_variation_1Down",
                "test_obj_variation_2Up",
                "test_obj_variation_2Down"
            ]

        def calibrate(self, events, events_orig, variation, already_applied_calibrators=None):
            self.cache = {
                "nominal": {
                    "Electron.pt": events.Electron.pt * 1.2
                },
                "test_obj_variation_1Up": {
                    "Electron.pt": events.Electron.pt * 1.3
                },
                "test_obj_variation_1Down": {
                    "Electron.pt": events.Electron.pt * 1.1
                },
                "test_obj_variation_2Up": {
                    "Electron.pt": events.Electron.pt * 1.4
                },
                "test_obj_variation_2Down": {
                    "Electron.pt": events.Electron.pt * 1.0
                }
            }   
            # Return the cached variation
            return self.cache.get(variation, self.cache["nominal"])
    
    manager = CalibratorsManager(
        calibrators_list=[MyCalibrator],
        events=events,
        params=params,
        metadata={"isMC":True, "year":"2018"}
    )

    # Check if the calibrator is initialized correctly
    assert len(manager.calibrator_sequence) == 1
    assert manager.calibrator_sequence[0].name == "test_obj_calibrator"

    # Check if the variations are set correctly
    assert manager.calibrator_sequence[0].variations == [
        "test_obj_variation_1Up",
        "test_obj_variation_1Down",
        "test_obj_variation_2Up",
        "test_obj_variation_2Down"
    ]
    assert manager.available_variations == [
        "nominal",
        "test_obj_variation_1Up",
        "test_obj_variation_1Down",
        "test_obj_variation_2Up",
        "test_obj_variation_2Down"
    ]
    assert manager.calibrated_collections["Electron.pt"] == [manager.calibrator_sequence[0]]
    assert manager.available_variations_bycalibrator["test_obj_calibrator"] == [
        "test_obj_variation_1Up",
        "test_obj_variation_1Down",
        "test_obj_variation_2Up",
        "test_obj_variation_2Down"
    ]
    # Now checking the application of the calibration
    original_pt = ak.copy(events.Electron.pt)
    events = manager.calibrate(events, variation="nominal")
    assert ak.all(events.Electron.pt == manager.original_coll["Electron.pt"] * 1.2)
    assert ak.all(events.Electron.pt == original_pt * 1.2)
    manager.reset_events_to_original(events)

    events = manager.calibrate(events, variation="test_obj_variation_1Up")
    assert ak.all(events.Electron.pt == manager.original_coll["Electron.pt"] * 1.3)
    assert ak.all(events.Electron.pt == original_pt * 1.3)
    manager.reset_events_to_original(events)

    events = manager.calibrate(events, variation="test_obj_variation_1Down")
    assert ak.all(events.Electron.pt == manager.original_coll["Electron.pt"] * 1.1)
    assert ak.all(events.Electron.pt == original_pt * 1.1)
    manager.reset_events_to_original(events)

    events = manager.calibrate(events, variation="test_obj_variation_2Up")
    assert ak.all(events.Electron.pt == manager.original_coll["Electron.pt"] * 1.4)
    assert ak.all(events.Electron.pt == original_pt * 1.4)
    manager.reset_events_to_original(events)

    events = manager.calibrate(events, variation="test_obj_variation_2Down")
    assert ak.all(events.Electron.pt == manager.original_coll["Electron.pt"] * 1.0)
    assert ak.all(events.Electron.pt == original_pt * 1.0)
    manager.reset_events_to_original(events)

    # Now testing without resetting
    events = manager.calibrate(events, variation="nominal")
    assert ak.all(events.Electron.pt == manager.original_coll["Electron.pt"] * 1.2)
    events = manager.calibrate(events, variation="test_obj_variation_1Up")
    assert not ak.all(events.Electron.pt == manager.original_coll["Electron.pt"] * 1.3)
    manager.reset_events_to_original(events)


def test_calibrator_variations_single(events, params):
    class MyCalibrator(Calibrator):
        name = 'test_obj_calibrator2'
        has_variations = True
        isMC_only = True
        calibrated_collections = ["Electron.pt"]

        def initialize(self, events):
            # Create some custom electron pt variation
            # initialize variations
            self._variations = [
                "test_obj_variation_1Up",
                "test_obj_variation_1Down",
                "test_obj_variation_2Up",
                "test_obj_variation_2Down"
            ]

        def calibrate(self, events, events_orig, variation, already_applied_calibrators=None):
            self.cache = {
                "nominal": {
                    "Electron.pt": events.Electron.pt * 1.2
                },
                "test_obj_variation_1Up": {
                    "Electron.pt": events.Electron.pt * 1.3
                },
                "test_obj_variation_1Down": {
                    "Electron.pt": events.Electron.pt * 1.1
                },
                "test_obj_variation_2Up": {
                    "Electron.pt": events.Electron.pt * 1.4
                },
                "test_obj_variation_2Down": {
                    "Electron.pt": events.Electron.pt * 1.0
                }
            }   
            # Return the cached variation
            return self.cache.get(variation, self.cache["nominal"])
    
    manager = CalibratorsManager(
        calibrators_list=[MyCalibrator],
        events=events,
        params=params,
        metadata={"isMC":True, "year":"2018"}
    )

    # Check if the calibrator is initialized correctly
    assert len(manager.calibrator_sequence) == 1
    assert manager.calibrator_sequence[0].name == "test_obj_calibrator2"

    # Check if the variations are set correctly
    assert manager.calibrator_sequence[0].variations == [
        "test_obj_variation_1Up",
        "test_obj_variation_1Down",
        "test_obj_variation_2Up",
        "test_obj_variation_2Down"
    ]
    assert manager.available_variations == [
        "nominal",
        "test_obj_variation_1Up",
        "test_obj_variation_1Down",
        "test_obj_variation_2Up",
        "test_obj_variation_2Down"
    ]
    assert manager.calibrated_collections["Electron.pt"] == [manager.calibrator_sequence[0]]
    assert manager.available_variations_bycalibrator["test_obj_calibrator2"] == [
        "test_obj_variation_1Up",
        "test_obj_variation_1Down",
        "test_obj_variation_2Up",
        "test_obj_variation_2Down"
    ]

    original_pt = ak.copy(events.Electron.pt)
    # Now checking the application of the calibration
    for variation, events in manager.calibration_loop(events):
        assert ak.all(manager.original_coll["Electron.pt"] == original_pt)

        if variation == "nominal":
            assert ak.all(events.Electron.pt == original_pt * 1.2)
        elif variation == "test_obj_variation_1Up":
            assert ak.all(events.Electron.pt == original_pt * 1.3)
        elif variation == "test_obj_variation_1Down":
            assert ak.all(events.Electron.pt == original_pt * 1.1)
        elif variation == "test_obj_variation_2Up":
            assert ak.all(events.Electron.pt == original_pt * 1.4)
        elif variation == "test_obj_variation_2Down":
            assert ak.all(events.Electron.pt == original_pt * 1.0)
        else:
            raise ValueError(f"Unknown variation: {variation}")



def test_calibrators_sequence(events, params):
    class MyCalibrator(Calibrator):
        name = 'test_obj_calibrator3'
        has_variations = True
        isMC_only = True
        calibrated_collections = ["Electron.pt"]

        def initialize(self, events):
            # Create some custom electron pt variation
            # initialize variations
            self._variations = [
                "test_obj_variation_1Up",
                "test_obj_variation_1Down",
                "test_obj_variation_2Up",
                "test_obj_variation_2Down"
            ]

        def calibrate(self, events, events_orig, variation, already_applied_calibrators=None):
            self.cache = {
                "nominal": {
                    "Electron.pt": events.Electron.pt * 1.2
                },
                "test_obj_variation_1Up": {
                    "Electron.pt": events.Electron.pt * 1.3
                },
                "test_obj_variation_1Down": {
                    "Electron.pt": events.Electron.pt * 1.1
                },
                "test_obj_variation_2Up": {
                    "Electron.pt": events.Electron.pt * 1.4
                },
                "test_obj_variation_2Down": {
                    "Electron.pt": events.Electron.pt * 1.0
                }
            }   
            # Return the cached variation
            return self.cache.get(variation, self.cache["nominal"])
    
    class MyCalibrator2(Calibrator):
        name = 'test_obj_calibrator4'
        has_variations = True
        isMC_only = True
        calibrated_collections = ["Electron.pt"]

        def initialize(self, events):
            # Create some custom electron pt variation
            # initialize variations
            self._variations = [
                "test_obj_variation_3Up",
                "test_obj_variation_3Down",
                "test_obj_variation_4Up",
                "test_obj_variation_4Down"
            ]

        def calibrate(self, events, events_orig, variation, already_applied_calibrators=None):
            assert already_applied_calibrators is not None, "Already applied calibrators should be provided"
            assert "test_obj_calibrator3" in already_applied_calibrators, "test_obj_calibrator3 should be applied before test_obj_calibrator4"
            # Create some custom electron pt variation"
            self.cache = {
                "nominal": {
                    "Electron.pt": events.Electron.pt + 5.
                },
                "test_obj_variation_3Up": {
                    "Electron.pt": events.Electron.pt + 6.
                },
                "test_obj_variation_3Down": {
                    "Electron.pt": events.Electron.pt + 4.
                },
                "test_obj_variation_4Up": {
                    "Electron.pt": events.Electron.pt + 7.
                },
                "test_obj_variation_4Down": {
                    "Electron.pt": events.Electron.pt + 3.
                }
            }   
            # Return the cached variation
            return self.cache.get(variation, self.cache["nominal"])
    

    manager = CalibratorsManager(
        calibrators_list=[MyCalibrator, MyCalibrator2],
        events=events,
        params=params,
        metadata={"isMC":True, "year":"2018"}
    )

    # Check if the calibrator is initialized correctly
    assert len(manager.calibrator_sequence) == 2
    assert manager.calibrator_sequence[0].name == "test_obj_calibrator3"
    assert manager.calibrator_sequence[1].name == "test_obj_calibrator4"

    # Check if the variations are set correctly
    assert manager.available_variations == [
        "nominal",
        "test_obj_variation_1Up",
        "test_obj_variation_1Down",
        "test_obj_variation_2Up",
        "test_obj_variation_2Down",
        "test_obj_variation_3Up",
        "test_obj_variation_3Down",
        "test_obj_variation_4Up",
        "test_obj_variation_4Down"
    ]
    assert manager.calibrated_collections["Electron.pt"] == [manager.calibrator_sequence[0], manager.calibrator_sequence[1]]
    assert manager.available_variations_bycalibrator["test_obj_calibrator3"] == [
        "test_obj_variation_1Up",
        "test_obj_variation_1Down",
        "test_obj_variation_2Up",
        "test_obj_variation_2Down"
    ]
    assert manager.available_variations_bycalibrator["test_obj_calibrator4"] == [
        "test_obj_variation_3Up",
        "test_obj_variation_3Down",
        "test_obj_variation_4Up",
        "test_obj_variation_4Down"
    ]


    original_pt = ak.copy(events.Electron.pt)
    # This is testing both the order and the fact that the calibrators are applied in sequence
    for variation, events in manager.calibration_loop(events):
        assert ak.all(manager.original_coll["Electron.pt"] == original_pt)
        if variation == "nominal":
            assert ak.all(events.Electron.pt == original_pt * 1.2 + 5.)
        # now variations for the calibrator 1 and nominal for the second
        elif variation == "test_obj_variation_1Up":
            assert ak.all(events.Electron.pt == original_pt * 1.3 +5)
        elif variation == "test_obj_variation_1Down":
            assert ak.all(events.Electron.pt == original_pt * 1.1 + 5)
        elif variation == "test_obj_variation_2Up":
            assert ak.all(events.Electron.pt == original_pt * 1.4 + 5)
        elif variation == "test_obj_variation_2Down":
            assert ak.all(events.Electron.pt == original_pt * 1.0 +5.)
        # now variations for the calibrator 2 and nominal for the first
        elif variation == "test_obj_variation_3Up":
            assert ak.all(events.Electron.pt == original_pt * 1.2 + 6.)
        elif variation == "test_obj_variation_3Down":
            assert ak.all(events.Electron.pt == original_pt * 1.2 + 4.)
        elif variation == "test_obj_variation_4Up":
            assert ak.all(events.Electron.pt == original_pt * 1.2 + 7.)
        elif variation == "test_obj_variation_4Down":
            assert ak.all(events.Electron.pt == original_pt * 1.2 + 3.)
        else:
            raise ValueError(f"Unknown variation: {variation}")



def test_jets_calibrator(events, params):
    jets_calibrator = JetsCalibrator(params=params, metadata={"isMC": True, "year": "2018"},
                                     do_variations=True)
    jets_calibrator.initialize(events)

    orig_events = ak.copy(events)

    out1 = jets_calibrator.calibrate(events, {}, variation="nominal")
    assert "Jet" in out1
    
    for variation in jets_calibrator.variations:
        out2 = jets_calibrator.calibrate(events, {}, variation=variation)

        if "AK4" in variation:
            assert "Jet" in out2
            assert ak.all(out2["Jet"].pt != orig_events.Jet.pt)
            assert ak.all(out2["Jet"].pt != out1["Jet"].pt)

            # Check that the jets are always sorted by pt
            sorted_indices1 = ak.argsort(out2["Jet"].pt, axis=1, ascending=False)
            sorted_jets1 = out2["Jet"][sorted_indices1]
            assert ak.all(sorted_jets1.pt[:, :-1] >= sorted_jets1.pt[:, 1:])

        if "AK8" in variation:
            assert "FatJet" in out2
            assert ak.all(out2["FatJet"].pt != orig_events.FatJet.pt)
            assert ak.all(out2["FatJet"].pt != out1["FatJet"].pt)

            # Check that the fatjets are always sorted by pt
            sorted_indices2 = ak.argsort(out2["FatJet"].pt, axis=1, ascending=False)
            sorted_jets2 = out2["FatJet"][sorted_indices2]
            assert ak.all(sorted_jets2.pt[:, :-1] >= sorted_jets2.pt[:, 1:])


def test_jets_softdrop_mass_calibrator(events_run3, params):
    """Test the JetsSoftdropMassCalibrator for AK8 jets softdrop mass correction"""
    # Test requires AK8 jets to be present in events
    for year, events in events_run3.items():
        print(f"\n=== Testing JetsSoftdropMassCalibrator for year {year} ===")
        if "FatJet" not in events.fields:
            pytest.skip("FatJet collection not found in test events")
        
        # Check if we have jets with subjets (required for softdrop mass correction)
        if hasattr(events.FatJet, 'subjets') and ak.any(ak.num(events.FatJet.subjets) > 0):
            jets_with_subjets = True
        else:
            jets_with_subjets = False
            print("Warning: No subjets found in FatJet collection, creating mock subjets for testing")

        isMC = "genWeight" in events.fields

        # Create the calibrator
        softdrop_calibrator = JetsSoftdropMassCalibrator(
            params=params, 
            metadata={"isMC": isMC, "year": year},
            do_variations=True
        )
        
        # Store original events for comparison
        orig_events = ak.copy(events)
        original_msoftdrop = ak.copy(events.FatJet.msoftdrop) if "msoftdrop" in events.FatJet.fields else None

        assert original_msoftdrop is not None, "msoftdrop field not found in FatJet collection"
        
        try:
            # Initialize the calibrator
            softdrop_calibrator.initialize(events)
            
            print(f"Calibrator initialized successfully")
            print(f"Available variations: {softdrop_calibrator.variations}")
            print(f"Calibrated collections: {softdrop_calibrator.calibrated_collections}")
            
            # Test nominal calibration
            out_nominal = softdrop_calibrator.calibrate(events, {}, variation="nominal")

            # Check that calibration produces output
            assert isinstance(out_nominal, dict), "Calibrator should return a dictionary"
            assert "FatJet" in out_nominal, "FatJet collection should be in calibrated output"
            assert "msoftdrop" in out_nominal["FatJet"].fields, "msoftdrop field should be in calibrated FatJet collection"

            calibrated_msoftdrop = out_nominal["FatJet"].msoftdrop
            
            # Calculate and print the ratio of corrected/original
            # Only for jets where both original and corrected values are > 0
            valid_mask = (original_msoftdrop > 0) & (calibrated_msoftdrop > 0)
            
            if ak.any(valid_mask):
                ratio = ak.where(valid_mask, calibrated_msoftdrop / original_msoftdrop, 1.0)
                mean_ratio = ak.mean(ratio[valid_mask])
                std_ratio = ak.std(ratio[valid_mask])
                
                print(f"✓ Softdrop mass correction statistics:")
                print(f"  - Number of valid jets: {ak.sum(valid_mask)}")
                print(f"  - Mean correction ratio (corrected/original): {mean_ratio:.4f}")
                print(f"  - Std deviation of ratio: {std_ratio:.4f}")
                print(f"  - Min ratio: {ak.min(ratio[valid_mask]):.4f}")
                print(f"  - Max ratio: {ak.max(ratio[valid_mask]):.4f}")
            else:
                print("⚠ No valid jets found for ratio calculation")
            
            # Test systematic variations if available
            if softdrop_calibrator.variations:
                print(f"\n✓ Testing {len(softdrop_calibrator.variations)} systematic variations...")
                
                for i, variation in enumerate(softdrop_calibrator.variations[:3]):  # Test first 3 variations
                    print(f"  Testing variation: {variation}")
                    out_var = softdrop_calibrator.calibrate(events, {}, variation=variation)
                    
                    if "FatJet" in out_var and "msoftdrop" in out_var["FatJet"].fields:
                        var_msoftdrop = out_var["FatJet"].msoftdrop
                        nominal_msoftdrop = out_nominal["FatJet"].msoftdrop
                        
                        # Check that variation produces different results from nominal
                        if not ak.all(var_msoftdrop == nominal_msoftdrop):
                            print(f"    ✓ Variation {variation} produces different results from nominal")
                        else:
                            print(f"    ⚠ Variation {variation} produces same results as nominal")
                            
                        # Basic sanity check
                        assert ak.all(var_msoftdrop >= 0), f"Variation {variation} should produce non-negative msoftdrop"
            else:
                print("⚠ No systematic variations available to test")
                
            print("\n✓ JetsSoftdropMassCalibrator test completed successfully!")
            
        except Exception as e:
            print(f"\n✗ Error in JetsSoftdropMassCalibrator test: {str(e)}")
            # Print more details for debugging
            print(f"Available FatJet fields: {list(events.FatJet.fields) if 'FatJet' in events.fields else 'No FatJet'}")
            if "FatJet" in events.fields and hasattr(events.FatJet, 'subjets'):
                print(f"FatJet has subjets: True")
                print(f"Number of jets with subjets: {ak.sum(ak.count(events.FatJet.subjets.pt, axis=-1) > 0)}")
            else:
                print(f"FatJet has subjets: False")
            
            # Re-raise the exception for pytest
            raise


def test_muons_calibrator(events, params):
    """
    Tests the updated MuonsCalibrator to ensure:
    - pt_original is stored and preserved
    - energyErr exists and is zeros
    - nominal returns smeared pt
    - scaleUp/scaleDown use scaled_pt
    - smearUp/smearDown use smeared_pt
    """

    mu_cal = MuonsCalibrator(
        params=params,
        metadata={"isMC": True, "year": "2022_preEE"},
        do_variations=True
    )

    # ---- INITIALIZE ----
    mu_cal.initialize(events)

    # Required fields must be created
    assert hasattr(mu_cal, "muons")
    assert "pt_original" in mu_cal.muons.fields
    assert "energyErr" in mu_cal.muons.fields

    original_pt = ak.copy(mu_cal.muons.pt_original)
    original_err = ak.copy(mu_cal.muons.energyErr)

    # Check pt_original stores real uncalibrated pt
    assert ak.all(original_pt == events.Muon.pt)
    assert ak.all(original_err == 0)

    # ---- CHECK VARIATIONS DEFINED CORRECTLY ----
    assert mu_cal.variations == [
        "muon_scaleUp",
        "muon_scaleDown",
        "muon_smearUp",
        "muon_smearDown",
    ]

    # ---- NOMINAL ----
    out_nom = mu_cal.calibrate(events, {}, variation="nominal")

    assert "Muon.pt" in out_nom
    assert "Muon.pt_original" in out_nom
    assert "Muon.energyErr" in out_nom

    assert ak.all(out_nom["Muon.pt_original"] == original_pt)
    assert ak.all(out_nom["Muon.energyErr"] == 0)

    expected_nom_pt = mu_cal.smeared_pt["pt"]["nominal"]
    assert ak.all(out_nom["Muon.pt"] == expected_nom_pt)

    # ---- SCALE UP ----
    out_scale_up = mu_cal.calibrate(events, {}, variation="muon_scaleUp")
    assert ak.all(out_scale_up["Muon.pt"] == mu_cal.scaled_pt["pt"]["up"])
    assert ak.all(out_scale_up["Muon.pt_original"] == original_pt)

    # ---- SCALE DOWN ----
    out_scale_down = mu_cal.calibrate(events, {}, variation="muon_scaleDown")
    assert ak.all(out_scale_down["Muon.pt"] == mu_cal.scaled_pt["pt"]["down"])
    assert ak.all(out_scale_down["Muon.pt_original"] == original_pt)

    # ---- SMEAR UP ----
    out_smear_up = mu_cal.calibrate(events, {}, variation="muon_smearUp")
    assert ak.all(out_smear_up["Muon.pt"] == mu_cal.smeared_pt["pt"]["up"])
    assert ak.all(out_smear_up["Muon.pt_original"] == original_pt)

    # ---- SMEAR DOWN ----
    out_smear_down = mu_cal.calibrate(events, {}, variation="muon_smearDown")
    assert ak.all(out_smear_down["Muon.pt"] == mu_cal.smeared_pt["pt"]["down"])
    assert ak.all(out_smear_down["Muon.pt_original"] == original_pt)


def test_muons_calibrator_explicit(events, params):
    """
    Explicitly tests:
    - nominal == smeared_pt["nominal"]
    - scaleUp/down == scaled_pt
    - smearUp/down == smeared_pt
    - pt_original preserved
    - energyErr is always zeros
    """

    mu_cal = MuonsCalibrator(
        params=params,
        metadata={"isMC": True, "year": "2022_preEE"},  # ensure enabled
        do_variations=True,
    )

    # initialize
    mu_cal.initialize(events)

    mu = events.Muon
    pt_raw = ak.copy(mu.pt)

    # convenience
    s = mu_cal.smeared_pt["pt"]
    sc = mu_cal.scaled_pt["pt"]
    e = mu_cal.smeared_pt["energyErr"]["nominal"]

    # ---- nominal ----
    out = mu_cal.calibrate(events, {}, variation="nominal")
    assert ak.all(out["Muon.pt"] == s["nominal"])
    assert ak.all(out["Muon.pt_original"] == pt_raw)
    assert ak.all(out["Muon.energyErr"] == e)

    # ---- scale up ----
    out = mu_cal.calibrate(events, {}, variation="muon_scaleUp")
    assert ak.all(out["Muon.pt"] == sc["up"])
    assert ak.all(out["Muon.pt_original"] == pt_raw)
    assert ak.all(out["Muon.energyErr"] == 0)

    # ---- scale down ----
    out = mu_cal.calibrate(events, {}, variation="muon_scaleDown")
    assert ak.all(out["Muon.pt"] == sc["down"])
    assert ak.all(out["Muon.pt_original"] == pt_raw)
    assert ak.all(out["Muon.energyErr"] == 0)

    # ---- smear up ----
    out = mu_cal.calibrate(events, {}, variation="muon_smearUp")
    assert ak.all(out["Muon.pt"] == s["up"])
    assert ak.all(out["Muon.pt_original"] == pt_raw)
    assert ak.all(out["Muon.energyErr"] == 0)

    # ---- smear down ----
    out = mu_cal.calibrate(events, {}, variation="muon_smearDown")
    assert ak.all(out["Muon.pt"] == s["down"])
    assert ak.all(out["Muon.pt_original"] == pt_raw)
    assert ak.all(out["Muon.energyErr"] == 0)
