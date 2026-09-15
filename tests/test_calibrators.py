
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


def test_jets_sort_and_jetidx_remap(events, params):
    """The JetsCalibrator re-sorts the nominal AND the varied jets by corrected pt and
    records the permutation as a field, so jets_in_original_order can restore the NanoAOD
    order for per-jet-index (lepton.jetIdx) lookups."""
    from pocket_coffea.lib.jets import jets_in_original_order, JET_SORTIDX_FIELD

    jets_calibrator = JetsCalibrator(params=params, metadata={"isMC": True, "year": "2018"},
                                     do_variations=True)
    jets_calibrator.initialize(events)

    # calibrate() does not mutate events, so the original NanoAOD jets remain available
    orig_btag = ak.copy(events.Jet.btagDeepFlavB)
    orig_counts = ak.num(events.Jet.pt)
    valid = ak.mask(events.Electron.jetIdx, events.Electron.jetIdx != -1)
    btag_orig = ak.fill_none(events.Jet[valid].btagDeepFlavB, -10.0)

    variations_to_check = ["nominal"] + [v for v in jets_calibrator.variations if "AK4" in v][:1]
    for variation in variations_to_check:
        out = jets_calibrator.calibrate(events, {}, variation=variation)
        jets_cal = out["Jet"]

        # Nominal AND variations are re-sorted by pt (descending) and carry the permutation
        assert JET_SORTIDX_FIELD in jets_cal.fields
        assert ak.all(jets_cal.pt[:, :-1] >= jets_cal.pt[:, 1:])

        # Undoing the sort restores the NanoAOD order (btag is not touched by the JEC)
        recovered = jets_in_original_order(jets_cal)
        assert ak.all(ak.num(recovered) == orig_counts)
        assert ak.all(ak.flatten(recovered.btagDeepFlavB) == ak.flatten(orig_btag))

        # jetIdx remap: the closest-jet btag via the recovered order matches the lookup on
        # the original unsorted jets — the electron still points at the right jet.
        btag_recovered = ak.fill_none(recovered[valid].btagDeepFlavB, -10.0)
        assert ak.all(ak.flatten(btag_recovered) == ak.flatten(btag_orig))

    # jets_in_original_order is a no-op when the sort field is absent
    plain = jets_in_original_order(events.Jet)
    assert ak.all(ak.flatten(plain.btagDeepFlavB) == ak.flatten(orig_btag))


def test_jets_collection_name_alias(events, params):
    """`collection_name_alias` lets a jet collection resolve its per-jet-type
    configuration (jet_types / variations / sort_by_pt / apply_jec_MC / ...)
    under a *different* jet-type key.

    The default 2018 config uses this for the type-1-MET jets: jet_type
    `AK4CorrT1METJetchs` (collection `CorrT1METJet`) is aliased to `AK4PFchs`,
    and `sort_by_pt` / `variations` have NO `AK4CorrT1METJetchs` entry, so any
    lookup that ignored the alias would KeyError. `calibrate()` re-sorts by pt
    per collection using `jetcoll_to_alias` to recover that alias from the
    collection name, so this pins both the feature and that map.
    """
    calib_param = params.jets_calibration

    # Preconditions: the alias is configured, and the aliased-away jet_type is
    # deliberately absent from the per-jet-type config (proving the lookups below
    # can only succeed via the alias).
    assert calib_param.collection_name_alias["2018"]["AK4CorrT1METJetchs"] == "AK4PFchs"
    assert "AK4CorrT1METJetchs" not in calib_param.sort_by_pt["2018"]
    assert "AK4CorrT1METJetchs" not in calib_param.variations

    jets_calibrator = JetsCalibrator(params=params, metadata={"isMC": True, "year": "2018"},
                                     do_variations=True)
    jets_calibrator.initialize(events)

    # 1. jetcoll_to_alias maps each calibrated collection to the alias used for all
    #    its config lookups: 'Jet'/'FatJet' have no alias (jet_type == alias);
    #    'CorrT1METJet' is aliased to 'AK4PFchs'.
    assert jets_calibrator.jetcoll_to_alias["Jet"] == "AK4PFchs"
    assert jets_calibrator.jetcoll_to_alias["FatJet"] == "AK8PFPuppi"
    assert jets_calibrator.jetcoll_to_alias["CorrT1METJet"] == "AK4PFchs"

    # 2. The aliased collection is actually calibrated (apply_jec_MC resolved via
    #    the alias == True) and re-sorted by pt (sort_by_pt resolved via the alias).
    out = jets_calibrator.calibrate(events, {}, variation="nominal")
    assert "CorrT1METJet" in out
    corr = out["CorrT1METJet"]
    assert ak.all(corr.pt[:, :-1] >= corr.pt[:, 1:])


def test_jets_collection_name_alias_value_equivalence(events, params):
    """Calibrating a collection through a custom alias yields exactly the same
    result as configuring that jet-type key directly: the alias only redirects
    where the config is read, not the config itself.

    We rename the 'Jet' jet-type to a key present in NO config sub-dict and alias
    it back to the real 'AK4PFchs' key; if any lookup bypassed the alias it would
    KeyError, and the calibrated jets must match the un-aliased baseline.
    """
    import copy
    from omegaconf import OmegaConf

    base = JetsCalibrator(params=params, metadata={"isMC": True, "year": "2018"},
                          do_variations=False)
    base.initialize(events)
    out_base = base.calibrate(events, {}, variation="nominal")["Jet"]

    p2 = copy.deepcopy(params)
    OmegaConf.set_struct(p2, False)
    # Only the Jet collection, under a jet-type name that exists nowhere else,
    # aliased to the real config key.
    p2.jets_calibration.collection["2018"] = {"MyJetType": "Jet"}
    p2.jets_calibration.collection_name_alias["2018"] = {"MyJetType": "AK4PFchs"}

    cal = JetsCalibrator(params=p2, metadata={"isMC": True, "year": "2018"},
                         do_variations=False)
    cal.initialize(events)
    assert cal.jetcoll_to_alias["Jet"] == "AK4PFchs"
    out_alias = cal.calibrate(events, {}, variation="nominal")["Jet"]

    assert ak.all(ak.flatten(out_alias.pt) == ak.flatten(out_base.pt))
    assert ak.all(ak.flatten(out_alias.mass) == ak.flatten(out_base.mass))


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

def test_muons_rochester_calibrator(events, params):
    """
    Tests the MuonsRochesterCalibrator:
    - enabled for Run 2 years, disabled for Run 3
    - pt_original is stored and preserved
    - nominal returns corrected pt (different from original)
    - up/down variations are different from nominal
    - no NaN or negative values
    """
    from pocket_coffea.lib.calibrators.legacy.legacy_calibrators import MuonsRochesterCalibrator

    # ---- DISABLED for Run 3 ----
    cal_run3 = MuonsRochesterCalibrator(
        params=params,
        metadata={"isMC": True, "year": "2022_preEE"},
        do_variations=True
    )
    assert cal_run3.enabled == False
    assert cal_run3._variations == []

    # ---- ENABLED for Run 2 MC ----
    cal = MuonsRochesterCalibrator(
        params=params,
        metadata={"isMC": True, "year": "2018"},
        do_variations=True
    )
    assert cal.enabled == True
    assert cal._variations == ["muon_roccorUp", "muon_roccorDown"]

    # ---- INITIALIZE ----
    cal.initialize(events)
    assert hasattr(cal, "muons")
    assert "pt_original" in cal.muons.fields

    # ---- NOMINAL ----
    out_nom = cal.calibrate(events, {}, variation="nominal")
    assert "Muon.pt" in out_nom
    assert "Muon.pt_original" in out_nom
    assert not ak.any(np.isnan(ak.to_numpy(ak.flatten(out_nom["Muon.pt"]))))
    assert ak.all(out_nom["Muon.pt"] > 0)
    assert not ak.all(out_nom["Muon.pt"] == out_nom["Muon.pt_original"])

    # ---- UP ----
    out_up = cal.calibrate(events, {}, variation="muon_roccorUp")
    assert ak.all(out_up["Muon.pt"] > 0)
    assert not ak.all(out_up["Muon.pt"] == out_nom["Muon.pt"])

    # ---- DOWN ----
    out_down = cal.calibrate(events, {}, variation="muon_roccorDown")
    assert ak.all(out_down["Muon.pt"] > 0)
    assert not ak.all(out_down["Muon.pt"] == out_nom["Muon.pt"])

    # ---- DATA ----
    cal_data = MuonsRochesterCalibrator(
        params=params,
        metadata={"isMC": False, "year": "2018"},
        do_variations=True
    )
    assert cal_data._variations == []
    cal_data.initialize(events)
    out_data = cal_data.calibrate(events, {}, variation="nominal")
    assert ak.all(out_data["Muon.pt"] > 0)
    assert not ak.all(out_data["Muon.pt"] == out_data["Muon.pt_original"])


def test_met_type1_variation_propagation(events, params):
    """Validation harness for the type-1 MET recompute (A1).

    Runs JetsCalibrator + METCalibrator and, for the nominal and every JES/JER variation,
    recomputes the type-1 MET independently from the (varied) jets using the *invariant*
    raw pt (jet.pt_raw). The METCalibrator must match this recomputation.

    This is what catches A1: before the fix the METCalibrator derived the jet raw pt as
    pt*(1-rawFactor); for a variation the jets carry the varied pt with the nominal
    rawFactor, so that raw pt became pt_raw*(varied/nominal) and the varied MET did NOT
    match this pt_raw-based recomputation.
    """
    import vector

    def independent_t1_met(rawmet_pt, rawmet_phi, jets, corrt1):
        # --- main jets: invariant raw pt, Puppi L1 = 1 ---
        jet_pt_raw = jets["pt_raw"]
        jecL1L2L3 = jets["pt"] / jet_pt_raw
        pt_noMuRaw = jet_pt_raw * (1.0 - jets["muonSubtrFactor"])
        phi = jets["muonSubtrDeltaPhi"] + jets["phi"] if "muonSubtrDeltaPhi" in jets.fields else jets["phi"]
        pt_L1 = pt_noMuRaw
        pt_L1L2L3 = pt_noMuRaw * jecL1L2L3
        mask = (pt_L1L2L3 > 15) & (jets["chEmEF"] + jets["neEmEF"] < 0.9)
        d = vector.zip({"rho": pt_L1L2L3[mask], "phi": phi[mask]}) - vector.zip({"rho": pt_L1[mask], "phi": phi[mask]})
        corr = vector.zip({"x": ak.sum(d.x, axis=1), "y": ak.sum(d.y, axis=1)})
        # --- CorrT1METJet: nominal (deferred), matches the calibrator ---
        c_jecL1L2L3 = 1.0 / (1.0 - corrt1["rawFactor"])
        c_pt_noMuRaw = corrt1["rawPt"] * (1.0 - corrt1["muonSubtrFactor"])
        c_phi = corrt1["muonSubtrDeltaPhi"] + corrt1["phi"] if "muonSubtrDeltaPhi" in corrt1.fields else corrt1["phi"]
        c_pt_L1 = c_pt_noMuRaw
        c_pt_L1L2L3 = c_pt_noMuRaw * c_jecL1L2L3
        c_mask = (c_pt_L1L2L3 > 15) & (corrt1["EmEF"] < 0.9) if "EmEF" in corrt1.fields else c_pt_L1L2L3 > 15
        cd = vector.zip({"rho": c_pt_L1L2L3[c_mask], "phi": c_phi[c_mask]}) - vector.zip({"rho": c_pt_L1[c_mask], "phi": c_phi[c_mask]})
        c_corr = vector.zip({"x": ak.sum(cd.x, axis=1), "y": ak.sum(cd.y, axis=1)})
        return vector.zip({"rho": rawmet_pt, "phi": rawmet_phi}) - corr - c_corr

    manager = CalibratorsManager(
        calibrators_list=[JetsCalibrator, METCalibrator],
        events=events, params=params, metadata={"isMC": True, "year": "2018"},
    )
    met_calib = params.met_calibration["2018"]
    rawmet_branch = met_calib["RawMET_collection"]
    corrt1_branch = met_calib["CorrT1METJet_collection"]

    captured = {}
    for variation, ev in manager.calibration_loop(events, variations_for_calibrators=["jet_calibration"]):
        # The rawFactor must stay consistent with the (varied) pt so that consumers that
        # derive the JEC factor / raw pt from it (e.g. the type-1 MET below) are correct.
        jets = ev["Jet"]
        expected_rawfactor = ak.where(jets["pt"] != 0, 1 - jets["pt_raw"] / jets["pt"], 0)
        assert np.allclose(
            ak.to_numpy(ak.flatten(jets["rawFactor"])),
            ak.to_numpy(ak.flatten(expected_rawfactor)), rtol=1e-5, atol=1e-6,
        ), f"jet rawFactor is not consistent with the varied pt for variation {variation}"

        rec = independent_t1_met(ev[rawmet_branch]["pt"], ev[rawmet_branch]["phi"], ev["Jet"], ev[corrt1_branch])
        met = ev["MET"]
        assert np.allclose(ak.to_numpy(met.pt), ak.to_numpy(rec.rho), rtol=1e-4, atol=1e-2), \
            f"type-1 MET pt does not match the independent recomputation for variation {variation}"
        assert np.allclose(ak.to_numpy(met.phi), ak.to_numpy(rec.phi), rtol=1e-4, atol=1e-2), \
            f"type-1 MET phi does not match the independent recomputation for variation {variation}"
        captured[variation] = ak.to_numpy(met.pt)

    assert "nominal" in captured
    # The MET jet collection is "Jet" (AK4). Its JES/JER variations must propagate to the
    # type-1 MET; AK8 (FatJet) variations act on a different collection and must not.
    ak4_jes = [v for v in captured if "JES" in v and "AK4" in v]
    ak4_jer = [v for v in captured if "JER" in v and "AK4" in v]
    assert ak4_jes and ak4_jer, "expected AK4 JES and JER MET variations to be produced"
    for v in ak4_jes[:1] + ak4_jer[:1]:
        assert not np.allclose(captured[v], captured["nominal"]), f"MET variation {v} did not propagate"
    for v in [x for x in captured if "AK8" in x][:1]:
        assert np.allclose(captured[v], captured["nominal"]), \
            f"AK8 variation {v} must not change the AK4-jet-based MET"
