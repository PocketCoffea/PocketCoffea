
import pytest
import os
from pocket_coffea.lib.calibrators.common.common import (
    JetsCalibrator, 
    METCalibrator, 
    ElectronsScaleCalibrator, 
    MuonsCalibrator)
from pocket_coffea.lib.calibrators.calibrator import Calibrator
from pocket_coffea.lib.calibrators.calibrators_manager import CalibratorsManager
from pocket_coffea.utils import build_jets_calibrator

from pocket_coffea.parameters import defaults
import awkward as ak
from utils import events


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
    if not os.path.exists(params.jets_calibration.factory_file):
        build_jets_calibrator.build(params.jets_calibration)

    from pocket_coffea.lib.jets import load_jet_factory
    jmefactory = load_jet_factory(params)
    jets_calibrator = JetsCalibrator(params=params, metadata={"isMC": True, "year": "2018"},
                                     do_variations=False,
                                     jme_factory=jmefactory)
    jets_calibrator.initialize(events)

    orig_events = ak.copy(events)
    out1 = jets_calibrator.calibrate(events, {}, variation="nominal")

    assert "Jet" in out1
    variation = jets_calibrator.variations[0]
    out2 = jets_calibrator.calibrate(events, {}, variation=variation)

    assert "Jet" in out2
    assert ak.all(out2["Jet"].pt != orig_events.Jet.pt)
    assert ak.all(out2["Jet"].pt != out1["Jet"].pt)