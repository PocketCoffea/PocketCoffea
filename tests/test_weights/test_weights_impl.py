import pytest
from pocket_coffea.lib.weights import WeightWrapper, WeightLambda, WeightData, WeightDataMultiVariation
from pocket_coffea.parameters.defaults import get_default_parameters
import awkward as ak
import numpy as np
import omegaconf

from pocket_coffea.lib.scale_factors import (
    sf_ele_reco,
    sf_ele_id,
    sf_ele_trigger,
    sf_mu,
    sf_btag,
    sf_btag_calib,
    sf_ctag,
    sf_ctag_calib,
    sf_jet_puId,
    sf_L1prefiring,
    sf_pileup_reweight,
)
from pocket_coffea.lib.objects import lepton_selection, jet_selection
import os
from coffea.nanoevents import NanoEventsFactory, NanoAODSchema

@pytest.fixture(scope="session")
def events():
    filename = "root://eoscms.cern.ch//eos/cms/store/mc/RunIISummer20UL18NanoAODv9/TTTo2L2Nu_TuneCP5_13TeV-powheg-pythia8/NANOAODSIM/106X_upgrade2018_realistic_v16_L1v1-v1/280000/01881676-C30A-2142-B3B2-7A8449DAF8EF.root"
    events = NanoEventsFactory.from_root(filename, schemaclass=NanoAODSchema, entry_stop=100).events()
    return events


@pytest.fixture(scope="module")
def params():
    return get_default_parameters()

def test_genWeight(events):
    from pocket_coffea.lib.weights.common import common_weights
    genWeight = WeightWrapper.get_weight_class_from_name("genWeight")()
    w = genWeight.compute(events, 100, "nominal")
    assert isinstance(w, WeightData)
    assert ak.all(w.nominal == events.genWeight)

def test_XS(events):
    from pocket_coffea.lib.weights.common import common_weights
    XS = WeightWrapper.get_weight_class_from_name("XS")(metadata={"xsec": 10.})
    w = XS.compute(events, 100, "nominal")
    assert isinstance(w, WeightData)
    assert np.all(w.nominal == 10.)


def test_pileup(events, params):
    from pocket_coffea.lib.weights.common import common_weights
    pileup = WeightWrapper.get_weight_class_from_name("pileup")(params, metadata={"year": "2018"})
    pileup_here = sf_pileup_reweight(params, events, "2018")
    w = pileup.compute(events, 100, "nominal")
    assert isinstance(w, WeightData)
    assert ak.all(w.nominal == pileup_here[0])


def test_sf_ele_id(events, params):
    from pocket_coffea.lib.weights.common import common_weights
    events["Electron"] = ak.with_field(
            events.Electron,
            events.Electron.eta+events.Electron.deltaEtaSC, "etaSC"
        )
    params["object_preselection"] = {
        "Electron":
        {"pt": 15, "eta": 2.4, "id": "mvaFall17V2Iso_WP80"}
    }
    events["ElectronGood"] = lepton_selection(events, "Electron", params)
    
    sf = WeightWrapper.get_weight_class_from_name("sf_ele_id")(params, metadata={"year": "2018"})
    sf_here = sf_ele_id(params, events, "2018")
    w = sf.compute(events, 100, "nominal")
    assert isinstance(w, WeightData)    
    assert ak.all(w.nominal == sf_here[0])


def test_sf_ele_trigger(events, params):
    from pocket_coffea.lib.weights.common import weights_run3
    events["Electron"] = ak.with_field(
            events.Electron,
            events.Electron.eta+events.Electron.deltaEtaSC, "etaSC"
        )
    params["object_preselection"] = {
        "Electron":
        {"pt": 15, "eta": 2.4, "id": "mvaFall17V2Iso_WP80"}
    }
    events["ElectronGood"] = lepton_selection(events, "Electron", params)
    sf = WeightWrapper.get_weight_class_from_name("sf_ele_trigger")(params, metadata={"year": "2018"})

    # by default we should get a missing file expection
    with pytest.raises(omegaconf.errors.MissingMandatoryValue):
        w = sf.compute(events, 100, "nominal")


def test_sf_btag(events, params):
    from pocket_coffea.lib.weights.common import common_weights
    params["object_preselection"] = {
        "Jet":
        {
            "pt": 30,
            "eta": 2.4,
            "jetId": 2,
            "puId": {"wp": "L", "value": 4, "maxpt": 50.0},
            "btag": {"wp": "M"}
        }
    }
    events["JetGood"], _ = jet_selection(events, "Jet", params, "2018")
    events["nJetGood"] = ak.num(events.JetGood)
    sf = WeightWrapper.get_weight_class_from_name("sf_btag")(params, metadata={"year": "2018"})

    w = sf.compute(events, 100, "nominal")
    assert isinstance(w, WeightDataMultiVariation)
    defined_variations = params.systematic_variations.weight_variations.sf_btag["2018"]
    assert w.variations == defined_variations
    assert ak.all(w.nominal == sf_btag(params, events["JetGood"], "2018", njets=events.nJetGood, variations=["central"])["central"][0])

    for i in range(len(defined_variations)):
        assert ak.all(w.up[i] == sf_btag(params, events["JetGood"], "2018", njets=events.nJetGood, variations=["central"]+defined_variations)[defined_variations[i]][1])
        assert ak.all(w.down[i] == sf_btag(params, events["JetGood"], "2018", njets=events.nJetGood, variations=["central"]+defined_variations)[defined_variations[i]][2])


def test_sfbtag_coffea_multivariations(events, params):
    from pocket_coffea.lib.weights.common import common_weights
    params["object_preselection"] = {
        "Jet":
        {
            "pt": 30,
            "eta": 2.4,
            "jetId": 2,
            "puId": {"wp": "L", "value": 4, "maxpt": 50.0},
            "btag": {"wp": "M"}
        }
    }
    events["JetGood"], _ = jet_selection(events, "Jet", params, "2018")
    events["nJetGood"] = ak.num(events.JetGood)
    sf = WeightWrapper.get_weight_class_from_name("sf_btag")(params, metadata={"year": "2018"})

    w = sf.compute(events, 100, "nominal")

    defined_variations = params.systematic_variations.weight_variations.sf_btag["2018"]
    raw_SF = sf_btag(params, events["JetGood"], "2018", njets=events.nJetGood,
                     variations=["central"]+defined_variations)

    assert isinstance(w, WeightDataMultiVariation)

    from coffea.analysis_tools import Weights
    weights1 = Weights(len(events))
    weights2 = Weights(len(events))

    from copy import deepcopy
    w_copy = deepcopy(w)
    weights1.add_multivariation("sf_btag",
                                w_copy.nominal, w_copy.variations,
                                w_copy.up, w_copy.down)
    # Using the old "trick" of rescaling the variations
    weights2.add("sf_btag", w.nominal)
    for i in range(len(w.variations)):
        weights2.add(f"sf_btag_{w.variations[i]}", np.ones(100),
                     w.up[i]/w.nominal, w.down[i]/w.nominal) 

    # Check with raw SF values
    for var in w.variations:
        assert np.allclose(weights1.weight(f"sf_btag_{var}Up"), raw_SF[var][1])
        assert np.allclose(weights1.weight(f"sf_btag_{var}Down"), raw_SF[var][2])
        assert np.allclose(weights2.weight(f"sf_btag_{var}Up"), raw_SF[var][1])
        assert np.allclose(weights2.weight(f"sf_btag_{var}Down"), raw_SF[var][2])

    assert np.all(weights1.weight() == weights2.weight())
    for var in w.variations:
        assert np.all(weights1.weight(f"sf_btag_{var}Up") == weights2.weight(f"sf_btag_{var}Up"))
        assert np.all(weights1.weight(f"sf_btag_{var}Down") == weights2.weight(f"sf_btag_{var}Down"))

    
    
