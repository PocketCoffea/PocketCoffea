import pytest
from pocket_coffea.lib.weights import WeightWrapper, WeightLambda, WeightData, WeightDataMultiVariation
from coffea.nanoevents import NanoEventsFactory, NanoAODSchema
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

@pytest.fixture(scope="module")
def events():
    #filename = "root://xrootd-cms.infn.it///store/mc/RunIISummer20UL18NanoAODv9/ttHTobb_M125_TuneCP5_13TeV-powheg-pythia8/NANOAODSIM/106X_upgrade2018_realistic_v16_L1v1-v2/2500000/6BF93845-49D5-2547-B860-4F7601074715.root"
    filename = "test_file.root"
    events = NanoEventsFactory.from_root(filename, schemaclass=NanoAODSchema, entry_stop=1000).events()
    return events

@pytest.fixture(scope="module")
def params():
    return get_default_parameters()

def test_genWeight(events):
    from pocket_coffea.lib.weights.common import common_weights
    genWeight = WeightWrapper.get_weight_class_from_name("genWeight")()
    w = genWeight.compute(events, 1000, "nominal")
    assert isinstance(w, WeightData)
    assert ak.all(w.nominal == events.genWeight)

def test_XS(events):
    from pocket_coffea.lib.weights.common import common_weights
    XS = WeightWrapper.get_weight_class_from_name("XS")(metadata={"xsec": 10.})
    w = XS.compute(events, 1000, "nominal")
    assert isinstance(w, WeightData)
    assert np.all(w.nominal == 10.)


def test_pileup(events, params):
    from pocket_coffea.lib.weights.common import common_weights
    pileup = WeightWrapper.get_weight_class_from_name("pileup")(params, metadata={"year": "2018"})
    pileup_here = sf_pileup_reweight(params, events, "2018")
    w = pileup.compute(events, 1000, "nominal")
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
    w = sf.compute(events, 1000, "nominal")
    assert isinstance(w, WeightData)    
    assert ak.all(w.nominal == sf_here[0])


def test_sf_ele_trigger(events, params):
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
    sf = WeightWrapper.get_weight_class_from_name("sf_ele_trigger")(params, metadata={"year": "2018"})

    # by default we should get a missing file expection
    with pytest.raises(omegaconf.errors.MissingMandatoryValue):
        w = sf.compute(events, 1000, "nominal")


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

    w = sf.compute(events, 1000, "nominal")
    assert isinstance(w, WeightDataMultiVariation)
    defined_variations = params.systematic_variations.weight_variations.sf_btag["2018"]
    assert w.variations == defined_variations
    assert ak.all(w.nominal == sf_btag(params, events["JetGood"], "2018", njets=events.nJetGood, variations=["central"])["central"][0])

    for i in range(len(defined_variations)):
        assert ak.all(w.up[i] == sf_btag(params, events["JetGood"], "2018", njets=events.nJetGood, variations=["central"]+defined_variations)[defined_variations[i]][1])
        assert ak.all(w.down[i] == sf_btag(params, events["JetGood"], "2018", njets=events.nJetGood, variations=["central"]+defined_variations)[defined_variations[i]][2])

   
