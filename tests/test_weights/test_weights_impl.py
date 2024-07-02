import pytest
from pocket_coffea.lib.weights import WeightWrapper, WeightLambda, WeightData, WeightDataMultiVariation
from coffea.nanoevents import NanoEventsFactory, NanoAODSchema
from pocket_coffea.parameters.defaults import get_default_parameters
import awkward as ak
import numpy as np

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

@pytest.fixture
def events():
    filename = "root://xrootd-cms.infn.it///store/mc/RunIISummer20UL18NanoAODv9/ttHTobb_M125_TuneCP5_13TeV-powheg-pythia8/NANOAODSIM/106X_upgrade2018_realistic_v16_L1v1-v2/2500000/6BF93845-49D5-2547-B860-4F7601074715.root"
    events = NanoEventsFactory.from_root(filename, schemaclass=NanoAODSchema, entry_stop=1000).events()
    return events

@pytest.fixture
def params():
    return get_default_parameters()

def test_genWeight(events):
    from pocket_coffea.lib.weights.defaults.common import common_weights
    genWeight = WeightWrapper.get_weight_class_from_name("genWeight")()
    w = genWeight.compute(events, 1000, "nominal")
    assert ak.all(w.nominal == events.genWeight)

def test_lumi(events):
    from pocket_coffea.lib.weights.defaults.common import common_weights
    XS = WeightWrapper.get_weight_class_from_name("XS")(metadata={"xsec": 10.})
    w = XS.compute(events, 1000, "nominal")
    assert np.all(w.nominal == 10.)


def test_pileup(events, params):
    from pocket_coffea.lib.weights.defaults.common import common_weights
    pileup = WeightWrapper.get_weight_class_from_name("pileup")(params, metadata={"year": "2018"})
    pileup_here = sf_pileup_reweight(params, events, "2018")
    w = pileup.compute(events, 1000, "nominal")
    assert ak.all(w.nominal == pileup_here[0])
