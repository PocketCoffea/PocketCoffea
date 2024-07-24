import pytest
from pocket_coffea.lib.cut_functions import get_HLTsel
from pocket_coffea.lib.cut_definition import Cut
from coffea.nanoevents import NanoEventsFactory, NanoAODSchema
from pocket_coffea.parameters import defaults
import awkward as ak

@pytest.fixture(scope="module")
def events():
    filename = "root://xrootd-cms.infn.it///store/mc/RunIISummer20UL18NanoAODv9/ttHTobb_M125_TuneCP5_13TeV-powheg-pythia8/NANOAODSIM/106X_upgrade2018_realistic_v16_L1v1-v2/2500000/6BF93845-49D5-2547-B860-4F7601074715.root"
    events = NanoEventsFactory.from_root(filename, schemaclass=NanoAODSchema, entry_stop=1000).events()
    return events

@pytest.fixture(scope="module")
def params():
    params = defaults.get_default_parameters()
    # Adding triggers
    params["HLT_triggers"] = {
        "2018":
        {
            "SingleEle": ["Ele32_WPTight_Gsf", "Ele28_eta2p1_WPTight_Gsf_HT150"],
            "SingleMu": ["IsoMu24"]
        }
    }
    return params




def test_HTLsel(events, params):
    hlt = get_HLTsel()
    mask = hlt.get_mask(events, params, year="2018", isMC=True)

    #Checking without the mask
    tot = 0
    cfg = params.HLT_triggers
    for tr in  cfg["2018"]["SingleEle"]:
        tot += ak.sum(events.HLT[tr.lstrip("HLT_")])
    assert tot >0

    #Checking the function
    assert ak.sum(mask) > 0
    assert ak.all(events[mask].event == events[(events.HLT.Ele32_WPTight_Gsf)|(events.HLT.Ele28_eta2p1_WPTight_Gsf_HT150)|
                                                  (events.HLT.IsoMu24)].event)
    
def test_HLTsel_primaryDatasets(events, params):
    hlt_singleEle = get_HLTsel(primaryDatasets=["SingleEle"])
    mask_ele = hlt_singleEle.get_mask(events, params, year="2018", isMC=True)

    assert ak.all(events[mask_ele].event == events[(events.HLT.Ele32_WPTight_Gsf)|(events.HLT.Ele28_eta2p1_WPTight_Gsf_HT150)].event)

     


# def test_HTLsel_primaryDatasets():
#     key = "semileptonic"
#     cfg = triggers[key]

#     # Ask to NOT fire singleMu
#     hlt = get_HLTsel(key, invert=True, primaryDatasets=["SingleMu"])
#     mask = hlt.get_mask(events, year="2018", isMC=True)

#     #Checking without the mask
#     tot = 0
#     tot_mask = 0
#     for tr in  cfg["2018"]["SingleMu"]:
#         tot += ak.sum(events.HLT[tr.lstrip("HLT_")])
#         tot_mask += ak.sum(events[mask].HLT[tr.lstrip("HLT_")])
#     assert tot != 0
#     assert tot_mask == 0
    


    
