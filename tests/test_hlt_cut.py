from pocket_coffea.lib.cut_functions import get_HLTsel
from coffea.nanoevents import NanoEventsFactory, NanoAODSchema
from pocket_coffea.parameters.triggers import triggers
import awkward as ak

filename = "root://xrootd-cms.infn.it///store/mc/RunIISummer20UL18NanoAODv9/ttHTobb_M125_TuneCP5_13TeV-powheg-pythia8/NANOAODSIM/106X_upgrade2018_realistic_v16_L1v1-v2/2500000/6BF93845-49D5-2547-B860-4F7601074715.root"
events = NanoEventsFactory.from_root(filename, schemaclass=NanoAODSchema, entry_stop=1000).events()

def test_HLTsel():
    key = "semileptonic"
    cfg = triggers[key]

    hlt = get_HLTsel(key)
    mask = hlt.get_mask(events, year="2018", isMC=True)
    
    assert ak.sum(mask) >0


def test_HTLsel_inverse():
    key = "semileptonic"
    cfg = triggers[key]

    hlt = get_HLTsel(key, invert=True)
    mask = hlt.get_mask(events, year="2018", isMC=True)

    #Checking without the mask
    tot = 0
    for tr in  cfg["2018"]["SingleEle"]:
        tot += ak.sum(events.HLT[tr.lstrip("HLT_")])
    assert tot >0
    
    #Now checking one of the triggers
    for tr in  cfg["2018"]["SingleEle"]:
        assert ak.sum(events[mask].HLT[tr.lstrip("HLT_")]) == 0

    
    
