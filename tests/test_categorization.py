import pytest
from pocket_coffea.lib.categorization import MaskStorage
from coffea.nanoevents import NanoEventsFactory, NanoAODSchema
import awkward as ak

filename = "root://xrootd-cms.infn.it///store/mc/RunIISummer20UL18NanoAODv9/ttHTobb_M125_TuneCP5_13TeV-powheg-pythia8/NANOAODSIM/106X_upgrade2018_realistic_v16_L1v1-v2/2500000/6BF93845-49D5-2547-B860-4F7601074715.root"
events = NanoEventsFactory.from_root(filename, schemaclass=NanoAODSchema, entry_stop=10000).events()

def test_mask_storage_1D():
    #count the number of jets
    mask = ak.sum(events.Jet.pt > 10, axis=1)>3

    storage = MaskStorage(len(events), dim=1)
    storage.add("njets", mask)

    with pytest.raises(Exception):
        mask_jet = events.Jet.pt > 10
        storage.add("jetpt", mask_jet)


