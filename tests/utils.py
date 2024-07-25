import pytest
import os
from coffea.nanoevents import NanoEventsFactory, NanoAODSchema


@pytest.fixture(scope="session")
def events():
    filename = "root://eoscms.cern.ch//eos/cms/store/mc/RunIISummer20UL18NanoAODv9/TTTo2L2Nu_TuneCP5_13TeV-powheg-pythia8/NANOAODSIM/106X_upgrade2018_realistic_v16_L1v1-v1/280000/01881676-C30A-2142-B3B2-7A8449DAF8EF.root"

    print(filename)
    events = NanoEventsFactory.from_root(filename, schemaclass=NanoAODSchema, entry_stop=1000).events()
    return events
