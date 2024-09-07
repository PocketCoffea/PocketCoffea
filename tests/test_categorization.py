import pytest
from pocket_coffea.lib.categorization import MaskStorage

import awkward as ak
from utils import events

def test_mask_storage_1D(events):
    #count the number of jets
    mask = ak.sum(events.Jet.pt > 10, axis=1)>3

    storage = MaskStorage(dim=1)
    storage.add("njets", mask)

    with pytest.raises(Exception):
        mask_jet = events.Jet.pt > 10
        storage.add("jetpt", mask_jet)


