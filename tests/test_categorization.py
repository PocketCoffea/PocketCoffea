import pytest
from pocket_coffea.lib.categorization import MaskStorage
from tests.utils import events

import awkward as ak

def test_mask_storage_1D(events):
    #count the number of jets
    mask = ak.sum(events.Jet.pt > 10, axis=1)>3

    storage = MaskStorage(dim=1)
    storage.add("njets", mask)

    with pytest.raises(Exception):
        mask_jet = events.Jet.pt > 10
        storage.add("jetpt", mask_jet)


