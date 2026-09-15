import pytest
from tests.utils import events

from pocket_coffea.lib.cut_functions import *


def test_cut_Nobjs(events):

    mask = min_nObj(events,
                    {"coll":"Jet",
                     "N": 4})
    mymask = ak.num(events.Jet, axis=1) >= 4
    assert ak.all(mask == mymask)

    mask = min_nObj_minPt(events,
                            {"coll":"Jet",
                             "N": 4,
                             "minpt": 30})
    mymask = (ak.sum(events.Jet.pt >= 30, axis=1) >= 4)
    assert ak.all(mask == mymask)

    mask = eq_nObj(events,
                    {"coll":"Jet",
                     "N": 4})
    mymask = ak.num(events.Jet, axis=1) == 4
    assert ak.all(mask == mymask)

    mask = eq_nObj_minPt(events,
                            {"coll":"Jet",
                             "N": 4,
                             "minpt": 30})
    mymask = (ak.sum(events.Jet.pt >= 30, axis=1) == 4)
    assert ak.all(mask == mymask)


    
