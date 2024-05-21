# Per-event cuts applied to each event
import awkward as ak
import custom_cut_functions as cuts_f
from pocket_coffea.lib.cut_definition import Cut


dilepton_presel = Cut(
    name="dilepton",
    params={
        "METbranch": {
            '2016_PreVFP': "MET",
            '2016_PostVFP': "MET",
            '2017': "MET",
            '2018': "MET",
        },
        "njet": 2,
        "nbjet": 1,
        "pt_leading_lepton": 25,
        "met": 40,
        "mll": 20,
        "mll_SFOS": {'low': 76, 'high': 106},
    },
    function=cuts_f.dilepton,
)

semileptonic_presel = Cut(
    name="semileptonic",
    params={
        "METbranch": {
            '2016_PreVFP': "MET",
            '2016_PostVFP': "MET",
            '2017': "MET",
            '2018': "MET",
        },
        "njet": 4,
        "nbjet": 3,
        "pt_leading_electron": {
            '2016_PreVFP': 29,
            '2016_PostVFP': 29,
            '2017': 30,
            '2018': 30,
        },
        "pt_leading_muon": {
            '2016_PreVFP': 26,
            '2016_PostVFP': 26,
            '2017': 29,
            '2018': 26,
        },
        "met": 20,
    },
    function=cuts_f.semileptonic,
)

# Same preselection as the standard semileptonic but without
# requirements on the number of btagged jets
# --> used for btagSF normalization calibration
semileptonic_presel_nobtag = Cut(
    name="semileptonic_nobtag",
    params={
        "METbranch": {
            '2016_PreVFP': "MET",
            '2016_PostVFP': "MET",
            '2017': "MET",
            '2018': "MET",
        },
        "njet": 4,
        "nbjet": 0,
        "pt_leading_electron": {
            '2016_PreVFP': 29,
            '2016_PostVFP': 29,
            '2017': 30,
            '2018': 30,
        },
        "pt_leading_muon": {
            '2016_PreVFP': 26,
            '2016_PostVFP': 26,
            '2017': 29,
            '2018': 26,
        },
        "met": 20,
    },
    function=cuts_f.semileptonic,
)

semileptonic_triggerSF_presel = Cut(
    name="semileptonic_triggerSF",
    params={
        "njet": 4,
        "pt_leading_electron": {
             '2016_PreVFP': 29,
            '2016_PostVFP': 29,
            '2017': 30,
            '2018': 30,
        },
        "pt_leading_muon": {
            '2016_PreVFP': 26,
            '2016_PostVFP': 26,
            '2017': 29,
            '2018': 26,
        },
    },
    function=cuts_f.semileptonic_triggerSF,
)
