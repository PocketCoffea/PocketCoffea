# Trigger requirements

triggers = {
    "dilepton": {
        "2016": {
            "DoubleEle": [
                "HLT_Ele23_Ele12_CaloIdL_TrackIdL_IsoVL_DZ",
                "HLT_Ele27_WPTight_Gsf",
            ],
            "EleMu": [
                "HLT_Mu23_TrkIsoVVL_Ele12_CaloIdL_TrackIdL_IsoVL",
                "HLT_Mu23_TrkIsoVVL_Ele12_CaloIdL_TrackIdL_IsoVL_DZ",
                "HLT_Mu8_TrkIsoVVL_Ele23_CaloIdL_TrackIdL_IsoVL",
                "HLT_Mu8_TrkIsoVVL_Ele23_CaloIdL_TrackIdL_IsoVL_DZ",
                "HLT_Ele27_WPTight_Gsf",
                "HLT_IsoMu24",
                "HLT_IsoTkMu24",
            ],
            "DoubleMu": [
                "HLT_Mu17_TrkIsoVVL_Mu8_TrkIsoVVL",
                "HLT_Mu17_TrkIsoVVL_Mu8_TrkIsoVVL_DZ",
                "HLT_Mu17_TrkIsoVVL_TkMu8_TrkIsoVVL",
                "HLT_Mu17_TrkIsoVVL_TkMu8_TrkIsoVVL_DZ",
            ],
        },
        "2017": {
            "DoubleEle": [
                "HLT_Ele23_Ele12_CaloIdL_TrackIdL_IsoVL",
                "HLT_Ele23_Ele12_CaloIdL_TrackIdL_IsoVL_DZ",
                "HLT_Ele32_WPTight_Gsf",
            ],
            "EleMu": [
                "HLT_Mu23_TrkIsoVVL_Ele12_CaloIdL_TrackIdL_IsoVL",
                "HLT_Mu23_TrkIsoVVL_Ele12_CaloIdL_TrackIdL_IsoVL_DZ",
                "HLT_Mu12_TrkIsoVVL_Ele23_CaloIdL_TrackIdL_IsoVL_DZ",
                "HLT_Mu8_TrkIsoVVL_Ele23_CaloIdL_TrackIdL_IsoVL_DZ",
                "HLT_Ele32_WPTight_Gsf",
                "HLT_IsoMu24_eta2p1",
                "HLT_IsoMu27",
            ],
            "DoubleMu": [
                "HLT_Mu17_TrkIsoVVL_Mu8_TrkIsoVVL_DZ",
                "HLT_Mu17_TrkIsoVVL_Mu8_TrkIsoVVL_DZ_Mass3p8",
                "HLT_IsoMu24_eta2p1",
                "HLT_IsoMu27",
            ],
        },
        "2018": {
            "DoubleEle": [
                "HLT_Ele23_Ele12_CaloIdL_TrackIdL_IsoVL",
                "HLT_Ele23_Ele12_CaloIdL_TrackIdL_IsoVL_DZ",
                "HLT_Ele32_WPTight_Gsf",
            ],
            "EleMu": [
                "HLT_Mu23_TrkIsoVVL_Ele12_CaloIdL_TrackIdL_IsoVL",
                "HLT_Mu23_TrkIsoVVL_Ele12_CaloIdL_TrackIdL_IsoVL_DZ",
                "HLT_Mu12_TrkIsoVVL_Ele23_CaloIdL_TrackIdL_IsoVL_DZ",
                "HLT_Mu8_TrkIsoVVL_Ele23_CaloIdL_TrackIdL_IsoVL_DZ",
                "HLT_Ele32_WPTight_Gsf",
                "HLT_IsoMu24",
            ],
            "DoubleMu": [
                "HLT_Mu17_TrkIsoVVL_Mu8_TrkIsoVVL_DZ_Mass8",
                "HLT_Mu17_TrkIsoVVL_Mu8_TrkIsoVVL_DZ_Mass3p8",
                "HLT_IsoMu24",
            ],
        },
    },
    "semileptonic": {
        "2016": {
            "SingleEle": [
                "Ele27_WPTight_Gsf",
            ],
            "SingleMu": ["IsoMu24", "IsoTkMu24"],
        },
        "2017": {
            "SingleEle": [
                "Ele32_WPTight_Gsf_L1DoubleEG",
                "Ele28_eta2p1_WPTight_Gsf_HT150",
            ],
            "SingleMu": [
                "IsoMu27",
            ],
        },
        "2018": {
            "SingleEle": [
                "Ele32_WPTight_Gsf",
                "Ele28_eta2p1_WPTight_Gsf_HT150",
            ],
            "SingleMu": [
                "IsoMu24",
            ],
        },
    },
}


### Customizations
triggers["semileptonic_partonmatching"] = triggers["semileptonic"]
