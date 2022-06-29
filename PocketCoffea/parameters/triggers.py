# Trigger requirements

triggers = {
    "dilepton" : {
        "2016" : {
            "ee" : [
                "HLT_Ele23_Ele12_CaloIdL_TrackIdL_IsoVL_DZ",
                "HLT_Ele27_WPTight_Gsf",
            ],
            "emu" : [
                "HLT_Mu23_TrkIsoVVL_Ele12_CaloIdL_TrackIdL_IsoVL",
                "HLT_Mu23_TrkIsoVVL_Ele12_CaloIdL_TrackIdL_IsoVL_DZ",
                "HLT_Mu8_TrkIsoVVL_Ele23_CaloIdL_TrackIdL_IsoVL",
                "HLT_Mu8_TrkIsoVVL_Ele23_CaloIdL_TrackIdL_IsoVL_DZ",
                "HLT_Ele27_WPTight_Gsf",
                "HLT_IsoMu24",
                "HLT_IsoTkMu24",
            ],
            "mumu" : [
                "HLT_Mu17_TrkIsoVVL_Mu8_TrkIsoVVL",
                "HLT_Mu17_TrkIsoVVL_Mu8_TrkIsoVVL_DZ",
                "HLT_Mu17_TrkIsoVVL_TkMu8_TrkIsoVVL",
                "HLT_Mu17_TrkIsoVVL_TkMu8_TrkIsoVVL_DZ",
            ],
        },
        "2017" : {
            "ee" : [
                "HLT_Ele23_Ele12_CaloIdL_TrackIdL_IsoVL",
                "HLT_Ele23_Ele12_CaloIdL_TrackIdL_IsoVL_DZ",
                "HLT_Ele32_WPTight_Gsf",
            ],
            "emu" : [
                "HLT_Mu23_TrkIsoVVL_Ele12_CaloIdL_TrackIdL_IsoVL",
                "HLT_Mu23_TrkIsoVVL_Ele12_CaloIdL_TrackIdL_IsoVL_DZ",
                "HLT_Mu12_TrkIsoVVL_Ele23_CaloIdL_TrackIdL_IsoVL_DZ",
                "HLT_Mu8_TrkIsoVVL_Ele23_CaloIdL_TrackIdL_IsoVL_DZ",
                "HLT_Ele32_WPTight_Gsf",
                "HLT_IsoMu24_eta2p1",
                "HLT_IsoMu27",
            ],
            "mumu" : [
                "HLT_Mu17_TrkIsoVVL_Mu8_TrkIsoVVL_DZ",
                "HLT_Mu17_TrkIsoVVL_Mu8_TrkIsoVVL_DZ_Mass3p8",
                "HLT_IsoMu24_eta2p1",
                "HLT_IsoMu27",
            ],
        },
        "2018" : {
            "ee" : [
                "HLT_Ele23_Ele12_CaloIdL_TrackIdL_IsoVL",
                "HLT_Ele23_Ele12_CaloIdL_TrackIdL_IsoVL_DZ",
                "HLT_Ele32_WPTight_Gsf",
            ],
            "emu" : [
                "HLT_Mu23_TrkIsoVVL_Ele12_CaloIdL_TrackIdL_IsoVL",
                "HLT_Mu23_TrkIsoVVL_Ele12_CaloIdL_TrackIdL_IsoVL_DZ",
                "HLT_Mu12_TrkIsoVVL_Ele23_CaloIdL_TrackIdL_IsoVL_DZ",
                "HLT_Mu8_TrkIsoVVL_Ele23_CaloIdL_TrackIdL_IsoVL_DZ",
                "HLT_Ele32_WPTight_Gsf",
                "HLT_IsoMu24",
            ],
            "mumu" : [
                "HLT_Mu17_TrkIsoVVL_Mu8_TrkIsoVVL_DZ_Mass8",
                "HLT_Mu17_TrkIsoVVL_Mu8_TrkIsoVVL_DZ_Mass3p8",
                "HLT_IsoMu24",
            ],
        },
    },
    "semileptonic" : {
        "2016" : {
            "e" : [
                "Ele27_WPTight_Gsf",
                "Ele27_WPTight_Gsf_L1JetTauSeeded"
            ],
            "mu" : [
                "IsoMu24",
                "IsoTkMu24"
            ]
        },
        "2017" : {
            "e" : [
                "Ele32_WPTight_Gsf_L1DoubleEG",
                "Ele28_eta2p1_WPTight_Gsf_HT150"
            ],
            "e_flag" : [
            ],
            "mu" : [
                "IsoMu27",
            ]
        },
        "2018" : {
            "e" : [
                "Ele32_WPTight_Gsf",
                "Ele32_WPTight_Gsf_L1DoubleEG",
                "Ele28_eta2p1_WPTight_Gsf_HT150"
            ],
            "mu" : [
                "IsoMu24_eta2p1_TightChargedIsoPFTauHPS35_Trk1_eta2p1_Reg_CrossL1",
                "IsoMu24_eta2p1_MediumChargedIsoPFTauHPS35_Trk1_TightID_eta2p1_Reg_CrossL1",
                "IsoMu24_eta2p1_TightChargedIsoPFTauHPS35_Trk1_TightID_eta2p1_Reg_CrossL1",
                "IsoMu24_eta2p1_MediumChargedIsoPFTauHPS35_Trk1_eta2p1_Reg_CrossL1",
                "IsoMu24",
                "IsoMu24_eta2p1",
                "IsoMu24_TwoProngs35",
                "IsoMu24_eta2p1_MediumChargedIsoPFTau50_Trk30_eta2p1_1pr"
            ]
        }
    },
    "semileptonic_triggerSF" : {
        "2016" : {
            "mu" : ["IsoMu24"]
        },
        "2017" : {
            "mu" : ["IsoMu27"]
        },
        "2018" : {
            "mu" : ["IsoMu24"]
        },
    }
}
