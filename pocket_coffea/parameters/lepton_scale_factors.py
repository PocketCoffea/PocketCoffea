electronSF = {
    'reco': {'pt>20': "RecoAbove20", 'pt<20': "RecoBelow20"},
    'id': "wp80iso",
    'trigger': {'2018': "sf_Ele32_EleHT"},
}

electronJSONfiles = {
    '2016_PreVFP': {
        'file_POG': "/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/EGM/2016preVFP_UL/electron.json.gz",
        'file_triggerSF': "",
        'name': "UL-Electron-ID-SF",
    },
    '2016_PostVFP': {
        'file_POG': "/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/EGM/2016postVFP_UL/electron.json.gz",
        'file_triggerSF': "",
        'name': "UL-Electron-ID-SF",
    },
    '2017': {
        'file_POG': "/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/EGM/2017_UL/electron.json.gz",
        'file_triggerSF': "",
        'name': "UL-Electron-ID-SF",
    },
    '2018': {
        'file_POG': "/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/EGM/2018_UL/electron.json.gz",
        'file_triggerSF': "/work/mmarcheg/PocketCoffea/PocketCoffea/parameters/semileptonic_triggerSF/triggerSF_2018_Ele32_EleHT_allsystematics/sf_trigger_electron_etaSC_vs_electron_pt_2018_Ele32_EleHT_pass_v08.json",
        'name': "UL-Electron-ID-SF",
    },
}

muonSF = {
    'id': "NUM_TightID_DEN_TrackerMuons",
    'iso': "NUM_LooseRelIso_DEN_TightIDandIPCut",
}

muonJSONfiles = {
    '2016_PreVFP': {
        'file': "/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/MUO/2016postVFP_UL/muon_Z.json.gz",
    },
    '2016_PostVFP': {
        'file': "/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/MUO/2016preVFP_UL/muon_Z.json.gz",
    },
    '2017': {
        'file': "/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/MUO/2017_UL/muon_Z.json.gz",
    },
    '2018': {
        'file': "/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/MUO/2018_UL/muon_Z.json.gz",
    },
}
