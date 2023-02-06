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
        'file_triggerSF': "/work/mmarcheg/PocketCoffea/output/sf_ele_trigger_semilep/semileptonic_triggerSF_2018_optimized_binning_v06/semileptonic_triggerSF/triggerSF_2018_optimized_binning/sf_trigger_electron_etaSC_pt_leading_2018_Ele32_EleHT_pass_v06.json",
        'name': "UL-Electron-ID-SF",
    },
}

muonSF = {
    '2016_PreVFP': {
        'id': "NUM_TightID_DEN_TrackerMuons",
        'iso': "NUM_LooseRelIso_DEN_TightIDandIPCut",
        'trigger': "NUM_IsoMu24_or_IsoTkMu24_DEN_CutBasedIdTight_and_PFIsoTight",
    },
    '2016_PostVFP': {
        'id': "NUM_TightID_DEN_TrackerMuons",
        'iso': "NUM_LooseRelIso_DEN_TightIDandIPCut",
        'trigger': "NUM_IsoMu24_or_IsoTkMu24_DEN_CutBasedIdTight_and_PFIsoTight",
    },
    '2017': {
        'id': "NUM_TightID_DEN_TrackerMuons",
        'iso': "NUM_LooseRelIso_DEN_TightIDandIPCut",
        'trigger': "NUM_IsoMu27_DEN_CutBasedIdTight_and_PFIsoTight",
    },
    '2018': {
        'id': "NUM_TightID_DEN_TrackerMuons",
        'iso': "NUM_LooseRelIso_DEN_TightIDandIPCut",
        'trigger': "NUM_IsoMu24_DEN_CutBasedIdTight_and_PFIsoTight",
    },
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
