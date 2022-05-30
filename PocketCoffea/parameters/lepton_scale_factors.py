electronSF = {
    'reco' : {
        'pt>20' : "RecoAbove20",
        'pt<20' : "RecoBelow20"
    },
    'id' : "wp80iso"
}

electronJSONfiles = {
    '2016preVFP' : {
        'file' : "/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/EGM/2016preVFP_UL/electron.json.gz",
        'name' : "UL-Electron-ID-SF"
    },
    '2016postVFP' : {
        'file' : "/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/EGM/2016postVFP_UL/electron.json.gz",
        'name' : "UL-Electron-ID-SF"
    },
    '2017' : {
        'file' : "/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/EGM/2017_UL/electron.json.gz",
        'name' : "UL-Electron-ID-SF"
    },
    '2018' : {
        'file' : "/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/EGM/2018_UL/electron.json.gz",
        'name' : "UL-Electron-ID-SF"
    }
}

muonSF = {
    'id'  : "NUM_TightID_DEN_TrackerMuons",
    'iso' : "NUM_LooseRelIso_DEN_TightIDandIPCut",
}

muonJSONfiles = {
    '2016preVFP' : {
        'file' : "/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/MUO/2017postVFP_UL/muon_Z.json.gz",
    },
    '2016postVFP' : {
        'file' : "/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/MUO/2017preVFP_UL/muon_Z.json.gz",
    },
    '2017' : {
        'file' : "/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/MUO/2017_UL/muon_Z.json.gz",
    },
    '2018' : {
        'file' : "/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/MUO/2018_UL/muon_Z.json.gz",
    }
}
