from os import path

btagSF = {
    # DeepJet AK4 tagger shape SF
    # https://cms-nanoaod-integration.web.cern.ch/commonJSONSFs/BTV_btagging_Run2_UL/BTV_btagging_2018_UL.html
    '2016preVFP' : "/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/BTV/2016preVFP_UL/btagging.json.gz",
    '2016postVFP' : "/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/BTV/2016postVFP_UL/btagging.json.gz",
    '2017' : "/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/BTV/2017_UL/btagging.json.gz",
    '2018' : "/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/BTV/2018_UL/btagging.json.gz",
}

btagSF_calibration = {
    '2016preVFP': "",
    '2016postVFP': "",
    '2017': "",
    '2018' : path.join(path.dirname(__file__), "btag_SF_calibration","btagSF_calibration_allyears_v2.json")
}

jet_puId = {
    # Jet PU ID SF to be applied only on selected jets (pt<50) that are matched to GenJets
    #https://cms-nanoaod-integration.web.cern.ch/commonJSONSFs/JME_jmar_Run2_UL/JME_jmar_2018_UL.html
    '2016preVFP' : "/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/JME/2016preVFP_UL/jmar.json.gz",
    '2016postVFP' : "/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/JME/2016postVFP_UL/jmar.json.gz",
    '2017' : "/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/JME/2017_UL/jmar.json.gz",
    '2018' : "/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/JME/2018_UL/jmar.json.gz",
}
