# B-tagging algorithms and working points

btag = {
    # N.B: ALL THESE VALUES NEED TO BE CHECKED AND UPDATED
    "2016_PreVFP": {
        "btagging_algorithm": "btagDeepFlavB",  # "btagDeepB",
        "btagging_WP": 0.3093,
        "bbtagging_algorithm": "btagDDBvL",  # "deepTagMD_HbbvsQCD", #"deepTagMD_bbvsLight",
        "bbtagging_WP": 0.86,
        'bbtagSF_DDBvL_M1_loPt': 0.86,
        'bbtagSF_DDBvL_M1_loPt_up': 0.97,
        'bbtagSF_DDBvL_M1_loPt_down': 0.82,
        'bbtagSF_DDBvL_M1_hiPt': 0.86,
        'bbtagSF_DDBvL_M1_hiPt_up': 0.97,
        'bbtagSF_DDBvL_M1_hiPt_down': 0.82,
    },
    "2016_PostVFP": {
        "btagging_algorithm": "btagDeepFlavB",  # "btagDeepB",
        "btagging_WP": 0.3093,
        "bbtagging_algorithm": "btagDDBvL",  # "deepTagMD_HbbvsQCD", #"deepTagMD_bbvsLight",
        "bbtagging_WP": 0.86,
        'bbtagSF_DDBvL_M1_loPt': 0.86,
        'bbtagSF_DDBvL_M1_loPt_up': 0.97,
        'bbtagSF_DDBvL_M1_loPt_down': 0.82,
        'bbtagSF_DDBvL_M1_hiPt': 0.86,
        'bbtagSF_DDBvL_M1_hiPt_up': 0.97,
        'bbtagSF_DDBvL_M1_hiPt_down': 0.82,
    },
    "2017": {
        "btagging_algorithm": "btagDeepFlavB",  # "btagDeepB",
        "btagging_WP": 0.3033,  # 0.4941, # medium working point for btagDeepB
        "bbtagging_algorithm": "btagDDBvL",  # "deepTagMD_HbbvsQCD", #"deepTagMD_bbvsLight",
        "bbtagging_WP": 0.86,  ## https://indico.cern.ch/event/853828/contributions/3723593/attachments/1977626/3292045/lg-btv-deepak8v2-sf-20200127.pdf
        'bbtagSF_DDBvL_M1_loPt': 0.82,
        'bbtagSF_DDBvL_M1_loPt_up': 0.86,
        'bbtagSF_DDBvL_M1_loPt_down': 0.77,
        'bbtagSF_DDBvL_M1_hiPt': 0.77,
        'bbtagSF_DDBvL_M1_hiPt_up': 0.83,
        'bbtagSF_DDBvL_M1_hiPt_down': 0.67,
    },
    "2018": {
        "btagging_algorithm": "btagDeepFlavB",  # "btagDeepB",
        "btagging_WP": 0.2770,  # medium working point for btagDeepB
        "bbtagging_algorithm": "btagDDBvL",  # "deepTagMD_HbbvsQCD", #"deepTagMD_bbvsLight",
        "bbtagging_WP": 0.86,
        'bbtagSF_DDBvL_M1_loPt': 0.81,
        'bbtagSF_DDBvL_M1_loPt_up': 0.88,
        'bbtagSF_DDBvL_M1_loPt_down': 0.76,
        'bbtagSF_DDBvL_M1_hiPt': 0.76,
        'bbtagSF_DDBvL_M1_hiPt_up': 0.82,
        'bbtagSF_DDBvL_M1_hiPt_down': 0.71,
    },
}

btag_variations = {
    "2016preVFP": [
        "hf",
        "lf",
        "hfstats1",
        "hfstats2",
        "lfstats1",
        "lfstats2",
        "cferr1",
        "cferr2",
    ],
    "2016postFP": [
        "hf",
        "lf",
        "hfstats1",
        "hfstats2",
        "lfstats1",
        "lfstats2",
        "cferr1",
        "cferr2",
    ],
    "2017": [
        "hf",
        "lf",
        "hfstats1",
        "hfstats2",
        "lfstats1",
        "lfstats2",
        "cferr1",
        "cferr2",
    ],
    "2018": [
        "hf",
        "lf",
        "hfstats1",
        "hfstats2",
        "lfstats1",
        "lfstats2",
        "cferr1",
        "cferr2",
    ],
}
