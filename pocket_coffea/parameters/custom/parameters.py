import numpy as np

# Luminosity in fb^-1
lumi = {
    # https://twiki.cern.ch/twiki/bin/viewauth/CMS/PdmVAnalysisSummaryTable
    'EOY': {
        '2016' : 36.33,
        '2017' : 41.53,
        '2018' : 59.74,
    },
    # https://twiki.cern.ch/twiki/bin/viewauth/CMS/PdmVRun2LegacyAnalysis
    'UL': {
        '2016_PreVFP' : 19.52,
        '2016_PostVFP' : 16.81,
        '2017' : 41.48,
        '2018' : 59.83,
    }
}

# Cross-sections in pb
xsecs = {
    #"QCD_Pt-15to20_MuEnrichedPt5"    : 2799000.0,
    #"QCD_Pt-20to30_MuEnrichedPt5"    : 2526000.0,
    #"QCD_Pt-30to50_MuEnrichedPt5"    : 1362000.0,
    #"QCD_Pt-50to80_MuEnrichedPt5"    : 376600.0,
    #"QCD_Pt-80to120_MuEnrichedPt5"   : 88930.0,
    "QCD_Pt-120to170_MuEnrichedPt5"  : 21230.0,
    "QCD_Pt-170to300_MuEnrichedPt5"  : 7055.0,
    "QCD_Pt-300to470_MuEnrichedPt5"  : 619.3,
    "QCD_Pt-470to600_MuEnrichedPt5"  : 59.24,
    "QCD_Pt-600to800_MuEnrichedPt5"  : 18.21,
    "QCD_Pt-800to1000_MuEnrichedPt5" : 3.275,
    "QCD_Pt-1000toInf_MuEnrichedPt5" : 1.078,

    "QCD_Pt-120to170_MuEnrichedPt5_TuneCP5_13TeV_pythia8"  : 21230.0,
    "QCD_Pt-170to300_MuEnrichedPt5_TuneCP5_13TeV_pythia8"  : 7055.0,
    "QCD_Pt-300to470_MuEnrichedPt5_TuneCP5_13TeV_pythia8"  : 619.3,
    "QCD_Pt-470to600_MuEnrichedPt5_TuneCP5_13TeV_pythia8"  : 59.24,
    "QCD_Pt-600to800_MuEnrichedPt5_TuneCP5_13TeV_pythia8"  : 18.21,
    "QCD_Pt-800to1000_MuEnrichedPt5_TuneCP5_13TeV_pythia8" : 3.275,
    "QCD_Pt-1000toInf_MuEnrichedPt5_TuneCP5_13TeV_pythia8" : 1.078,

    "QCD_Pt-120To170_MuEnrichedPt5_TuneCP5_13TeV-pythia8"  : 21230.0,
    "QCD_Pt-170To300_MuEnrichedPt5_TuneCP5_13TeV-pythia8"  : 7055.0,
    "QCD_Pt-300To470_MuEnrichedPt5_TuneCP5_13TeV-pythia8"  : 619.3,
    "QCD_Pt-470To600_MuEnrichedPt5_TuneCP5_13TeV-pythia8"  : 59.24,
    "QCD_Pt-600To800_MuEnrichedPt5_TuneCP5_13TeV-pythia8"  : 18.21,
    "QCD_Pt-800To1000_MuEnrichedPt5_TuneCP5_13TeV-pythia8" : 3.275,
    "QCD_Pt-1000_MuEnrichedPt5_TuneCP5_13TeV-pythia8" : 1.078,

    "QCD_Pt-120to170_MuEnrichedPt5_TuneCUETP8M1_13TeV_pythia8" : 25700.0,
    "QCD_Pt-170to300_MuEnrichedPt5_TuneCUETP8M1_13TeV_pythia8" : 8683.0,
    "QCD_Pt-300to470_MuEnrichedPt5_TuneCUETP8M1_13TeV_pythia8" : 800.9,
    "QCD_Pt-470to600_MuEnrichedPt5_TuneCUETP8M1_13TeV_pythia8" : 79.25,
    "QCD_Pt-600to800_MuEnrichedPt5_TuneCUETP8M1_13TeV_pythia8" : 25.25,
    "QCD_Pt-800to1000_MuEnrichedPt5_TuneCUETP8M1_13TeV_pythia8" : 4.723,
    "QCD_Pt-1000toInf_MuEnrichedPt5_TuneCUETP8M1_13TeV_pythia8" : 1.613,

    "GluGluHToBB_M-125_13TeV"  : 27.8,
    "GluGluHToCC_M-125_13TeV"  : 27.8,
    "GluGluHToBB_M-125_13TeV_powheg_MINLO_NNLOPS_pythia8"  : 27.8,
    "GluGluHToCC_M-125_13TeV_powheg_MINLO_NNLOPS_pythia8"  : 27.8,
}

triggers = {
    'EOY': {
        '2016' : ['HLT_BTagMu_AK4Jet300_Mu5', 'HLT_BTagMu_AK8Jet300_Mu5'],
        '2017' : ['HLT_BTagMu_AK4Jet300_Mu5', 'HLT_BTagMu_AK8Jet300_Mu5'],
        '2018' : ['HLT_BTagMu_AK4Jet300_Mu5', 'HLT_BTagMu_AK8Jet300_Mu5'],
    }
}

JECversions = {
    'EOY': {
        '2016' : {
            'MC' : 'Summer16_07Aug2017_V11_MC',
            'Data' : {
                'B' : 'Summer16_07Aug2017BCD_V11_DATA',
                'C' : 'Summer16_07Aug2017BCD_V11_DATA',
                'D' : 'Summer16_07Aug2017BCD_V11_DATA',
                'E' : 'Summer16_07Aug2017EF_V11_DATA',
                'F' : 'Summer16_07Aug2017EF_V11_DATA',
                'G' : 'Summer16_07Aug2017GH_V11_DATA',
                'H' : 'Summer16_07Aug2017GH_V11_DATA'
                }
            },
        '2017' : {
            'MC' : 'Fall17_17Nov2017_V32_MC',
            'Data' : {
                'B' : 'Fall17_17Nov2017B_V32_DATA',
                'C' : 'Fall17_17Nov2017C_V32_DATA',
                'D' : 'Fall17_17Nov2017DE_V32_DATA',
                'E' : 'Fall17_17Nov2017DE_V32_DATA',
                'F' : 'Fall17_17Nov2017F_V32_DATA'
                }
            },
        '2018' : {
            'MC' : 'Autumn18_V19_MC',
            'Data' : {
                'A' : 'Autumn18_RunA_V19_DATA',
                'B' : 'Autumn18_RunB_V19_DATA',
                'C' : 'Autumn18_RunC_V19_DATA',
                'D' : 'Autumn18_RunD_V19_DATA'
                }
            }
        },
    'UL': {
        '2016_PreVFP' : {
            'MC' : 'Summer19UL16APV_V7_MC',
            'Data' : {
                'B' : 'Summer19UL16APV_RunBCD_V7_DATA',
                'C' : 'Summer19UL16APV_RunBCD_V7_DATA',
                'D' : 'Summer19UL16APV_RunBCD_V7_DATA',
                'E' : 'Summer19UL16APV_RunEF_V7_DATA',
                'F' : 'Summer19UL16APV_RunEF_V7_DATA'
                }
            },
        '2016_PostVFP' : {
            'MC' : 'Summer19UL16_V7_MC',
            'Data' : {
                'F' : 'Summer19UL16_RunFGH_V7_DATA',
                'G' : 'Summer19UL16_RunFGH_V7_DATA',
                'H' : 'Summer19UL16_RunFGH_V7_DATA'
                }
            },
        '2017' : {
            'MC' : 'Summer19UL17_V5_MC',
            'Data' : {
                'B' : 'Summer19UL17_RunB_V5_DATA',
                'C' : 'Summer19UL17_RunC_V5_DATA',
                'D' : 'Summer19UL17_RunD_V5_DATA',
                'E' : 'Summer19UL17_RunE_V5_DATA',
                'F' : 'Summer19UL17_RunF_V5_DATA'
                }
            },
        '2018' : {
            'MC' : 'Summer19UL18_V5_MC',
            'Data' : {
                'A' : 'Summer19UL18_RunA_V5_DATA',
                'B' : 'Summer19UL18_RunB_V5_DATA',
                'C' : 'Summer19UL18_RunC_V5_DATA',
                'D' : 'Summer19UL18_RunD_V5_DATA',
                }
            }
        },
}

jecTarFiles = {
    'EOY' : {
        '2016' :
            [
            '/correction_files/JEC/Summer16_07Aug2017BCD_V11_DATA.tar.gz',
            '/correction_files/JEC/Summer16_07Aug2017EF_V11_DATA.tar.gz',
            '/correction_files/JEC/Summer16_07Aug2017GH_V11_DATA.tar.gz',
            '/correction_files/JEC/Summer16_07Aug2017_V11_MC.tar.gz',
            ],
        '2017' :
            [
            '/correction_files/JEC/Fall17_17Nov2017B_V32_DATA.tar.gz',
            '/correction_files/JEC/Fall17_17Nov2017C_V32_DATA.tar.gz',
            '/correction_files/JEC/Fall17_17Nov2017DE_V32_DATA.tar.gz',
            '/correction_files/JEC/Fall17_17Nov2017F_V32_DATA.tar.gz',
            '/correction_files/JEC/Fall17_17Nov2017_V32_MC.tar.gz',
            ],
        '2018' :
            [
            '/correction_files/JEC/Autumn18_RunA_V19_DATA.tar.gz',
            '/correction_files/JEC/Autumn18_RunB_V19_DATA.tar.gz',
            '/correction_files/JEC/Autumn18_RunC_V19_DATA.tar.gz',
            '/correction_files/JEC/Autumn18_RunD_V19_DATA.tar.gz',
            '/correction_files/JEC/Autumn18_V19_MC.tar.gz',
            ]
    },
    'UL' : {
        '2016_PreVFP' : [
            '/correction_files/JEC/Summer19UL16APV_V7_MC.tar.gz',
            '/correction_files/JEC/Summer19UL16APV_RunBCD_V7_DATA.tar.gz',
            '/correction_files/JEC/Summer19UL16APV_RunEF_V7_DATA.tar.gz',
            ],
        '2016_PostVFP' : [
            '/correction_files/JEC/Summer19UL16_V7_MC.tar.gz',
            '/correction_files/JEC/Summer19UL16_RunFGH_V7_DATA.tar.gz',
            ],
        '2017' :
            [
            '/correction_files/JEC/Summer19UL17_RunB_V5_DATA.tar.gz',
            '/correction_files/JEC/Summer19UL17_RunC_V5_DATA.tar.gz',
            '/correction_files/JEC/Summer19UL17_RunD_V5_DATA.tar.gz',
            '/correction_files/JEC/Summer19UL17_RunE_V5_DATA.tar.gz',
            '/correction_files/JEC/Summer19UL17_RunF_V5_DATA.tar.gz',
            ],
        '2018' :
            [
            '/correction_files/JEC/Summer19UL18_RunA_V5_DATA.tar.gz',
            '/correction_files/JEC/Summer19UL18_RunB_V5_DATA.tar.gz',
            '/correction_files/JEC/Summer19UL18_RunC_V5_DATA.tar.gz',
            '/correction_files/JEC/Summer19UL18_RunD_V5_DATA.tar.gz',
            '/correction_files/JEC/Summer19UL18_V5_MC.tar.gz',
            ]
    }
}

FinalMask = ['msd100tau06']

AK8Taggers = ['btagDDBvLV2', 'btagDDCvLV2', 'particleNetMD_Xbb_QCD', 'particleNetMD_Xcc_QCD', 'deepTagMD_ZHbbvsQCD', 'deepTagMD_ZHccvsQCD', 'btagHbb']
#AK8Taggers = ['btagDDBvLV2', 'btagDDCvLV2', 'btagDDCvBV2', 'btagHbb', 'particleNetMD_Xbb', 'particleNetMD_Xcc', 'deepTag_H', 'deepTagMD_ccvsLight']

#logsv1mass_bins = np.concatenate( ( (-4, -3.6, -3.2, -2.8, -2.4, -2, -1.59, -1.2, -0.8, -0.4), np.arange(0., 1.8, 0.1), (1.8, 2.5, 3.2) ) )
AK8Tagger_parameters = {'binning' : {'n_or_arr' : 20,  'lo' : 0,      'hi' : 1},     'xlim' : {'xmin' : 0, 'xmax' : 1}}

PtBinning = {
    'UL': {
        '2016_PreVFP' : {
            'L' : (450, 500),
            'M' : (500, 600),
            'H' : (600, 'Inf'),
        },
        '2016_PostVFP' : {
            'L' : (450, 500),
            'M' : (500, 600),
            'H' : (600, 'Inf'),
        },
        '2017' : {
            'L' : (450, 500),
            'M' : (500, 600),
            'H' : (600, 'Inf'),
        },
        '2018' : {
            'L' : (450, 500),
            'M' : (500, 600),
            'H' : (600, 'Inf'),
        }
    },
    'EOY': {
        '2016' : {
            'M' : (350, 450),
            'H' : (450, 'Inf'),
        },
        '2017' : {
            'M' : (350, 450),
            'H' : (450, 'Inf'),
        },
        '2018' : {
            'M' : (350, 450),
            'H' : (450, 'Inf'),
        }
    },

}


"""
# Old WPs (before 31.10.2022)

AK8TaggerWP = {
    'EOY' : {
        '2016' : {
            'btagDDBvLV2' : {
                'H' : (0.64, 1)
            },
            'btagDDCvLV2' : {
                'H' : (0.45, 1)
            },
        },
        '2017' : {
            'btagDDBvLV2' : {
                'H' : (0.64, 1)
            },
            'btagDDCvLV2' : {
                'H' : (0.45, 1)
            },
        },
        '2018' : {
            'btagDDBvLV2' : {
                'H' : (0.64, 1)
            },
            'btagDDCvLV2' : {
                'H' : (0.45, 1)
            },
        }
    },
    # 2016 UL WPs are copied from 2017 since they have not been computed yet
    'UL' : {
        '2016' : {
            'btagDDBvLV2' : {
                'L' : (0.2310, 0.4111),
                'M' : (0.4111, 0.6479),
                'H' : (0.6479, 1),
            },
            'btagDDCvLV2' : {
                'L' : (0.0435, 0.1287),
                'M' : (0.1287, 0.3276),
                'H' : (0.3276, 1),
            },
            'particleNetMD_Xbb_QCD' : {
                'L' : (0.9789, 0.9904),
                'M' : (0.9904, 0.9964),
                'H' : (0.9964, 1),
            },
            'particleNetMD_Xcc_QCD' : {
                'L' : (0.9282, 0.9737),
                'M' : (0.9737, 0.9916),
                'H' : (0.9916, 1),
            },
        },
        '2017' : {
            'btagDDBvLV2' : {
                'L' : (0.2310, 0.4111),
                'M' : (0.4111, 0.6479),
                'H' : (0.6479, 1),
            },
            'btagDDCvLV2' : {
                'L' : (0.0435, 0.1287),
                'M' : (0.1287, 0.3276),
                'H' : (0.3276, 1),
            },
            'particleNetMD_Xbb_QCD' : {
                'L' : (0.9789, 0.9904),
                'M' : (0.9904, 0.9964),
                'H' : (0.9964, 1),
            },
            'particleNetMD_Xcc_QCD' : {
                'L' : (0.9282, 0.9737),
                'M' : (0.9737, 0.9916),
                'H' : (0.9916, 1),
            },
        },
        '2018' : {
            'btagDDBvLV2' : {
                'L' : (0.2301, 0.4139),
                'M' : (0.4139, 0.6509),
                'H' : (0.6509, 1),
            },
            'btagDDCvLV2' : {
                'L' : (0.0433, 0.1309),
                'M' : (0.1309, 0.3323),
                'H' : (0.3323, 1),
            },
            'particleNetMD_Xbb_QCD' : {
                'L' : (0.9806, 0.9913),
                'M' : (0.9913, 0.9967),
                'H' : (0.9967, 1),
            },
            'particleNetMD_Xcc_QCD' : {
                'L' : (0.9333, 0.9757),
                'M' : (0.9757, 0.9925),
                'H' : (0.9925, 1),
            },
        },
    }
}
"""

# New WPs (31.10.2022)

AK8TaggerWP = {
    'EOY' : {
        '2016' : {
            'btagDDBvLV2' : {
                'H' : (0.64, 1)
            },
            'btagDDCvLV2' : {
                'H' : (0.45, 1)
            },
        },
        '2017' : {
            'btagDDBvLV2' : {
                'H' : (0.64, 1)
            },
            'btagDDCvLV2' : {
                'H' : (0.45, 1)
            },
        },
        '2018' : {
            'btagDDBvLV2' : {
                'H' : (0.64, 1)
            },
            'btagDDCvLV2' : {
                'H' : (0.45, 1)
            },
        }
    },
    # 2016 UL WPs are copied from 2017 since they have not been computed yet
    'UL' : {
        '2016_PreVFP' : {
            'btagDDBvLV2' : {
                'L' : (0.0256, 0.1180),
                'M' : (0.1180, 0.2739),
                'H' : (0.2739, 1),
            },
            'btagDDCvLV2' : {
                'L' : (0.0327, 0.1043),
                'M' : (0.1043, 0.2568),
                'H' : (0.2568, 1),
            },
            'particleNetMD_Xbb_QCD' : {
                'L' : (0.9088, 0.9737),
                'M' : (0.9737, 0.9883),
                'H' : (0.9883, 1),
            },
            'particleNetMD_Xcc_QCD' : {
                'L' : (0.9252, 0.9751),
                'M' : (0.9751, 0.9909),
                'H' : (0.9909, 1),
            },
            'deepTagMD_ZHbbvsQCD' : {
                'L' : (0.7021, 0.8940),
                'M' : (0.8940, 0.9521),
                'H' : (0.9521, 1),
            },
            'deepTagMD_ZHccvsQCD' : {
                'L' : (0.6733, 0.8496),
                'M' : (0.8496, 0.9326),
                'H' : (0.9326, 1),
            },
            'btagHbb' : {
                'L' : (0.3104, 0.7559),
                'M' : (0.7559, 0.8926),
                'H' : (0.8926, 1),
            },
        },
        '2016_PostVFP' : {
            'btagDDBvLV2' : {
                'L' : (0.0270, 0.1213),
                'M' : (0.1213, 0.2786),
                'H' : (0.2786, 1),
            },
            'btagDDCvLV2' : {
                'L' : (0.0321, 0.1021),
                'M' : (0.1021, 0.2505),
                'H' : (0.2505, 1),
            },
            'particleNetMD_Xbb_QCD' : {
                'L' : (0.9137, 0.9735),
                'M' : (0.9735, 0.9883),
                'H' : (0.9883, 1),
            },
            'particleNetMD_Xcc_QCD' : {
                'L' : (0.9252, 0.9743),
                'M' : (0.9743, 0.9905),
                'H' : (0.9905, 1),
            },
            'deepTagMD_ZHbbvsQCD' : {
                'L' : (0.6890, 0.8877),
                'M' : (0.8877, 0.9507),
                'H' : (0.9507, 1),
            },
            'deepTagMD_ZHccvsQCD' : {
                'L' : (0.6458, 0.8369),
                'M' : (0.8369, 0.9277),
                'H' : (0.9277, 1),
            },
            'btagHbb' : {
                'L' : (0.3279, 0.7646),
                'M' : (0.7646, 0.8955),
                'H' : (0.8955, 1),
            },
        },
        '2017' : {
            'btagDDBvLV2' : {
                'L' : (0.0404, 0.1566),
                'M' : (0.1566, 0.3154),
                'H' : (0.3154, 1),
            },
            'btagDDCvLV2' : {
                'L' : (0.0457, 0.1361),
                'M' : (0.1361, 0.3013),
                'H' : (0.3013, 1),
            },
            'particleNetMD_Xbb_QCD' : {
                'L' : (0.9105, 0.9714),
                'M' : (0.9714, 0.9870),
                'H' : (0.9870, 1),
            },
            'particleNetMD_Xcc_QCD' : {
                'L' : (0.9347, 0.9765),
                'M' : (0.9765, 0.9909),
                'H' : (0.9909, 1),
            },
            'deepTagMD_ZHbbvsQCD' : {
                'L' : (0.7422, 0.9033),
                'M' : (0.9033, 0.9546),
                'H' : (0.9546, 1),
            },
            'deepTagMD_ZHccvsQCD' : {
                'L' : (0.7100, 0.8677),
                'M' : (0.8677, 0.9390),
                'H' : (0.9390, 1),
            },
            'btagHbb' : {
                'L' : (0.5068, 0.8237),
                'M' : (0.8237, 0.9185),
                'H' : (0.9185, 1),
            },
        },
        '2018' : {
            'btagDDBvLV2' : {
                'L' : (0.0399, 0.1566),
                'M' : (0.1566, 0.3140),
                'H' : (0.3140, 1),
            },
            'btagDDCvLV2' : {
                'L' : (0.0425, 0.1322),
                'M' : (0.1322, 0.3013),
                'H' : (0.3013, 1),
            },
            'particleNetMD_Xbb_QCD' : {
                'L' : (0.9172, 0.9734),
                'M' : (0.9734, 0.9880),
                'H' : (0.9880, 1),
            },
            'particleNetMD_Xcc_QCD' : {
                'L' : (0.9368, 0.9777),
                'M' : (0.9777, 0.9917),
                'H' : (0.9917, 1),
            },
            'deepTagMD_ZHbbvsQCD' : {
                'L' : (0.7432, 0.9058),
                'M' : (0.9058, 0.9551),
                'H' : (0.9551, 1),
            },
            'deepTagMD_ZHccvsQCD' : {
                'L' : (0.7017, 0.8662),
                'M' : (0.8662, 0.9390),
                'H' : (0.9390, 1),
            },
            'btagHbb' : {
                'L' : (0.4939, 0.8193),
                'M' : (0.8193, 0.9175),
                'H' : (0.9175, 1),
            },
        },
    }
}

histogram_settings = {
    'EOY' : {
        'variables' : {
            'fatjet_pt'    : {'binning' : {'n_or_arr' : 40,  'lo' : 0,      'hi' : 2000},  'xlim' : {'xmin' : 0,      'xmax' : 2000}},
            'fatjet_eta'   : {'binning' : {'n_or_arr' : 30,  'lo' : -3,     'hi' : 3},     'xlim' : {'xmin' : -2.4,   'xmax' : 2.4}},
            'fatjet_phi'   : {'binning' : {'n_or_arr' : 30,  'lo' : -np.pi, 'hi' : np.pi}, 'xlim' : {'xmin' : -np.pi, 'xmax' : np.pi}},
            #'fatjet_phi'   : {'binning' : {'n_or_arr' : 30,  'lo' : -3,     'hi' : 3},     'xlim' : {'xmin' : -3, 'xmax' : 3}},
            'fatjet_mass'  : {'binning' : {'n_or_arr' : 20,  'lo' : 0,      'hi' : 400},   'xlim' : {'xmin' : 0, 'xmax' : 400}},
            'fatjet_btagDDBvLV2'  : {'binning' : {'n_or_arr' : 20,  'lo' : 0,      'hi' : 1},     'xlim' : {'xmin' : 0, 'xmax' : 1}},
            'fatjet_btagDDCvLV2'  : {'binning' : {'n_or_arr' : 20,  'lo' : 0,      'hi' : 1},     'xlim' : {'xmin' : 0, 'xmax' : 1}},
            'fatjet_btagDDCvBV2'  : {'binning' : {'n_or_arr' : 20,  'lo' : 0,      'hi' : 1},     'xlim' : {'xmin' : 0, 'xmax' : 1}},
            'fatjet_particleNetMD': {'binning' : {'n_or_arr' : 20,  'lo' : 0,      'hi' : 1},     'xlim' : {'xmin' : 0, 'xmax' : 1}},
            'fatjet_deepTagMD'    : {'binning' : {'n_or_arr' : 20,  'lo' : 0,      'hi' : 1},     'xlim' : {'xmin' : 0, 'xmax' : 1}},
            'fatjet_nsv1'         : {'binning' : {'n_or_arr' : 30,  'lo' : 0,      'hi' : 30},    'xlim' : {'xmin' : 0, 'xmax' : 10}},
            'fatjet_nsv2'         : {'binning' : {'n_or_arr' : 30,  'lo' : 0,      'hi' : 30},    'xlim' : {'xmin' : 0, 'xmax' : 10}},
            'fatjet_nmusj1'       : {'binning' : {'n_or_arr' : 30,  'lo' : 0,      'hi' : 30},    'xlim' : {'xmin' : 0, 'xmax' : 10}},
            'fatjet_nmusj2'       : {'binning' : {'n_or_arr' : 30,  'lo' : 0,      'hi' : 30},    'xlim' : {'xmin' : 0, 'xmax' : 10}},
            'fatjet_jetproba'     : {'binning' : {'n_or_arr' : 25,  'lo' : 0,      'hi' : 2.5},   'xlim' : {'xmin' : 0, 'xmax' : 2.5}},
            'sv_sv1mass'              : {'binning' : {'n_or_arr' : 50,  'lo' : 0,      'hi' : 50},    'xlim' : {'xmin' : 0, 'xmax' : 50}},
            'sv_sv1mass_maxdxySig'    : {'binning' : {'n_or_arr' : 50,  'lo' : 0,      'hi' : 50},    'xlim' : {'xmin' : 0, 'xmax' : 50}},
            'sv_logsv1mass'           : {'binning' : {'n_or_arr' : 80,  'lo' : -4,     'hi' : 4},     'xlim' : {'xmin' : -1.2, 'xmax' : 3.2}},
            'sv_logsv1mass_maxdxySig' : {'binning' : {'n_or_arr' : 80,  'lo' : -4,     'hi' : 4},     'xlim' : {'xmin' : -4, 'xmax' : 4}},
            'sv_logsv1massratio'      : {'binning' : {'n_or_arr' : 200,  'lo' : -100,     'hi' : 100},     'xlim' : {'xmin' : -100, 'xmax' : 100}},
            'sv_logsv1massres'        : {'binning' : {'n_or_arr' : 100,  'lo' : -1,     'hi' : 1},     'xlim' : {'xmin' : -1, 'xmax' : 1}},
            'sv_pt'                   : {'binning' : {'n_or_arr' : 200,  'lo' : 0,      'hi' : 1000},    'xlim' : {'xmin' : 0, 'xmax' : 300}},
            'sv_eta'                  : {'binning' : {'n_or_arr' : 30,  'lo' : -3,      'hi' : 3},    'xlim' : {'xmin' : -3, 'xmax' : 3}},
            'sv_mass'                 : {'binning' : {'n_or_arr' : 50,  'lo' : 0,      'hi' : 50},    'xlim' : {'xmin' : 0, 'xmax' : 50}},
            
            #'sv_logsv1mass'       : {'binning' : {'n_or_arr' : np.concatenate((np.arange(-4,1.9,0.1), [2.5,3.2]))},     'xlim' : {'xmin' : -0.8, 'xmax' : 3.2}},
            'nd_projmass'           : {'binning' : {'n_or_arr' :100,  'lo' : 0,     'hi' : 1000},     'xlim' : {'xmin' : 0, 'xmax' : 1000}},
            'nd_logprojmass'        : {'binning' : {'n_or_arr' :100,  'lo' : -2.5,     'hi' : 6},     'xlim' : {'xmin' : -2.5, 'xmax' : 6}},
        },
        'postfit' : {
            'sv_logsv1mass'       : {'binning' : {'n_or_arr' : np.arange(-4,2.1,0.1)},     'xlim' : {'xmin' : -1.2, 'xmax' : 2.0}},
        },
        'crop' : {
            'sfpass' : {'length' : 1780, 'height' : 2100},
            'sffail' : {'length' : 1725, 'height' : 2100},
        }
    },
    'UL' : {
        'variables' : {
            'fatjet_pt'    : {'binning' : {'n_or_arr' : 40,  'lo' : 0,      'hi' : 2000},  'xlim' : {'xmin' : 0,      'xmax' : 2000}},
            'fatjet_eta'   : {'binning' : {'n_or_arr' : 30,  'lo' : -3,     'hi' : 3},     'xlim' : {'xmin' : -2.4,   'xmax' : 2.4}},
            'fatjet_phi'   : {'binning' : {'n_or_arr' : 30,  'lo' : -np.pi, 'hi' : np.pi}, 'xlim' : {'xmin' : -np.pi, 'xmax' : np.pi}},
            #'fatjet_phi'   : {'binning' : {'n_or_arr' : 30,  'lo' : -3,     'hi' : 3},     'xlim' : {'xmin' : -3, 'xmax' : 3}},
            'fatjet_mass'  : {'binning' : {'n_or_arr' : 20,  'lo' : 0,      'hi' : 400},   'xlim' : {'xmin' : 0, 'xmax' : 400}},
            'fatjet_btagDDBvLV2'  : {'binning' : {'n_or_arr' : 20,  'lo' : 0,      'hi' : 1},     'xlim' : {'xmin' : 0, 'xmax' : 1}},
            'fatjet_btagDDCvLV2'  : {'binning' : {'n_or_arr' : 20,  'lo' : 0,      'hi' : 1},     'xlim' : {'xmin' : 0, 'xmax' : 1}},
            'fatjet_btagDDCvBV2'  : {'binning' : {'n_or_arr' : 20,  'lo' : 0,      'hi' : 1},     'xlim' : {'xmin' : 0, 'xmax' : 1}},
            'fatjet_particleNetMD': {'binning' : {'n_or_arr' : 20,  'lo' : 0,      'hi' : 1},     'xlim' : {'xmin' : 0, 'xmax' : 1}},
            'fatjet_deepTagMD'    : {'binning' : {'n_or_arr' : 20,  'lo' : 0,      'hi' : 1},     'xlim' : {'xmin' : 0, 'xmax' : 1}},
            'fatjet_nsv1'         : {'binning' : {'n_or_arr' : 30,  'lo' : 0,      'hi' : 30},    'xlim' : {'xmin' : 0, 'xmax' : 10}},
            'fatjet_nsv2'         : {'binning' : {'n_or_arr' : 30,  'lo' : 0,      'hi' : 30},    'xlim' : {'xmin' : 0, 'xmax' : 10}},
            'fatjet_nmusj1'       : {'binning' : {'n_or_arr' : 30,  'lo' : 0,      'hi' : 30},    'xlim' : {'xmin' : 0, 'xmax' : 10}},
            'fatjet_nmusj2'       : {'binning' : {'n_or_arr' : 30,  'lo' : 0,      'hi' : 30},    'xlim' : {'xmin' : 0, 'xmax' : 10}},
            'fatjet_jetproba'     : {'binning' : {'n_or_arr' : 25,  'lo' : 0,      'hi' : 2.5},   'xlim' : {'xmin' : 0, 'xmax' : 2.5}},
            'sv_sv1mass'              : {'binning' : {'n_or_arr' : 50,  'lo' : 0,      'hi' : 50},    'xlim' : {'xmin' : 0, 'xmax' : 50}},
            'sv_sv1mass_maxdxySig'    : {'binning' : {'n_or_arr' : 50,  'lo' : 0,      'hi' : 50},    'xlim' : {'xmin' : 0, 'xmax' : 50}},
            'sv_logsv1mass'           : {'binning' : {'n_or_arr' : 40,  'lo' : -4,     'hi' : 4},     'xlim' : {'xmin' : -1.2, 'xmax' : 3.2}},
            'sv_logsv1mass_maxdxySig' : {'binning' : {'n_or_arr' : 40,  'lo' : -4,     'hi' : 4},     'xlim' : {'xmin' : -1.2, 'xmax' : 3.2}},
            'sv_logsv1massratio'      : {'binning' : {'n_or_arr' : 200,  'lo' : -100,     'hi' : 100},     'xlim' : {'xmin' : -100, 'xmax' : 100}},
            'sv_logsv1massres'        : {'binning' : {'n_or_arr' : 100,  'lo' : -1,     'hi' : 1},     'xlim' : {'xmin' : -1, 'xmax' : 1}},
        },
        'postfit' : {
            'sv_logsv1mass'       : {'binning' : {'n_or_arr' : np.arange(-4,2.2,0.2)},     'xlim' : {'xmin' : -1.2, 'xmax' : 2.0}},
        },
        'crop' : {
            'sfpass' : {'length' : 1780, 'height' : 2100},
            'sffail' : {'length' : 1725, 'height' : 2100},
        }
    }
}

for campaign in ['EOY', 'UL']:
    for tagger in AK8Taggers:
        histogram_settings[campaign]['variables']['fatjet_{}'.format(tagger)] = AK8Tagger_parameters

#sample_names = ['bb', 'cc', 'b', 'c', 'l']
sample_baseline_names = ['c_cc', 'b_bb', 'l']
sample_merged_names = ['bb_cc', 'l']
flavors_order = {
    'btagDDBvLV2' : ['l', 'c', 'cc', 'b', 'bb'],
    'btagDDCvLV2' : ['l', 'b', 'bb', 'c', 'cc'],
    #'particleNetMD_Xbb' : ['l', 'c_cc', 'b_bb'],
    #'particleNetMD_Xcc' : ['l', 'b_bb', 'c_cc'],
    }
flavors_color = {'l' : 'blue', 'b' : 'red', 'c' : 'green', 'bb' : 'cyan', 'cc' : 'magenta'}
flavor_opts = {
    'facecolor': [flavors_color[f] for f in flavors_color.keys()],
    'edgecolor': 'black',
    'alpha': 1.0
}

fit_extra_args = {
    'fixbkg_mergeMH' : {
        '2017': {
            'btagDDBvLV2': {
                'L' : {},
                'M' : {},
                'H' : {},
            },
            'btagDDCvLV2': {
                'L' : {
                    'M': '--X-rtd FITTER_NEW_CROSSING_ALGO --X-rtd FITTER_NEVER_GIVE_UP --X-rtd FITTER_BOUND --cminDefaultMinimizerAlgo Simplex',
                },
                'M' : {},
                'H' : {},
            },
            'particleNetMD_Xbb': {
                'L' : {},
                'M' : {},
                'H' : {},
            },
            'particleNetMD_Xcc': {
                'L' : {},
                'M' : {},
                'H' : {},
            },
        },
    }
}

fit_parameters = {
    'EOYpt500': {
        '2016': {
            'btagDDBvLV2': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : -20, 'hi' : 20}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
            'btagDDCvLV2': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 20}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}}
        },
        '2017': {
            'btagDDBvLV2': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : -20, 'hi' : 20}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
            'btagDDCvLV2': {'c_cc': {'value' : 1, 'lo' : -20, 'hi' : 20}, 'b_bb': {'value' : 1, 'lo' : -20, 'hi' : 20}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}}
        },
        '2018': {
            'btagDDBvLV2': {'c_cc': {'value' : 1, 'lo' : -20, 'hi' : 20}, 'b_bb': {'value' : 1, 'lo' : -20, 'hi' : 20}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
            'btagDDCvLV2': {'c_cc': {'value' : 1, 'lo' : -20, 'hi' : 20}, 'b_bb': {'value' : 1, 'lo' : -20, 'hi' : 20}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}}
        }
    },
    'EOYwp064pt450': {
        '2016': {
            'btagDDBvLV2': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0.5, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0.5, 'hi' : 2}},
            'btagDDCvLV2': {'c_cc': {'value' : 1, 'lo' : 0.5, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0.5, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0.5, 'hi' : 2}}
        },
        '2017': {
            'btagDDBvLV2': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0.5, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0.5, 'hi' : 2}},
            'btagDDCvLV2': {'c_cc': {'value' : 1, 'lo' : 0.5, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0.5, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0.5, 'hi' : 2}}
        },
        '2018': {
            'btagDDBvLV2': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0.5, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0.5, 'hi' : 2}},
            'btagDDCvLV2': {'c_cc': {'value' : 1, 'lo' : 0.5, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0.5, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0.5, 'hi' : 2}}
        }
    },
    'EOYpt450': {
        '2016': {
            'btagDDBvLV2': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 10}, 'b_bb': {'value' : 1, 'lo' : -20, 'hi' : 20}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
            'btagDDCvLV2': {'c_cc': {'value' : 1, 'lo' : -20, 'hi' : 20}, 'b_bb': {'value' : 1, 'lo' : -20, 'hi' : 20}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}}
        },
        '2017': {
            'btagDDBvLV2': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 10}, 'b_bb': {'value' : 1, 'lo' : -20, 'hi' : 20}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
            'btagDDCvLV2': {'c_cc': {'value' : 1, 'lo' : -50, 'hi' : 50}, 'b_bb': {'value' : 1, 'lo' : -20, 'hi' : 20}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}}
        },
        '2018': {
            'btagDDBvLV2': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 10}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 20}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
            'btagDDCvLV2': {'c_cc': {'value' : 1, 'lo' : -20, 'hi' : 20}, 'b_bb': {'value' : 1, 'lo' : -20, 'hi' : 20}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}}
        }
    },
    'EOYpt450shapeunc': {
        '2016': {
            'btagDDBvLV2': {'c_cc': {'value' : 1, 'lo' : -20, 'hi' : 20}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 20}, 'l': {'value' : 1, 'lo' : -2, 'hi' : 2}},
            'btagDDCvLV2': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 20}, 'b_bb': {'value' : 1, 'lo' : -10, 'hi' : 10}, 'l': {'value' : 1, 'lo' : -10, 'hi' : 10}}
        },
        '2017': {
            'btagDDBvLV2': {'c_cc': {'value' : 1, 'lo' : -20, 'hi' : 20}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 20}, 'l': {'value' : 1, 'lo' : -2, 'hi' : 2}},
            'btagDDCvLV2': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 20}, 'b_bb': {'value' : 1, 'lo' : -10, 'hi' : 10}, 'l': {'value' : 1, 'lo' : -10, 'hi' : 10}}
        },
        '2018': {
            'btagDDBvLV2': {'c_cc': {'value' : 1, 'lo' : -20, 'hi' : 20}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 20}, 'l': {'value' : 1, 'lo' : -2, 'hi' : 2}},
            'btagDDCvLV2': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : -10, 'hi' : 10}, 'l': {'value' : 1, 'lo' : -10, 'hi' : 10}}
        }
    },
    'UL': {
        #'2016': {
        #    'btagDDBvLV2': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : -20, 'hi' : 20}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
        #    'btagDDCvLV2': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 20}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}}
        #},
        '2017': {
            'btagDDBvLV2': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
            'btagDDCvLV2': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}}
        },
        '2018': {
            'btagDDBvLV2': {
                'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'M':         {'c_cc': {'value' : 1, 'lo' : -10, 'hi' : 10}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
            },
            'btagDDCvLV2': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}}
        }
    },
    'ULrange-1.2To2.0': {
        #'2016': {
        #    'btagDDBvLV2': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : -20, 'hi' : 20}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
        #    'btagDDCvLV2': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 20}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}}
        #},
        '2017': {
            'btagDDBvLV2': {
                'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
            },
            'btagDDCvLV2': {
                'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
            }
        },
        '2018': {
            'btagDDBvLV2': {
                'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'M':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : -100, 'hi' : 100}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
            },
            'btagDDCvLV2': {
                'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 10}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
            }
        }
    },

    'baseline': {
        '2016': {
            'btagDDBvLV2': {
                'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
            },
            'btagDDCvLV2': {
                'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
            },
            'particleNetMD_Xbb': {
                'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
            },
            'particleNetMD_Xcc': {
                'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
            }
        },
        '2017': {
            'btagDDBvLV2': {
                'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
            },
            'btagDDCvLV2': {
                'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
            },
            'particleNetMD_Xbb': {
                'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
            },
            'particleNetMD_Xcc': {
                'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
            },
        },
        '2018': {
            'btagDDBvLV2': {
                'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
            },
            'btagDDCvLV2': {
                'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
            },
            'particleNetMD_Xbb': {
                'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
            },
            'particleNetMD_Xcc': {
                'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
            },
        }
    },

    'fixbkg': {
        '2016': {
            'btagDDBvLV2': {
                'Inclusive': {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                'L':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                'M':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                'H':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
            },
            'btagDDCvLV2': {
                'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
            }
        },
        '2017': {
            'btagDDBvLV2': {
                'Inclusive': {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                'L':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                'M':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                'H':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
            },
            'btagDDCvLV2': {
                'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
            },
            'particleNetMD_Xbb': {
                'Inclusive': {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                'L':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                'M':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                'H':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
            },
            'particleNetMD_Xcc': {
                'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
            },
        },
        '2018': {
            'btagDDBvLV2': {
                'L' : {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                },
                'M' : {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : -20, 'hi' : 20}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                },
                'H' : {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                },
            },
            'btagDDCvLV2': {
                'L' : {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                },
                'M' : {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                },
                'H' : {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                },
            },
            'particleNetMD_Xbb': {
                'L' : {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0.5, 'hi' : 10000}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0.5, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0.5, 'hi' : 20}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                },
                'M' : {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0.5, 'hi' : 20}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0.5, 'hi' : 100}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0.5, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0.5, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                },
                'H' : {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0.5, 'hi' : 20}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0.5, 'hi' : 20}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0.5, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0.5, 'hi' : 20}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                },
            },
            'particleNetMD_Xcc': {
                'L' : {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                },
                'M' : {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                },
                'H' : {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 0.2, 'hi' : 20}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 0.2, 'hi' : 20}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                },
            },
        }
    },

    'fixbkg_mergeMH': {
        '2016': {
            'btagDDBvLV2': {
                'L' : {
                    'Inclusive': {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M':         {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'H':         {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M+H':       {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
                'M' : {
                    'Inclusive': {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M':         {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'H':         {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M+H':       {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
                'H' : {
                    'Inclusive': {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'b_bb': {'value' : 1, 'lo' : -20, 'hi' : 20}},
                    'M':         {'b_bb': {'value' : 1, 'lo' : -20, 'hi' : 20}},
                    'H':         {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M+H':       {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
            },
            'btagDDCvLV2': {
                'L' : {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : -20, 'hi' : 20}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M+H':       {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
                'M' : {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M+H':       {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
                'H' : {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M+H':       {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
            },
            'particleNetMD_Xbb': {
                'L' : {
                    'Inclusive': {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M':         {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'H':         {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M+H':       {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
                'M' : {
                    'Inclusive': {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M':         {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'H':         {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M+H':       {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
                'H' : {
                    'Inclusive': {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M':         {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'H':         {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M+H':       {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
            },
            'particleNetMD_Xcc': {
                'L' : {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M+H':       {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
                'M' : {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M+H':       {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
                'H' : {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M+H':       {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
            },
        },
        '2017': {
            'btagDDBvLV2': {
                'L' : {
                    'Inclusive': {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}}, # No post-fit uncertainties
                    'L':         {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M':         {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'H':         {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}}, # No post-fit uncertainties
                    'M+H':       {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
                'M' : {
                    'Inclusive': {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M':         {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'H':         {'b_bb': {'value' : 1, 'lo' : -20, 'hi' : 20}},
                    'M+H':       {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
                'H' : {
                    'Inclusive': {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M':         {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}}, # No post-fit uncertainties
                    'H':         {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M+H':       {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
            },
            'btagDDCvLV2': {
                'L' : {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}}, # No post-fit uncertainties
                    'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : -20, 'hi' : 20}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}}, # No post-fit uncertainties
                    'M+H':       {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
                'M' : {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}}, # No post-fit uncertainties
                    'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}}, # No post-fit uncertainties
                    'M+H':       {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
                'H' : {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}}, # No post-fit uncertainties
                    'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}}, # No post-fit uncertainties
                    'M+H':       {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
            },
            'particleNetMD_Xbb': {
                'L' : {
                    'Inclusive': {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'b_bb': {'value' : 1, 'lo' : 0.5, 'hi' : 10000}},
                    'M':         {'b_bb': {'value' : 1, 'lo' : 0.5, 'hi' : 2}}, # No post-fit uncertainties
                    'H':         {'b_bb': {'value' : 1, 'lo' : 0.5, 'hi' : 20}},
                    'M+H':       {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
                'M' : {
                    'Inclusive': {'b_bb': {'value' : 1, 'lo' : 0.5, 'hi' : 2}}, # No post-fit uncertainties
                    'L':         {'b_bb': {'value' : 1, 'lo' : 0.5, 'hi' : 2}},
                    'M':         {'b_bb': {'value' : 1, 'lo' : 0.5, 'hi' : 2}},
                    'H':         {'b_bb': {'value' : 1, 'lo' : 0.5, 'hi' : 2}},
                    'M+H':       {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}}, # No post-fit uncertainties
                },
                'H' : {
                    'Inclusive': {'b_bb': {'value' : 1, 'lo' : 0.5, 'hi' : 20}}, # No post-fit uncertainties
                    'L':         {'b_bb': {'value' : 1, 'lo' : 0.5, 'hi' : 20}},
                    'M':         {'b_bb': {'value' : 1, 'lo' : 0.5, 'hi' : 2}}, # No post-fit uncertainties
                    'H':         {'b_bb': {'value' : 1, 'lo' : 0.5, 'hi' : 20}},
                    'M+H':       {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}}, # No post-fit uncertainties
                },
            },
            'particleNetMD_Xcc': {
                'L' : {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M+H':       {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}}, # No post-fit uncertainties
                },
                'M' : {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M+H':       {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
                'H' : {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 0.2, 'hi' : 20}}, # No post-fit uncertainties
                    'M':         {'c_cc': {'value' : 1, 'lo' : 0.2, 'hi' : 20}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M+H':       {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
            },
        },
        '2018': {
            'btagDDBvLV2': {
                'L' : {
                    'Inclusive': {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M':         {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'H':         {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M+H':       {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
                'M' : {
                    'Inclusive': {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M':         {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'H':         {'b_bb': {'value' : 1, 'lo' : -20, 'hi' : 20}},
                    'M+H':       {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
                'H' : {
                    'Inclusive': {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M':         {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'H':         {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M+H':       {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
            },
            'btagDDCvLV2': {
                'L' : {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M+H':       {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
                'M' : {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M+H':       {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
                'H' : {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M+H':       {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
            },
            'particleNetMD_Xbb': {
                'L' : {
                    'Inclusive': {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'b_bb': {'value' : 1, 'lo' : 0.5, 'hi' : 10000}},
                    'M':         {'b_bb': {'value' : 1, 'lo' : 0.5, 'hi' : 2}},
                    'H':         {'b_bb': {'value' : 1, 'lo' : 0.5, 'hi' : 20}},
                    'M+H':       {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
                'M' : {
                    'Inclusive': {'b_bb': {'value' : 1, 'lo' : 0.5, 'hi' : 100}},
                    'L':         {'b_bb': {'value' : 1, 'lo' : 0.5, 'hi' : 100}},
                    'M':         {'b_bb': {'value' : 1, 'lo' : 0.5, 'hi' : 2}},
                    'H':         {'b_bb': {'value' : 1, 'lo' : 0.5, 'hi' : 2}},
                    'M+H':       {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
                'H' : {
                    'Inclusive': {'b_bb': {'value' : 1, 'lo' : 0.5, 'hi' : 20}},
                    'L':         {'b_bb': {'value' : 1, 'lo' : 0.5, 'hi' : 20}},
                    'M':         {'b_bb': {'value' : 1, 'lo' : 0.5, 'hi' : 2}},
                    'H':         {'b_bb': {'value' : 1, 'lo' : 0.5, 'hi' : 20}},
                    'M+H':       {'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
            },
            'particleNetMD_Xcc': {
                'L' : {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M+H':       {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
                'M' : {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M+H':       {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
                'H' : {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 0.2, 'hi' : 20}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 0.2, 'hi' : 20}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M+H':       {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
            },
        }
    },

    'mergebbcc': {
        '2016': {
            'btagDDCvLV2': {
                'Inclusive': {'bb_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : -2, 'hi' : 2}},
                'L':         {'bb_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : -2, 'hi' : 2}},
                'M':         {'bb_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : -2, 'hi' : 2}},
                'H':         {'bb_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : -2, 'hi' : 2}},
            },
        },
        '2018': {
            'btagDDCvLV2': {
                'Inclusive': {'bb_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : -2, 'hi' : 2}},
                'L':         {'bb_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : -2, 'hi' : 2}},
                'M':         {'bb_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : -2, 'hi' : 2}},
                'H':         {'bb_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : -2, 'hi' : 2}},
            },
        }
    },



    'ULWPs': {
        #'2016': {
        #    'btagDDBvLV2': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : -20, 'hi' : 20}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
        #    'btagDDCvLV2': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 20}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}}
        #},
        '2017': {
            'btagDDBvLV2': {
                'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
            },
            'btagDDCvLV2': {
                'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
            }
        },
        '2018': {
            'btagDDBvLV2': {
                'L' : {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
                'M' : {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
                'H' : {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                },
            },
            'btagDDCvLV2': {
                'L' : {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
                'M' : {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
                'H' : {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
            },
            'particleNetMD_Xbb': {
                'L': {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : -10, 'hi' : 10}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : -10, 'hi' : 10}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
                'M': {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : -10, 'hi' : 10}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : -10, 'hi' : 10}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
                'H': {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
            },
            'particleNetMD_Xcc': {
                'L': {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
                'M': {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : -10, 'hi' : 10}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
                'H': {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : -10, 'hi' : 10}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
            },
        }
    },
    'ULWPs_maxdxySig': {
        #'2016': {
        #    'btagDDBvLV2': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : -20, 'hi' : 20}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
        #    'btagDDCvLV2': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 20}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}}
        #},
        '2017': {
            'btagDDBvLV2': {
                'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
            },
            'btagDDCvLV2': {
                'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
            }
        },
        '2018': {
            'btagDDBvLV2': {
                'Inclusive': {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                'L':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                'M':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                'H':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
            },
            'btagDDCvLV2': {
                'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
            },
            'particleNetMD_Xbb': {
                'L': {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
                'M': {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
                'H': {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
            },
            'particleNetMD_Xcc': {
                'L': {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                },
                'M': {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : -10, 'hi' : 10}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
                'H': {
                    'Inclusive': {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                    'L':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'M':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 1, 'hi' : 1}},
                    'H':         {'c_cc': {'value' : 1, 'lo' : 0, 'hi' : 2}, 'b_bb': {'value' : 1, 'lo' : 1, 'hi' : 1}, 'l': {'value' : 1, 'lo' : 0, 'hi' : 2}},
                },
            },
        }
    }
}
