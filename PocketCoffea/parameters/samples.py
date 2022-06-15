# Information about cross section and MC weights of the generated samples

samples_info = {
    "ttHTobb": {
            "process": "ttHTobb",
            "XS": 0.2953,
            #"ngen_weight": 2410620.0644499995 #reduced file list
            "ngen_weight": {
                '2016' : 5253482.85,
                '2017' : 4216319.31,
                '2018' : 4996147.01 #11231808.09,
                },
            },
    "TTToSemiLeptonic": {
            "XS": 365.4574,
            "ngen_weight": {
                '2016' : 32366940321.33,
                '2017' : 59457024911.66,
                '2018' : 60050384119.23,
                },
            },
    "TTTo2L2Nu": {
            "XS": 88.3419,
            "ngen_weight": {
                '2016' : 4784620999.11,
                '2017' : 648729877.29,
                '2018' : 4622080044.95,
                },
            },
    "TTToHadronic": {
            "XS": 377.9607,
            "ngen_weight": {
                '2016' : 21500086465.24,
                '2017' : 61932449366.28,
                '2018' : 62639466237.,
                },
            },
    "ST_s-channel_4f_leptonDecays": {
      "process": "ST_s-channel",
      "XS": 3.36,#3.702224,
      #"ngen_weight": 24856809.513425056 #reduced file list
            "ngen_weight": {
                '2016' : 36768937.25,
                '2017' : 37052021.59,
                '2018' : 74634736.73,
                },
      },
    "ST_tW_antitop_5f_NoFullyHadronicDecays": {
      "process": "ST_tW_antitop",
      ### !!!!!!!!!!!!!!! TO BE CHECKED !!!!!!!!!!!!!!! ###
      "XS": 35.85,
      ### !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! ###
      #"ngen_weight": 182291193.36093727 #reduced file list
            "ngen_weight": {
                '2016' : 174109580.67,
                '2017' : 279005351.85,
                '2018' : 266470421.96,
                },
      },
    "ST_tW_top_5f_NoFullyHadronicDecays": {
      "process": "ST_tW_top",
      ### !!!!!!!!!!!!!!! TO BE CHECKED !!!!!!!!!!!!!!! ###
      "XS": 35.85,
      ### !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! ###
      #"ngen_weight": 241590614.9098064 #reduced file list
            "ngen_weight": {
                '2016' : 173908712.95,
                '2017' : 272081073.53,
                '2018' : 334874722.20,
                },
      },
    "ST_t-channel_antitop_4f_InclusiveDecays": {
      "process": "ST_t-channel_antitop",
      "XS": 80.95,
            "ngen_weight": {
                '2016' : 17771478.65,
                '2017' : 64689262.55,
                '2018' : 5125996535.38,
                },
      },
    "ST_t-channel_top_4f_InclusiveDecays": {
      "process": "ST_t-channel_top",
      "XS": 136.02,
            "ngen_weight": {
                '2016' : 67975483.38,
                '2017' : 5982064.0,
                '2018' : 16603455266.97,
                },
      },
}
