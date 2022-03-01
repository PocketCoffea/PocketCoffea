# Common cuts applied to objects

cuts = {
    "dilepton" : {
        "muons": {
            "type": "mu",
            "leading_pt": {
                '2016' : 25,
                '2017' : 25,
                '2018' : 25,
            },
            "subleading_pt": 15,
            "eta": 2.4,
            "leading_iso": 0.15,
            "subleading_iso": 0.25,
        },
        "electrons" : {
            "type": "el",
            "leading_pt": {
                '2016' : 25,
                '2017' : 25,
                '2018' : 25,
            },
            "subleading_pt": 15,
            "eta": 2.4
        },
        "jets": {
            "type": "jet",
            "dr": 0.4,
            "pt": 30,
            "eta": 2.4,
            "jetId": 2,
            "puId": 2
        },
        "fatjets": {
            "type": "fatjet",
            "dr": 0.8,
            "pt": 300,
            "eta": 2.4,
            "jetId": 2,
            "tau32cut": 0.4,
            "tau21cut": {
                '2016' : 0.35,
                '2017' : 0.45,
                '2018' : 0.45,
            },
            "tau21DDTcut": {
                '2016' : 0.43,
                '2017' : 0.43,
                '2018' : 0.43,
            },
        },
        "W": {
            'min_mass': 65,
            'max_mass': 105
        },
        "met"   : 40,
        'btags' : 0
    }
}
