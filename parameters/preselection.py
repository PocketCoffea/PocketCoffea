# Common cuts applied to objects

object_preselection = {
    "dilepton" : {
        "Muon": {
            "pt"  : 15,
            "eta" : 2.4,
            "iso" : 0.25,
            "id"  : "tightId",
        },
        "Electron" : {
            "pt"  : 15,
            "eta" : 2.4,
            "iso" : 0.06,
            "id"  : "mvaFall17V2Iso_WP80",
        },
        "Jet": {
            "dr": 0.4,
            "pt": 30,
            "eta": 2.4,
            "jetId": 2,
            "puId": 2,
            "puId_ptlim": 50,
        },
        "FatJet": {
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
    }
}
