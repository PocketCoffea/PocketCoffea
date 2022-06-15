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
    },
    "semileptonic" : {
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
    }
}
