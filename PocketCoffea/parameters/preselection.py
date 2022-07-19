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
    },
    "semileptonic_triggerSF" : {
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
    "mutag" : {
        "Muon" : {
            "pt"  : 5,
            "eta" : 2.4,
            "iso" : 0.15,
            "id"  : "tightId"
        },
        "Electron" : {
            "pt"  : 15,
            "eta" : 2.4,
            "iso" : 0.06,
            "id"  : "mvaFall17V2Iso_WP80",
        },
        "Jet": {
            "dr"  : 0.4,
            "pt"  : 25,
            "eta" : 2.5,
            "jetId" : 2,
            "puId"  : 2,
            "puId_ptlim" : 50,
        },
        "FatJet" : {
            "pt"  : 250,
            "eta" : 2.4,
            "jetId" : 2,
            "nsubjet" : 2,
        }
    }
}
