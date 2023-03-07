from pocket_coffea.parameters.cuts.preselection_cuts import passthrough, semileptonic_presel_nobtag
from pocket_coffea.workflows.genweights import genWeightsProcessor
from pocket_coffea.lib.cut_functions import get_nObj_min

samples = ["ttHTobb",
           "TTToSemiLeptonic",
           "TTTo2L2Nu",
           "ST",
           "WJetsToLNu_HT"]

cfg =  {
    "dataset" : {
        "jsons": ["datasets/signal_ttHTobb.json",
                  "datasets/backgrounds_MC_ttbar.json",
                  "datasets/backgrounds_MC.json"],
        "filter" : {
            "samples": samples,
            "samples_exclude" : [],
            "year": ['2018']
        },
    },

    # Input and output files
    "workflow" : genWeightsProcessor,
    "output"   : "output/genweights/genweights_2018",
    "worflow_options" : {},

    "run_options" : {
        "executor"       : "dask/slurm",
        "workers"        : 1,
        "scaleout"       : 150,
        "queue"          : "standard",
        "walltime"       : "12:00:00",
        "mem_per_worker" : "4GB", # GB
        "exclusive"      : False,
        "chunk"          : 400000,
        "retries"        : 50,
        "treereduction"  : 10,
        "max"            : None,
        "skipbadfiles"   : None,
        "voms"           : None,
        "limit"          : None,
        "adapt"          : False,
    },

    # Cuts and plots settings
    "finalstate" : "semileptonic",
    "skim": [get_nObj_min(4, 15., "Jet") ],
    "preselections" : [semileptonic_presel_nobtag],
    "categories": {
        "baseline": [passthrough],
    },

    

    "weights": {
        "common": {
            "inclusive": ["genWeight","lumi","XS",
                          ],
            "bycategory" : {

            }
        },
        "bysample": {
        }
    },

    "variations": {
        "weights": {
            "common": {
                "inclusive": [  ],
                "bycategory" : {

                }
            },
        "bysample": {
        }    
        },
        
    },

   "variables": {

   },

}
