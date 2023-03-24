from pocket_coffea.parameters.cuts.preselection_cuts import *
from pocket_coffea.workflows.tthbb_base_processor import ttHbbBaseProcessor
from pocket_coffea.lib.cut_functions import get_nObj_min, get_nObj_eq, get_HLTsel, get_nObj_less
from pocket_coffea.parameters.histograms import *
from pocket_coffea.lib.columns_manager import ColOut
from pocket_coffea.parameters.btag import btag_variations

def ptmsd(events, params, **kwargs):
    # Mask to select events with a fatjet with minimum softdrop mass and maximum tau21
    #return (events.FatJetGood[:,0].pt > params["pt"]) & (events.FatJetGood[:,0].msoftdrop > params["msd"])
    mask = (events.FatJetGood.pt > params["pt"]) & (events.FatJetGood.msoftdrop > params["msd"])

    assert not ak.any(ak.is_none(mask, axis=1)), f"None in ptmsd\n{events.FatJetGood.pt[ak.is_none(mask, axis=1)]}"

    return ak.where(~ak.is_none(mask, axis=1), mask, False)

def get_ptmsd(pt, msd, name=None):
    if name == None:
        name = f"pt{pt}msd{msd}"
    return Cut(
        name=name,
        params= {"pt" : pt, "msd" : msd},
        function=ptmsd,
        collection="FatJetGood"
    )

def flavor_mask(events, params, **kwargs):
    mask = {
        "l"  : events.FatJetGood.hadronFlavour < 4,
        "c"  : events.FatJetGood.hadronFlavour == 4,
        "b"  : events.FatJetGood.hadronFlavour == 5,
        "cc" : abs(events.FatJetGood.hadronFlavour == 4) & (events.FatJetGood.nBHadrons == 0) & (events.FatJetGood.nCHadrons >= 2),
        "bb" : abs(events.FatJetGood.hadronFlavour == 5) & (events.FatJetGood.nBHadrons >= 2)
    }

    if params["flavor"] in ["bb", "cc"]:
        return mask[params["flavor"]]
    elif params["flavor"] == "b":
        return mask[params["flavor"]] & ~mask["bb"]
    elif params["flavor"] == "c":
        return mask[params["flavor"]] & ~mask["cc"]
    elif params["flavor"] == "l":
        return mask[params["flavor"]] & ~mask["bb"] & ~mask["cc"] & ~mask["b"] & ~mask["c"]
    else:
        raise NotImplementedError

def get_flavor(flavor):
    return Cut(
        name=flavor,
        params={"flavor": flavor},
        function=flavor_mask,
        collection="FatJetGood"
    )

samples = ["TTToSemiLeptonic",
           "ttHTobb",
           "DATA_SingleMuon"
          ]

subsamples = {}
for s in filter(lambda x: 'DATA' not in x, samples):
    subsamples[s] = {f"{s}_{f}" : [get_flavor(f)] for f in ['l', 'c', 'b', 'cc', 'bb']}

cfg =  {
    "dataset" : {
        "jsons": ["datasets/signal_ttHTobb_local.json",
                  "datasets/backgrounds_MC_TTbb_local.json",
                  "datasets/backgrounds_MC_ttbar_local.json",
                  "datasets/DATA_SingleMuon_local.json",
                  "datasets/DATA_SingleEle.json"],
        "filter" : {
            "samples": samples,
            "samples_exclude" : [],
            "year": ['2018']
        },
        "subsamples": subsamples
    },
    

    # Input and output files
    "workflow" : ttHbbBaseProcessor,
    "output"   : "output/test/test_subsamples",
    "worflow_options" : {},

    "run_options" : {
        "executor"       : "dask/slurm",
        "workers"        : 1,
        "scaleout"       : 100,
        "queue"          : "short",
        "walltime"       : "00:40:00",
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
        "env"            : "conda"
    },

    # Cuts and plots settings
    "finalstate" : "mutag",
    "skim": [get_nObj_min(1, 400., "FatJet"),
             get_HLTsel("semileptonic")],
    "preselections" : [semileptonic_presel, get_nObj_min(1, 450., "FatJetGood")],
    "categories": {
        "baseline": [passthrough],
        "pt450msd40" : [get_ptmsd(450., 40.)],
    },

    "weights": {
        "common": {
            "inclusive": ["genWeight","lumi","XS",
                          "pileup",
                          "sf_ele_reco", "sf_ele_id",
                          "sf_mu_id","sf_mu_iso",
                          "sf_btag", "sf_jet_puId", 
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
                "inclusive": [  "pileup",
                                "sf_ele_reco", "sf_ele_id",
                                "sf_mu_id", "sf_mu_iso", "sf_jet_puId",
                                *[ f"sf_btag_{b}" for b in btag_variations["2018"]]                               
                              ],
                "bycategory" : {
                }
            },
        "bysample": {
        }    
        },
        
    },

   "variables":
    {
        **fatjet_hists(coll="FatJetGood", fields=["pt", "eta", "phi",]),
        **fatjet_hists(coll="FatJetGood", fields=["pt", "eta", "phi",], pos=0),
        **ele_hists(coll="ElectronGood", pos=0),
        **muon_hists(coll="MuonGood", pos=0),
    },

    "columns": {
        "common": {
            "inclusive": [ ColOut("FatJetGood", ["pt", "eta", "phi"])]
        },
        "bysample" : { subs : { "inclusive" : [ColOut("FatJetGood", ["hadronFlavour", "nBHadrons", "nCHadrons"])] } for subs in subsamples }
    }
}
