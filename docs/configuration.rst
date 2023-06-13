Configuration
#############






A PocketCoffea analysis can be customized by writing a configuration file, containing all the information needed
to setup an analysis run.

The PocketCoffea configuration comprehends:

- Input dataset specification
- Running mode configuration:  local, multiprocessing, cluster

- Custom processor specification
- Skimming, preselection and categories 
- Weights configuration
- Systematic variations configuration
- Histograms output configuration

The configuration is formatted as a python file containing a `cfg` dictionary.
See the detailed sections below for a descripton of each component.

.. code-block:: python
                
    from pocket_coffea.workflows.tthbb_base_processor import ttHbbBaseProcessor
    from pocket_coffea.parameters.cuts.preselection_cuts import *
    from pocket_coffea.lib.cut_functions import get_nObj_min, get_nObj_eq, get_nBtag
    from pocket_coffea.parameters.histograms import *
    
    cfg = {
        "dataset" : {
            "jsons": ["datasets/signal_ttHTobb_lxplus.json",
                      "datasets/backgrounds_MC.json"],
            "filter" : {
                "samples": ["TTToSemiLeptonic"],
                "samples_exclude" : [],
                "year": ['2018']
            }
        },

        "workflow" : ttHbbBaseProcessor,
        "output"   : "output/test_base",
        "worflow_options" : {},

        "run_options" : {
            "executor"       : "dask/lxplus",
            "workers"        : 1,
            "scaleout"       : 100,
            "queue"          : "microcentury",
            ...
        },

        # skimming, preselection and categories
        
        "finalstate" : "semileptonic",
        
        "skim": [get_nObj_min(4, 15., "Jet") ],
        
        "preselections" : [semileptonic_presel],
        
        "categories": {
            "baseline": [passthrough],
            "1b" : [ get_nBtag(1, coll="BJetGood")],
            "2b" : [ get_nBtag(2, coll="BJetGood")],
            "3b" : [ get_nBtag(3, coll="BJetGood")],
            "4b" : [ get_nBtag(4, coll="BJetGood")]
        },


        "weights": {
            "common": {
                "inclusive": ["genWeight","lumi","XS",
                              "pileup",
                              "sf_ele_reco", "sf_ele_id",
                              "sf_mu_id","sf_mu_iso",
                              "sf_btag",
                              "sf_btag_calib", "sf_jet_puId", 
                              ],
                "bycategory" : {
                    ....
                }
            },
            "bysample": {
                .....
            }
        },

        "variations": {
            "weights": {
                "common": {
                    "inclusive": [  "pileup",
                                    "sf_ele_reco", "sf_ele_id",
                                    "sf_mu_id", "sf_mu_iso", "sf_jet_puId",

                                  ],
                    "bycategory" : {
                        ....
                    }
                },
            "bysample": {
            }    
            },

        },

       "variables":
       {
           # 1D plots
            "jet_pt_leading": HistConf(
                [
                    Axis(coll="JetGood", field="pt", pos=0, bins=40, start=0, stop=1000,
                         label="Leading jet $p_T$"),
                ]
            ),

            # 2D plots
            "jet_eta_pt_leading": HistConf(
                [
                    Axis(coll="JetGood", field="pt", pos=0, bins=40, start=0, stop=1000,
                         label="Leading jet $p_T$"),
                    Axis(coll="JetGood", field="eta", pos=0, bins=40, start=-2.4, stop=2.4,
                         label="Leading jet $\eta$"),
                ]
            ),
            
            .... 
   
        }
    }

                

Datasets
========
The dataset configuration has the following structure:

.. code-block:: python

   "dataset" : {
            "jsons": ["datasets/signal_ttHTobb_lxplus.json",
                      "datasets/backgrounds_MC.json"],
            "filter" : {
                "samples": ["TTToSemiLeptonic"],
                "samples_exclude" : [],
                "year": ['2018']
            }
        },


- The `jsons` key contains the list of dataset definition file to consider as inputs
- The `filter` dictionary gives the user the possibility to filter on the fly the desidered samples to include or
  exclude from the full list taken from the jsons files. Samples can be filtered by name of by year.


Workflow
========

.. code-block:: python

   from pocket_coffea.workflows.tthbb_base_processor import ttHbbBaseProcessor
            
   "workflow" : ttHbbBaseProcessor,
   "output"   : "output/tth_semilep",
   "worflow_options" : {},


- `workflow` key specifies directly the class to use.
- `output` key contains the location of the output files. A version number is appended to the name at each run. 
- `workflow_options`: dictionary with additional options for specific processors (user defined)

  
Cuts and categories
===================

The events skimming, preselection and categorization is defined in a structured way in PocketCoffea:
see :ref:`filter_concept` for a detailed explanation of the difference between the steps.

.. code-block:: python
                
   # skimming, preselection and categories

   from pocket_coffea.lib.cut_functions import get_nObj_min, get_nObj_eq, get_nBtag
   
   "finalstate" : "semileptonic",
   
   "skim": [get_nObj_min(4, 15., "Jet") ],
   
   "preselections" : [semileptonic_presel],
   
   "categories": {
       "baseline": [passthrough],
       "1b" : [ get_nBtag(1, coll="BJetGood")],
       "2b" : [ get_nBtag(2, coll="BJetGood")],
       "3b" : [ get_nBtag(3, coll="BJetGood")],
       "4b" : [ get_nBtag(4, coll="BJetGood")]
   },


- Finalstate
     The finalstate key is used to define the set of HLT trigger to apply and should also be used by users to
     parametrize the object preselection code, in order to have a consistent trigger and object preselection
     configuration. **N.B.** the skimming step is applied before any object correction and preselection.
- Skim
     The skim configuration is a list of `Cut` object. Events passing the **AND** of the list of cuts pass the skim
- Preselections
     Again the preselection is a list of `Cut` object and **AND** between them is applied.
     **N.B.** :The preselection cut is applied after objects preselections and cleaning.
- Categories:
     Categories are defined by a dictionary which assigns unique names to list of `Cut` object.

The `Cut` objects are defined in `pocket_coffea.lib.cut_definition`.
Have a look at the documentation about the :ref:`cutobject` and its API (:ref:`cut_definition`).
This is a simple object grouping a name, a cut function, a dictionary of parameters.
The same `Cut` object can be used in different points of the configuration.

PocketCoffea implements a set of **factory methods** for common cut functions: they are defined in :ref:`cut_functions_lib`.


Weights
=======

Weights are handled in PocketCoffea through the `WeightsManager` object (see API :ref:`weightsmanager`).
The configuration file sets which weight is applied to which sample in which category.

.. code-block:: python
                
   "weights": {
        "common": {
            "inclusive": ["genWeight","lumi","XS",
                          "pileup",
                          "sf_ele_reco", "sf_ele_id",
                          "sf_mu_id","sf_mu_iso",
                          "sf_btag",
                          "sf_btag_calib", 
                          ],
            "bycategory" : {
                ....
            }
        },
        "bysample": {
            "ttHbb": {
                 "inclusive" : ["sf_jet_puId"],
                 "bycategory": {
                    ....
                 }
            }
        }
    },


To reduce boilerplate configuration the weights are specified following a `decision-tree` style and applied in a hierarchical fashion. 
Weights can be assigned to all samples (`common` key), inclusively or by category.
Weights can also be assigned to specific samples, again inclusively or in specific categories.

   
A set of *predefined weights* with centrally produced corrections and scale factors for the CMS Run2 ultra-legacy
analysis have been already implemented in PocketCoffea and are available in the configuration by using string
identifiers:

- **genWeight**: MC generator weight
- **lumi**
- **XS**: sample cross-section
- **pileup**: pileup scale factor
- **sf_ele_reco**, **sf_ele_id**: electron reconstruction and ID scalefactors. The working point is defined by the
  `finalstate` configuration.
- **sf_mu_id**, **sf_mu_iso**: muon id and isolation SF.
- **sf_btag**: btagPOG shape scale factors
- **sf_btag_calib**: custom computed btag SF corrections for ttHbb
- **sf_jet_puId**:  jet puID SF

If a weight is requested in the configuration, but it doens't exist, the framework emits an error before running.

On-the-flight custom weights
----------------------------

Weights can be created by the user directly in the configuration. The `WeightCustom` object allows to create
a function with a name that get called for each chunk to produce an array of weights (and optionally their variations).
Have a look at the API :ref:`weightsmanager`. 

.. code-block:: python

   WeightCustom(
      name="custom_weight",
      function= lambda events, size, metadata: [("pt_weight", 1 + events.JetGood[:,0].pt/400.)]
   )

The custom weight can be added in the configuration instead of the string identifier of centrally-defined weights.

.. code-block:: python

   custom_w = WeightCustom(
      name="custom_weight",
      function= lambda events, size, metadata: [("pt_weight", 1 + events.JetGood[:,0].pt/400.)]
   )

    "weights": {
        "common": {
            "inclusive": [... ],
            "bycategory" : {
                "3jets": [custom_w]
            }
        }
   }


The user can create a library of custom weights and include them in the configuration.



Variations
==========

Histograms configuration
========================
 
