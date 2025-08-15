# Configuration

A PocketCoffea analysis can be customized by writing a configuration file, containing all the information needed
to setup an analysis run.

The PocketCoffea configuration comprehends:

- Input dataset specification
- Analysis parameters (see [Parameters](./parameters.md) page)
- Custom processor specification
- Skimming, preselection and categories 
- Weights configuration
- Systematic variations configuration
- Histograms output configuration
- Running mode configuration:  local, multiprocessing, cluster

:::{note}
The configuration is wrapped by a `Configurator` object, usually saved in a python script containing a `cfg` variable.
:::

A full simplified example is available
[here](https://github.com/PocketCoffea/AnalysisConfigs/blob/main/configs/zmumu/example_config.py).
In this page we will describe in details all the components of a more complete example about [ttHbb semileptonic channel](https://github.com/PocketCoffea/AnalysisConfigs/blob/main/configs/tests/analysis_config_subsamples.py).

```python    
from pocket_coffea.utils.configurator import Configurator
from pocket_coffea.lib.cut_definition import Cut

from pocket_coffea.lib.cut_functions import  get_nPVgood, goldenJson, eventFlags
from pocket_coffea.lib.cut_functions import  get_nObj_min, get_HLTsel, get_nBtagEq, get_nBtagMin
from pocket_coffea.parameters.cuts import passthrough
from pocket_coffea.parameters.histograms import *
import os

from pocket_coffea.workflows.tthbb_base_processor import ttHbbBaseProcessor 
from pocket_coffea.lib.weights.common import common_weights
from pocket_coffea.lib.calibrators.common import default_calibrators_sequence

# importing custom cut functions
from custom_cut_functions import *
localdir = os.path.dirname(os.path.abspath(__file__))

# Loading default parameters
from pocket_coffea.parameters import defaults
default_parameters = defaults.get_default_parameters()
defaults.register_configuration_dir("config_dir", localdir+"/params")

# merging additional analysis specific parameters
parameters = defaults.merge_parameters_from_files(default_parameters,
                                                  f"{localdir}/params/object_preselection.yaml",
                                                  f"{localdir}/params/btagsf_calibration.yaml",
                                                  f"{localdir}/params/triggers.yaml",
                                                  update=True)

# Configurator instance
cfg = Configurator(
    parameters = parameters,
    datasets = {
        "jsons": [f"{localdir}/datasets/backgrounds_MC_ttbar_2018.json",
                  f"{localdir}/datasets/backgrounds_MC_ttbar_2017.json",
                  f"{localdir}/datasets/DATA_SingleEle.json",
                  f"{localdir}/datasets/DATA_SingleEle.json",
                    ],
        "filter" : {
            "samples": ["TTToSemiLeptonic","DATA_SingleEle","DATA_SingleEle"],
            "samples_exclude" : [],
            "year": ['2018','2017']
        },
        "subsamples":{
            "TTToSemiLeptonic": {
                "=1b":  [get_nBtagEq(1, coll="Jet")],
                "=2b" : [get_nBtagEq(2, coll="Jet")],
                ">2b" : [get_nBtagMin(3, coll="Jet")]
            }
        }
    },

    workflow = ttHbbBaseProcessor,
    workflow_options = {},
    
    # Skimming and categorization
    skim = [
        get_nPVgood(1), eventFlags, goldenJson,
        get_nObj_min(4, 15., "Jet"),
        get_HLTsel()
    ],
             
    preselections = [semileptonic_presel_nobtag],
    
    categories = {
        "baseline": [passthrough],
        "1b" : [ get_nBtagEq(1, coll="BJetGood")],
        "2b" : [ get_nBtagEq(2, coll="BJetGood")],
        "3b" : [ get_nBtagEq(3, coll="BJetGood")],
        "4b" : [ get_nBtagEq(4, coll="BJetGood")]
    },
    
    # Weights and calibrators configuration
    weights_classes = common_weights,
    calibrators = default_calibrators_sequence,  # JetsCalibrator, METCalibrator, ElectronsScaleCalibrator

    weights = {
        "common": {
            "inclusive": ["genWeight","lumi","XS",
                          "pileup",
                          "sf_ele_reco", "sf_ele_id",
                          "sf_mu_id","sf_mu_iso",
                          "sf_btag", "sf_jet_puId", 
                          ],
            "bycategory" : {
                "2jets_20pt" : [.....]
            }
        },
        "bysample": {
             "TTToSemiLeptonic": {
                  "inclusive": [...],
                  "bycategory": { 
                       "2jets_20pt": [....]
                   }
             }
        }
    },

    variations = {
        "weights": {
            "common": {
                "inclusive": [  "pileup",
                                "sf_ele_reco", "sf_ele_id",
                                "sf_mu_id", "sf_mu_iso",
                                 "sf_jet_puId","sf_btag"  
                              ],
                "bycategory" : {
                }
            },
            "bysample": {
              "TTToSemiLeptonic": {
                    "inclusive": [],
                    "bycategory": {}
                }
            } 
        },
        "shape": {
        ....
        }
    },

    
    variables = {
        "HT" : HistConf([Axis(coll="events", field="events", bins=100, start=0, stop=200, label="HT")] ),
        "leading_jet_pt_eta" : HistConf(
            [
                Axis(coll="JetGood", field="pt", bins=40, start=0, stop=200, pos=0, label="Leading jet $p_T$"),
                Axis(coll="JetGood", field="eta", bins=40, start=-5, stop=5, pos=0, label="Leading jet $\eta$")
            ] ),
            
         #Plotting all jets together
         "all_jets_pt_eta" : HistConf(
            [
                Axis(coll="JetGood", field="pt", bins=40, start=0, stop=200, pos=None, label="All jets $p_T$"),
                Axis(coll="JetGood", field="eta", bins=40, start=-5, stop=5, pos=None, label="All jets $\eta$")
            ] ),
            
        "subleading_jetpt_MET" : HistConf(
            [
                Axis(coll="JetGood", field="pt", bins=40, start=0, stop=200, pos=0, label="Leading jet $p_T$"),
                Axis(coll="MET", field="pt", bins=40, start=0, stop=100, label="MET")
            ] ),
                
        **ele_hists(coll="ElectronGood", pos=0),
        **muon_hists(coll="MuonGood", pos=0),
        **count_hist(name="nElectronGood", coll="ElectronGood",bins=3, start=0, stop=3),
        **count_hist(name="nMuonGood", coll="MuonGood",bins=3, start=0, stop=3),
        **count_hist(name="nJets", coll="JetGood",bins=10, start=4, stop=14),
        **count_hist(name="nBJets", coll="BJetGood",bins=12, start=2, stop=14),
        **jet_hists(coll="JetGood", pos=0),
        **jet_hists(coll="JetGood", pos=1),
        **jet_hists(coll="JetGood", pos=2),
        
    },

    columns = {
        "common": {
             "inclusive": [],
             "bycategory": {}
        },
        "bysample": {
            "TTToSemiLeptonic" : { "inclusive":  [ColOut("LeptonGood",["pt","eta","phi"])]},
            "TTToSemiLeptonic__=1b" :{ "inclusive":  [ColOut("JetGood",["pt","eta","phi"])]},
            "TTToSemiLeptonic__=2b":{ "inclusive":  [ColOut("BJetGood",["pt","eta","phi"])]},
        }
    }
)

```
                

## Datasets

The dataset configuration has the following structure:

```python
cfg = Configurator(
    datasets = {
        "jsons": [f"{localdir}/datasets/backgrounds_MC_ttbar_2018.json",
                  f"{localdir}/datasets/backgrounds_MC_ttbar_2017.json",
                  f"{localdir}/datasets/DATA_SingleEle.json",
                  f"{localdir}/datasets/DATA_SingleEle.json",
                    ],
        "filter" : {
            "samples": ["TTToSemiLeptonic","DATA_SingleEle","DATA_SingleEle"],
            "samples_exclude" : [],
            "year": ['2018','2017']
        },
        "subsamples":{
            "TTToSemiLeptonic": {
                "=1b":  [get_nBtagEq(1, coll="Jet")],
                "=2b" : [get_nBtagEq(2, coll="Jet")],
                ">2b" : [get_nBtagMin(3, coll="Jet")]
            }
        }
    },
    ....
    )
```

- The `jsons` key contains the list of dataset definition file to consider as inputs
- The `filter` dictionary gives the user the possibility to filter on the fly the desidered samples to include or
  exclude from the full list taken from the jsons files. Samples can be filtered by name of by year.
  
- `subsamples` makes possible to define cuts splitting the events in multiple sub-samples. See
  the [datasets](./datasets.md) page for a more in-depth definition of them. A list of Cut objects is used to define the
  subsample, an AND between them is used to mask the events. 
  
  In the example, by using the `subsamples` option
  effectively the `TTToSemiLeptonic` sample will be split in the framework in 3 pieces called  `TTToSemiLeptonic__=1b`,
  `TTToSemiLeptonic__=2b`, `TTToSemiLeptonic__>2b`. 
  
  :::{warning}
  Subsamples do not need to be exclusive. Subsample masks are applied before exporting histograms, columns and counting events.
  :::

### Subsample-specific Configuration

PocketCoffea allows applying weights and systematic variations specific to subsamples. When subsamples are defined, they can be referenced in the `weights` and `variations` configuration using their full name (original sample name + subsample suffix).

In the example configuration, the `TTToSemiLeptonic` sample is split into three subsamples:
- `TTToSemiLeptonic__=1b`
- `TTToSemiLeptonic__=2b` 
- `TTToSemiLeptonic__>2b`

These subsamples can have different weight and variation configurations:

```python
cfg = Configurator(
    datasets = {
        "subsamples":{
            "TTToSemiLeptonic": {
                "=1b":  [get_nBtagEq(1, coll="Jet")],
                "=2b" : [get_nBtagEq(2, coll="Jet")],
                ">2b" : [get_nBtagMin(3, coll="Jet")]
            }
        }
    },
    
    weights = {
        "common": {
            "inclusive": ["genWeight", "lumi", "XS", "pileup"],
        },
        "bysample": {
            # Original sample weights (applied to all subsamples if not overridden)
            "TTToSemiLeptonic": {
                "inclusive": ["sf_btag", "sf_jet_puId"],
            },
            # Subsample-specific weights
            "TTToSemiLeptonic__=1b": {
                "inclusive": ["sf_btag_1b_specific"],  # Custom b-tag SF for 1b events
                "bycategory": {
                    "baseline": ["additional_1b_weight"]
                }
            },
            "TTToSemiLeptonic__=2b": {
                "inclusive": ["sf_btag_2b_specific"],  # Different b-tag SF for 2b events
            },
            "TTToSemiLeptonic__>2b": {
                "inclusive": ["sf_btag_multi_specific", "top_pt_reweight"],
            }
        }
    },
    
    variations = {
        "weights": {
            "bysample": {
                "TTToSemiLeptonic__=1b": {
                    "inclusive": ["sf_btag_1b_specific"],  # Only vary 1b-specific SF
                },
                "TTToSemiLeptonic__=2b": {
                    "inclusive": ["sf_btag_2b_specific"],
                },
                "TTToSemiLeptonic__>2b": {
                    "inclusive": ["sf_btag_multi_specific", "top_pt_reweight"],
                }
            }
        },
        "shape": {
            "bysample": {
                "TTToSemiLeptonic__>2b": {
                    "inclusive": ["JESTotal", "JER"]  # Only apply shape variations to high b-jet multiplicity
                }
            }
        }
    }
)
```

:::{tip}
Subsample-specific configurations are particularly useful for:
- Applying different scale factors based on event topology (e.g., different b-tagging efficiencies for different b-jet multiplicities)
- Implementing data-driven background estimation techniques that require different weights per subsample
:::

:::{warning}
When configuring weights and variations for subsamples, ensure that the subsample name exactly matches the automatically generated name format: `{original_sample}__{subsample_suffix}`.
:::


## Workflow

```python
from pocket_coffea.workflows.tthbb_base_processor import ttHbbBaseProcessor

cfg = Configurator(
    workflow : ttHbbBaseProcessor,
    worflow_options : {},
    ....
)
```

- `workflow` key specifies directly the class to use.
- `workflow_options`: dictionary with additional options for specific processors (user defined)

  
## Calibrators

Object calibrations and systematic variations are handled by the **Calibrators** system. The calibrator sequence is configured in the `Configurator`:

```python
from pocket_coffea.lib.calibrators.common import default_calibrators_sequence

cfg = Configurator(
    # Default calibrator sequence
    calibrators = default_calibrators_sequence,
    ....
)
```

The `calibrators` parameter accepts a list of calibrator classes that will be applied sequentially to correct physics objects and handle systematic variations.

For a detailed explanation of how calibrators work in the PocketCoffea workflow, see the [Object calibration and systematic variations](./concepts.md#object-calibration-and-systematic-variations) section in the Concepts page. For implementation details and creating custom calibrators, see the dedicated [Calibrators](./calibrators.md) page.



### Default Calibrator Sequence

PocketCoffea provides a default sequence of calibrators for common corrections:

```python
default_calibrators_sequence = [
    JetsCalibrator, 
    METCalibrator, 
    ElectronsScaleCalibrator
]
```

- **JetsCalibrator**: Applies JEC/JER corrections and provides systematic variations (JESUp/Down, JERUp/Down, etc.)
- **METCalibrator**: Propagates jet corrections to MET collection
- **ElectronsScaleCalibrator**: Handles electron energy scale and smearing corrections


:::{warning}
If the user does not specify a calibration sequence in the configuration, the default one defined in `pocket_coffea.lib.calibrators.common.common is used. It's always better to define explicitely the calibration sequence!
:::

### Custom Calibrator Sequence

Users can define custom calibrator sequences by creating their own list:

```python
from pocket_coffea.lib.calibrators.common import JetsCalibrator, METCalibrator
from my_custom_calibrators import MyCustomMuonCalibrator

# Custom sequence
my_calibrators = [
    JetsCalibrator,
    METCalibrator, 
    MyCustomMuonCalibrator,  # Custom calibrator
]

cfg = Configurator(
    calibrators = my_calibrators,
    ....
)
```

### Calibrator Parameters

Calibrator behavior is usually controlled through the `parameters` configuration. Key calibrator parameters include:

- **jets_calibration**: Controls JEC/JER application and variations
- **lepton_scale_factors**: Configures electron/muon scale factors and corrections
- **rescale_MET_config**: MET correction configuration

See the [Parameters](./parameters.md) page for detailed calibrator parameter configuration, the [Calibrators](./calibrators.md) page for implementation details, and the [Concepts](./concepts.md#object-calibration-and-systematic-variations) page for how calibrators integrate into the workflow.

:::{tip}
The calibrator sequence order matters! For example, `METCalibrator` must come after `JetsCalibrator` because it needs the corrected jets to properly propagate JEC effects to the MET.
:::

:::{warning}
All requested systematic variations from calibrators must be properly configured in the `variations` section of the configuration to be processed.
:::


## Cuts and categories

The events skimming, preselection and categorization is defined in a structured way in PocketCoffea:
see [Concepts#Filtering](./concepts.md#filtering) for a detailed explanation of the difference between the steps.

```python
cfg = Configurator(
   skim = [
            get_nPVgood(1), eventFlags, goldenJson,
            get_nObj_min(4, 15., "Jet"),
            get_HLTsel() 
          ],

   preselections = [semileptonic_presel_nobtag],

   categories = StandardSelection({
          "baseline": [passthrough],
           "1b" : [ get_nBtagEq(1, coll="BJetGood")],
           "2b" : [ get_nBtagEq(2, coll="BJetGood")],
           "3b" : [ get_nBtagEq(3, coll="BJetGood")],
           "4b" : [ get_nBtagEq(4, coll="BJetGood")]
      }),
   ....
)
```

A `Cut` is a simple object grouping a name, a cut function, a dictionary of parameters.
The same `Cut` object can be used in different points of the configuration.
The `Cut` objects are defined in `pocket_coffea.lib.cut_definition`.
Have a look at the documentation about the [Cut object](./concepts.md#cut-object) and its
[API](pocket_coffea.lib.cut_definition). 


:::{tip}
Custom functions and modules, defined locally by the user and not part of the central PocketCoffea core, 
must be registered in a special way to be available to the Dask workers. 
Have a look at the [Register user defined custom modules](./configuration.md#register-user-defined-custom-modules) section. 
:::

PocketCoffea implements a set of **factory methods** for common cut functions: they are defined in [cut_functions](pocket_coffea.lib.cut_functions).

In the configuration the categorization is split in:

- **Skim**:
     The skim configuration is a list of `Cut` object. Events passing the **AND** of the list of cuts pass the skim.
- **Preselections**:
     The preselection is a list of `Cut` object and **AND** between them is applied.

     :::{alert}
     **N.B.**: The preselection cut is applied after objects preselections and cleaning. 
     :::

- **Categories**: Splitting of events for histograms and columns output.


### Save skimmed NanoAOD
PocketCoffea can dump events passing the skim selection to NanoAOD root files. This can be useful when your skimming
efficiecy is high and you can trade the usage of some disk storage for higher processing speed. 

The export of skimmed NanoAOD is activated by the `save_skimmed_files` argument of the `Configurator` object. If
`save_skimmed_files!=None` then the processing stops after the skimming and one root file for each chunk is saved in the
folder specified by the argument. 

It is recommended to use a xrootd endpoint: `save_skimmed_files='root://eosuser.cern.ch:/eos/user/...`. 

```python
cfg = Configurator(
     
    workflow = ttHbbBaseProcessor,
    workflow_options = {},
    
    save_skimmed_files = "root://eosuser.cern.ch://eos/user/x/xxx/skimmed_samples/Run2UL/",
    skim = [get_nPVgood(1),
            eventFlags,
            goldenJson,
            get_nBtagMin(3, minpt=15., coll="Jet", wp="M"),
            get_HLTsel(primaryDatasets=["SingleEle", "SingleMuon"])],
    )

```

The PocketCoffea output file contains the list of skimmed files with the number of skimmed events in each file. Moreover
the root files contain a new branch called `skimRescaleGenWeight` which store for each event the scaling factor
needed to recover the sum of genWeight of the original factor, and correct for the skimming efficiency.  The factor
is computed as `(original sum of genweight / sum of genweights of skimmed files)` for each file. This factor needs to
be multiplied to the sum of genweights accumulated in each chunk by the processor that runs on top of skimmed
datasets. Therefore the dataset definition file for skimmed datasets must contain the `isSkim:True` metadata,
which is used by the processor to apply the rescaling.

:::{alert}
**N.B.**: The skim is performed before the object calibration and preselection step. The analyzer must be careful to
apply a loose enough skim that is invariant under the shape uncertainties applied later in the analysis. For example
the selection on the minimum number of jets should be loose enought to not be affected by Jet energy scales, **which
are applied later**. 
:::

A full tutorial of the necessar steps to produce a skim and then to use the pocketcoffea tools to prepare a new dataset
configuration file can be found in the [How To section](./recipes.md#skimming-events).

### Categorization utilities
PocketCoffea defines different ways to categorize events. 
The code is available at [pocket_coffea.lib.categorization](pocket_coffea.lib.categorization).

- **StandardSelection**:
     handles the definition of categories from a dictionary of Cut objects. Each key defines a category with a list of
     `Cut` objects which are applied with an **AND**.
     
    ```python
    categories = StandardSelection({
          "baseline": [passthrough],
           "1b" : [ get_nBtagEq(1, coll="BJetGood")],
           "2b" : [ get_nBtagEq(2, coll="BJetGood")],
           "3b" : [ get_nBtagEq(3, coll="BJetGood")],
           "4b" : [ get_nBtagEq(4, coll="BJetGood")]
      }),
    ```
     
- **CartesianSelection**: 
    handles the definition of cartesian product of categories. The class keeps a list of
    [MultiCut](pocket_coffea.lib.categorization.MultiCut) objects, each defining a set of subcategories (or bins).
    The `CartesianSelection` utils defines automatically categories which are the cartesian products of the bins defined
    by each MultiCut. 
    A `StandardSelection` object can be embedded in the CartesianSelection to defined "common" categories not used in the
    cartesian product.
    This utility can be very useful to build a differential analysis. 
    
    For example, this is the configuration to build categories as
    $((N_{jets} [4,5,>6]) \times (N_{bjets} [3,4,5,>6])) + \text{inclusive} + 4jets40pt$
    
    ```python
    categories = CartesianSelection(
        multicuts = [
            MultiCut(name="Njets",
                     cuts=[
                         get_nObj_eq(4, 15., "JetGood"),
                         get_nObj_eq(5, 15., "JetGood"),
                         get_nObj_min(6, 15., "JetGood"),
                     ],
                     cuts_names=["4j","5j","6j"]),
            MultiCut(name="Nbjet",
                    cuts=[
                         get_nObj_eq(3, 15., "BJetGood"),
                         get_nObj_eq(4, 15., "BJetGood"),
                         get_nObj_eq(5, 15., "BJetGood"),
                         get_nObj_min(6, coll="BJetGood"),
                     ],
                     cuts_names=["3b","4b","5b","6b"])
        ],
        common_cats = StandardSelection({
            "inclusive": [passthrough],
            "4jets_40pt" : [get_nObj_min(4, 40., "JetGood")]
        })
    ),
    ```
    
    
    :::{warning}
    The standard `PackedSelection` utility from coffea can handle a maximum of 64 categories. The `CartesianSelection`
    tool overcomes this limitation internally.
    :::
    
     

## Weights

Weights are handled in PocketCoffea through the `WeightsManager` object (see [API](pocket_coffea.lib.weights_manager.WeightsManager)).
The configuration file specifies which weight is applied to which sample in which category.

```python
from pocket_coffea.lib.weights.common import common_weights

cfg = Configurator(
    weights_classes = common_weights,
    weights = {
        "common": {
            "inclusive": ["genWeight","lumi","XS",
                          "pileup",
                          "sf_ele_reco", "sf_ele_id",
                          "sf_mu_id","sf_mu_iso",
                          "sf_btag", "sf_jet_puId", 
                          ],
            "bycategory" : {
                "2jets_20pt" : [.....]
            }
        },
        "bysample": {
             "TTToSemiLeptonic": {
                  "inclusive": [...],
                  "bycategory": { 
                       "2jets_20pt": [....]
                   }
             }
        }
    },
    ....
)
```

To reduce boilerplate configuration the weights are specified following a `decision-tree` style and applied in a hierarchical fashion. 
Weights can be assigned to all samples (`common` key), inclusively or by category.
Weights can also be assigned to specific samples, again inclusively or in specific categories.

The available weights for a configuration are defined by the **weights_classes** passed to the Configurator. The
framework implements a set of common weights definitions: if the `weights_classes` argument is not passed, the common
ones are used by default. The user can add new weights classes in the configuration and use the corresponding string in
the `weights` configuration dictionary. The Weight classes implement a mechanism to check that the definition of the
string is unique and that users cannot inadvertly overwrite already defined weights. 

A list of available weights definition: 
- **genWeight**: MC generator weight
- **lumi**
- **XS**: sample cross-section
- **pileup**: pileup scale factor
- **sf_ele_reco**, **sf_ele_id**: electron reconstruction and ID scalefactors. The working point is defined by the
  [`lepton_scale_factors`](https://github.com/PocketCoffea/PocketCoffea/blob/main/pocket_coffea/parameters/lepton_scale_factors.yaml)
  key in the parameters (see [Parameters](./parameters.md) docs)
- **sf_mu_id**, **sf_mu_iso**: muon id and isolation SF.
- **sf_btag**: btagPOG shape scale factors
- **sf_jet_puId**:  jet puID SF

If a weight is requested in the configuration, but it doens't exist, the framework emits an error before running.

### On-the-flight custom weights

Weights can be created by the user directly in the configuration. It is enough to define a new class deriving from
[WeightWrapper](pocket_coffea.lib.weights.WeightWrapper) class defined by the framework. 

This wrapper is used to instantiate the weight object for each chunk of data with the corresponding parameters and
metadata. Doing so the user can customize the weight for different samples and conditions. Moreover the WeightWrapper
instance defined the weight **name**, the string to be used in the config, and the available variations. 


```python
from pocket_coffea.lib.weights_manager import WeightWrapper

# example of WeightWrapper definition
class CustomTopSF(WeightWrapper):
    
    name = "top_sf"
    has_variations = True
    
    def __init__(self, params, metadata):
        super().__init__(params, metadata)
        self.sf_file = params["top_sf"]["json_file"]
        # custom variations from config
        self._variations = params["top_sf"]["variations"]
        
    def compute(self, events, size, shape_variation):
        # custom computation
        
        if shape_variation == "nominal":
            sf_data = sf_function.compute(self.sf_file, events)
            return WeightDataMultiVariation(
                name = self.name, 
                nominal = sf_data["nominal"],
                up = [sf_data[var] for var in self._variations["up"]],
                down = [sf_data[var] for var in self._variations["down"]]
            )
        else:
            return WeightData(
                name = self.name, 
                nominal = np.ones(size),
            )

```

The class must be then passed to the configurator in order to be available:

```python
from pocket_coffea.lib.weights.common import common_weights

cfg = Configurator(
    weights_classes = common_weights + [CustomTopSF],  # note the addition here
    weights = {
        "common": {
            "inclusive": ["genWeight","lumi","XS",
                          "pileup",
                          "sf_ele_reco", "sf_ele_id",
                          "sf_mu_id","sf_mu_iso",
                          "sf_btag", "sf_jet_puId", 
                          ],
            "bycategory" : {
                "2jets_20pt" : [  "top_sf"  # custom weight
                ]
            }
        },
        ...
        
    ....
)

```

Moreover, often weights are easier to define: simple computations can be wrapped in a lambda without the need of
defining a full WeightWrapper class. 

```python
from pocket_coffea.lib.weights.weights import WeightLambda

my_custom_sf  = WeightLambda.wrap_func(
    name="sf_custom",
    function=lambda params, metadata, events, size, shape_variations:
        call_to_my_fancy_function(events, params, metadata, shape_variations)
    has_variations=True
    )
```

The return type of the lambda must be a [WeightData](pocket_coffea.lib.weights.weights.WeightData) or [WeightDataMultiVariation](pocket_coffea.lib.weights.weights.WeightData) object.


:::{tip}
The user can create a library of custom weights and include them in the configuration.
:::

## Register user defined custom modules
Users can define modules, library and functions locally in their configuration folder and import then in the
PocketCoffea configuration and workflows. In order to make them available to the dask workers, without being included in
the PocketCoffea core library, it is sufficient to register the modules with `cloudpickle`. 

Add this code in the configuration file. 
```python

import cloudpickle
cloudpickle.register_pickle_by_value(workflow) # just an example of user defined modules for processors and cuts
cloudpickle.register_pickle_by_value(custom_cut_functions)
cloudpickle.register_pickle_by_value(custom_cuts)

```

## Variations

Systematics variations are also configured in the `Configurator`. Weights and shape variations are supported. 
The configuration is applied in an hierarchical fashion as for the `Weights`, to compact the matrix of
samples and categories. 

* **Weights variations**:  if the weights defined in the `WeightsManager` has up and down variations, they can be
  activated by just putting the weight name in the `variations` configuration. Up and down shapes will be exported for
  histograms. 
  
  ```python
  cfg = Configurator(
     ....
    variations = {
        "weights": {
            "common": {
                "inclusive": [  "pileup",
                                "sf_ele_reco", "sf_ele_id",
                                "sf_mu_id", "sf_mu_iso",
                                 "sf_jet_puId","sf_btag"  
                              ],
                "bycategory" : {
                }
            },
            "bysample": {
              "TTToSemiLeptonic": {
                    "inclusive": [],
                    "bycategory": {}
                }
            } 
        },
        "shape": {
        ....
        }
    },
    ...
  )
  ```
  
* **Shape variations**: shape variations are related to lepton, jets and MET scale variations and similar systematics. 
  These variations are provided by the calibrators configured in the [Calibrators](#calibrators) section.
  The handling of these variations is more complex since everything after skimming (see [docs](./concepts.md#filtering))
  is rerun for each shape variation. 
  
  Have a look at the base processor
  [get_shape_variations()](pocket_coffea.workflows.base.BaseProcessorABC.get_shape_variations) function to learn about
  their implementation. 
  
  
  ```pythony
  cfg = Configurator(
     ....
    variations = {
        "weights": .....
        # Shape variations
        "shape": {
            "common":
              "inclusive": ["JESTotal","JER"]
        }
    },
    ...
  )
  ```
  
  :::{warning}
  The available shape variations depend on the calibrators configured in the [Calibrators](#calibrators) section. 
  Currently, JES and JER variations are implemented and available through the `JetsCalibrator`. 
  The available JES variations depend on the jet calibration configuration defined in the parameters ([docs](./parameters.md#cross-references)).
  :::
  
  
## Histograms configuration
 
The PocketCoffea configuration allows the user to define histograms without modifying the processor code. 
The histogram configuration closely follows the interface of the [scikit-hep/hist](https://github.com/scikit-hep/hist)
library, used by Coffea to handle histograms.

Histograms are identified by unique labels and built using a [`HistConf`](pocket_coffea.lib.hist_manager.HistConf)
object. Each `HistConf` object has a list of [`Axis`](pocket_coffea.lib.hist_manager.Axis) objets, which follow the
interface of the `hist` library axes. 

:::{Important}
The number of Axis contained in a `HistConf` is not limited! The user can work with 1,2,3,4..D histograms without
changing the interface. However, be aware of memory issues which may affect large histograms with too many bins. 
:::


```python
cfg = Configurator(
   variables = {
        "HT" : HistConf([Axis(coll="events", field="events", bins=100, start=0, stop=200, label="HT")] ),
        "leading_jet_pt_eta" : HistConf(
            [
                Axis(coll="JetGood", field="pt", bins=40, start=0, stop=200, pos=0, label="Leading jet $p_T$"),
                Axis(coll="JetGood", field="eta", bins=40, start=-5, stop=5, pos=0, label="Leading jet $\eta$")
            ] ),
            
         #Plotting all jets together
         "all_jets_pt_eta" : HistConf(
            [
                Axis(coll="JetGood", field="pt", bins=40, start=0, stop=200, pos=None, label="All jets $p_T$"),
                Axis(coll="JetGood", field="eta", bins=40, start=-5, stop=5, pos=None, label="All jets $\eta$")
            ] ),
            
        "subleading_jetpt_MET" : HistConf(
            [
                Axis(coll="JetGood", field="pt", bins=40, start=0, stop=200, pos=0, label="Leading jet $p_T$"),
                Axis(coll="MET", field="pt", bins=40, start=0, stop=100, label="MET")
            ] ),
            
                
        **ele_hists(coll="ElectronGood", pos=0),
        **muon_hists(coll="MuonGood", pos=0),
        **count_hist(name="nElectronGood", coll="ElectronGood",bins=3, start=0, stop=3),
        **count_hist(name="nMuonGood", coll="MuonGood",bins=3, start=0, stop=3),
        **count_hist(name="nJets", coll="JetGood",bins=10, start=4, stop=14),
        **count_hist(name="nBJets", coll="BJetGood",bins=12, start=2, stop=14),
        **jet_hists(coll="JetGood", pos=0),
        **jet_hists(coll="JetGood", pos=1),
        **jet_hists(coll="JetGood", pos=2),
        
    },
    ...
)

```

The `HistConf` class has many options, particularly useful to exclude some categories or samples from a specific
histogram. 

```python

@dataclass
class HistConf:
    axes: List[Axis]
    storage: str = "weight"
    autofill: bool = True  # Handle the filling automatically
    variations: bool = True
    only_variations: List[str] = None
    exclude_samples: List[str] = None
    only_samples: List[str] = None
    exclude_categories: List[str] = None
    only_categories: List[str] = None
    no_weights: bool = False  # Do not fill the weights
    metadata_hist: bool = False  # Non-event variables, for processing metadata
    hist_obj = None
    collapse_2D_masks = False  # if 2D masks are applied on the events
    # and the data_ndim=1, when collapse_2D_mask=True the OR
    # of the masks on the axis=2 is performed to get the mask
    # on axis=1, otherwise an exception is raised
    collapse_2D_masks_mode = "OR"  # Use OR or AND to collapse 2D masks for data_ndim=1 if collapse_2D_masks == True

```

The `Axis` object has many options: in particular the array to be plotted is taken from the `events` mother array
using the `coll` and `field` attributed. If an array is global in NanoAOD, the `coll` is `events`. 

```python

@dataclass
class Axis:
    field: str  # variable to plot
    label: str  # human readable label for the axis
    bins: int = None
    start: float = None
    stop: float = None
    coll: str = "events"  # Collection or events or metadata or custom
    name: str = None      # Identifier of the axis: By default is built as coll.field, if not provided
    pos: int = None       # index in the collection to plot. If None plot all the objects on the same histogram
    type: str = "regular" # regular/variable/integer/intcat/strcat
    transform: str = None
    lim: Tuple[float] = (0, 0)
    underflow: bool = True
    overflow: bool = True
    growth: bool = False
```

:::{tip}
A set of factory methods to build commonly used histogram configuration is available in
[](pocket_coffea.parameters.histograms).
They produce dictionaries of `HistConf` objects that need to be *unpacked* in the configuration file with the syntax: `**jet_hists(coll="JetGood", pos=2)`
:::


### Multidimensional arrays
A special mention is worth it for the `pos` attributes. The user can specify which object in a collection to use for the
field to plot:  if the collection contains more
than 1 object, e.g. Jet, and `pos=1`,  only the attributes of the 2nd object will be plotted. If the second object is missing, the
attributes are None-padded automatically. 

:::{tip}
If the collection contains more objects (e.g. the Jet collection) and the attribute `pos` is None, the array
is flattened before filling the histograms. This means that you can plot the $p_T$ of all the jets in a single plot just
by using `Axes(coll="Jet", field="pt", pos=None)`
:::

## Columns output

In PocketCoffea it is also possible to export arrays from NanoAOD events: the configuration is handled with a
[`ColOut`](pocket_coffea.lib.columns_manager.ColOut) object.

The configuration follows the same structure of the `Weights` configuration. 
A list of `ColOut` objects is assigner either inclusively and to all the samples or specifically to a sample and
category. 

```python
cfg = Configurator(
   # columns output configuration
   columns = {
        "common": {
             "inclusive": [],
             "bycategory": {}
        },
        "bysample": {
            "TTToSemiLeptonic" : { "inclusive":  [ColOut("LeptonGood",["pt","eta","phi"])]},
            "TTToSemiLeptonic__=1b" :{ "inclusive":  [ColOut("JetGood",["pt","eta","phi"])]},
            "TTToSemiLeptonic__=2b":{ "inclusive":  [ColOut("BJetGood",["pt","eta","phi"])]},
        }
    }
    )
```

The `ColOut` object defines which collection and fields get exported in the output file, moreover by default the number 
of object in the collection is saved only once along the fields. This is needed because the output accumulator contains
flattened arrays. The output can then be unflattened using the saved number of objects.

```python
@dataclass
class ColOut:
    collection: str  # Collection
    columns: List[str]  # list of columns to export
    flatten: bool = True  # Flatten by defaul
    store_size: bool = True
    fill_none: bool = True
    fill_value: float = -999.0  # by default the None elements are filled
    pos_start: int = None  # First position in the collection to export. If None export from the first element
    pos_end: int = None  # Last position in the collection to export. If None export until the last element
```

Similarly to the `pos` option for the `Axes` configuration, it is possible to specify a range of objects to restrict the
output over the collection.


:::{Warning}
At the moment the output columns gets accumulated over all the chunks of the processed datasets and returned as a single
file. 
This may cause memory problems in case of a large number of events or exported data. A solution is to export single
files separately: the option is under development. 
:::


### Exporting chunks in separate files
When exporting arrays from the processor, the size of the output may become an issue. In fact, by default the coffea
processor will accumulate the `column_accumulators` for each chunk to produce the total output at the end of the
processing. This process may accumulate too much memory and crush the processing. 

To overcome this issue there is the possibility to export the `Columns` output of each chunk in a separate file,
without adding anything to the standard PocketCoffea output. The file can be saved in a local folder or sent remotely
with xrootd. 

:::{Warning}
With this setup the output will be in parquet format, so it is not necessary to flatten the awkward arrays before
saving. The full awkward structure can be kept in the output arrays. 
:::

To activate this mode, just add the option `dump_columns_as_arrays_per_chunk` in the `workflow_options` dictionary of
the `Configurator`. 
The target directory can be local (no xrootD prefix) or a xRootD localtion. 

The following configuration shows the setup in action. **N.B.:** the columns are not flattened (the default), because
the output parquet file will contain directly awkward arrays (not column accumulators).

```python
cfg = Configurator(
    parameters = parameters,
    datasets = {
        "jsons": [f"{localdir}/datasets/signal_ttHTobb_local.json",
                  f"{localdir}/datasets/backgrounds_MC_ttbar_local.json",
                  f"{localdir}/datasets/backgrounds_MC_TTbb_local.json"],
        "filter" : {
            "samples": ["ttHTobb", "TTToSemiLeptonic", "TTbbSemiLeptonic"],
            "samples_exclude" : [],
            "year": ["2016_PreVFP",
                     "2016_PostVFP",
                     "2017","2018"] #All the years
        }
    },

    workflow = PartonMatchingProcessor,
    workflow_options = {"parton_jet_min_dR": 0.3,
                        "dump_columns_as_arrays_per_chunk": "root://t3se01.psi.ch:1094//store/user/dvalsecc/ttHbb/output_columns_parton_matching/sig_bkg_05_07_2023_v1/"},
    
    .... 
    columns = {
        "common": {
            "bycategory": {
                    "semilep_LHE": [
                        ColOut("Parton", ["pt", "eta", "phi", "mass", "pdgId", "provenance"], flatten=False),
                        ColOut(
                            "PartonMatched",
                            ["pt", "eta", "phi","mass", "pdgId", "provenance", "dRMatchedJet",], flatten=False
                        ),
                        ColOut(
                            "JetGood",
                            ["pt", "eta", "phi", "hadronFlavour", "btagDeepFlavB"], flatten=False
                        ),
                        ColOut(
                            "JetGoodMatched",
                            [
                                "pt",
                                "eta",
                                "phi",
                                "hadronFlavour",
                                "btagDeepFlavB",
                                "dRMatchedJet",
                            ], flatten=False
                        ),
                        
                        ColOut("LeptonGood",
                               ["pt","eta","phi"],flatten=False,
                               pos_end=1, store_size=False),
                        ColOut("MET", ["phi","pt","significance"], flatten=False),
                        ColOut("Generator",["x1","x2","id1","id2","xpdf1","xpdf2"], flatten=False),
                        ColOut("LeptonParton",["pt","eta","phi","mass","pdgId"], flatten=False)
                    ]
                }
        },
        "bysample":{
            "ttHTobb":{
                "bycategory": {
                    "semilep_LHE": [ColOut("HiggsParton",
                                           ["pt","eta","phi","mass","pdgId"], pos_end=1, store_size=False, flatten=False)]
                }
            }
        }
    },
)
```

This configuration will create a structure of folders containing the dataset name and the categories: 

```bash
# main output folder
(pocket-coffea) ➜  sig_bkg_05_07_2023_v1 lrt
total 3.5K
drwxr-xr-x 3 dvalsecc ethz-higgs 512 Jul  5 15:06 TTbbSemiLeptonic_Powheg_2018
drwxr-xr-x 3 dvalsecc ethz-higgs 512 Jul  5 15:06 TTbbSemiLeptonic_Powheg_2016_PreVFP
drwxr-xr-x 3 dvalsecc ethz-higgs 512 Jul  5 15:06 TTToSemiLeptonic_2016_PreVFP
drwxr-xr-x 3 dvalsecc ethz-higgs 512 Jul  5 15:06 TTbbSemiLeptonic_Powheg_2016_PostVFP
drwxr-xr-x 3 dvalsecc ethz-higgs 512 Jul  5 15:07 TTbbSemiLeptonic_Powheg_2017
drwxr-xr-x 3 dvalsecc ethz-higgs 512 Jul  5 15:14 TTToSemiLeptonic_2016_PostVFP
drwxr-xr-x 3 dvalsecc ethz-higgs 512 Jul  5 15:20 TTToSemiLeptonic_2017

# Output by dataset
(pocket-coffea) ➜  sig_bkg_05_07_2023_v1 cd TTbbSemiLeptonic_Powheg_2018
(pocket-coffea) ➜  TTbbSemiLeptonic_Powheg_2018 lrt
# categories
drwxr-xr-x 24 dvalsecc ethz-higgs 512 Jul  5 15:12 semilep_LHE

# Chunks output
(pocket-coffea) ➜  TTbbSemiLeptonic_Powheg_2018 cd semilep_LHE 
(pocket-coffea) ➜  semilep_LHE lrt
total 219M
-rw-r--r-- 1 dvalsecc ethz-higgs 161K Jul  5 15:06 58cae696-ff9a-11eb-8bcf-b4e45d9fbeef_%2FEvents%3B1_0-6000.parquet
-rw-r--r-- 1 dvalsecc ethz-higgs 8.8M Jul  5 15:07 f90f7300-022f-11ec-8fd2-0c0013acbeef_%2FEvents%3B1_403500-807000.parquet
-rw-r--r-- 1 dvalsecc ethz-higgs 9.2M Jul  5 15:07 b788eafa-0203-11ec-9ed1-0b0013acbeef_%2FEvents%3B1_429000-858000.parquet
-rw-r--r-- 1 dvalsecc ethz-higgs 8.8M Jul  5 15:07 f90f7300-022f-11ec-8fd2-0c0013acbeef_%2FEvents%3B1_0-403500.parquet
-rw-r--r-- 1 dvalsecc ethz-higgs  11M Jul  5 15:07 df0073b2-05f2-11ec-936f-118810acbeef_%2FEvents%3B1_0-495000.parquet
-rw-r--r-- 1 dvalsecc ethz-higgs 715K Jul  5 15:07 94c2a20e-ff92-11eb-9e5b-7e969e86beef_%2FEvents%3B1_0-28681.parquet
-rw-r--r-- 1 dvalsecc ethz-higgs 9.2M Jul  5 15:07 b788eafa-0203-11ec-9ed1-0b0013acbeef_%2FEvents%3B1_0-429000.parquet
-rw-r--r-- 1 dvalsecc ethz-higgs  14M Jul  5 15:07 b379fc2e-0203-11ec-8947-030013acbeef_%2FEvents%3B1_0-639000.parquet

```
