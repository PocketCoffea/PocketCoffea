# Concepts

In PocketCoffea the processing of the NanoAOD events starts from the raw unprocessed dataset up to histograms or ntuples defined in multiple categories.

Each piece of the NanoAOD dataset, called **chunk**, is elaborated by a **Coffea processor**, which defined the operations to skim, add quantities, define categories, create histograms of ntuples.
The engine of all these operations is the **awkward** array library, which make the processor operate on the whole chunk of events all together, in a **columnar approach**.

The form of a Coffea processor is completely free: in PocketCoffea we define a `BaseProcessorABC` class, which covers more rigidly most of the steps are usually done to perform a CMS HEP analysis.

**Flexibility** and **customization** is provided in two ways:

* **Workflow customization**
    The user defines a custom processor, which derives from the base class `BaseProcessorABC`. In this code the user is free to define the object preselection, custom collections and custom processing steps. The base class provides a series of entrypoints for the derived processor, to modify specific part of the computation, therefore improving a lot the readibility of the custom code, and keeping a more rigid structure.

* **Configuration**
    The configuration of the categories, weights, systematics and histograms to plot is defined in a configuration file
    and not in the code. This permits a user-friendly interface if one does not need to modify the processing steps. The
    user provides small piece of codes (mostly python dictionaries), to customize cuts, weights and histograms. The
    structure of the configuration is fixed to allow users to build on top of each other setups.  Moreover, all the
    parameters defining a CMS analyses, like working points, scale factors, calibration factors, are defined
    consistently for all the users with yaml files, as described in [Parameters](./parameters.md)

:::{tip}
Have a look at the rest of this page for a detailed description of the default processing steps and o the PocketCoffea configuration. 
:::

## Base workflow

The base workflow is defined by the `BaseProcessorABC::process()` function. The processor constructor is called only once for all the processing, whereas the `process()` function is called for each different chunk (piece of a NanoAOD dataset).
 
### Initialization

* **Load metadata**
     Each chunk of a NanoAOD file is read by coffea along with its metadata, specified in the dataset configuration. This function reads the sample name, year and prepares several files and configurations for the processing: triggers, SF files, JEC files, the goldenJSON. 

* **Load metadata extra**
      The use can add more metadata preparation by redefining the function `load_metadata_extra()` in the derived processor. 

* **Initialize weights**
     Compute the sum of the genWeights before any preselection and initialize the configuration of the weights for the sample type of the current chunk.

        
### Skimming of events

The first step in the processing reduces the number of events on which we need to apply object preselections and correction by applying a skimming cut.
* **User defined extra processing**
    The user can redefine the function `process_extra_before_skim()` and `process_extra_after_skim()` to add additional
    processing before and after the skimming phase, for example to add variables.

* **Skim**
    The function `skim_events` applied the events flags, primary vertex requirement (at least 1 good PV). Moreover, the
    function applies the skimming functions requested by the user from the configuration (see Configuration chapter for
    more details). 
    Only the events passing the skimming mask are further processed down the chain.
    
* **Exporting skimmed files**
    Skimmed NanoAODs can be exported at this stage: check the Configurations page for details. 

### Object calibration and systematic variations

After the skimming phase, the framework applies object calibrations and handles systematic variations through the **Calibrators** system:

* **Initialize Calibrators**
    The `CalibratorsManager` is created with a sequence of `Calibrator` objects. Each calibrator declares which collections it modifies and what systematic variations it provides (e.g., JEC uncertainties, electron energy scale variations).

* **Variation Loop**
    The framework automatically loops over all requested systematic variations. For each variation:
    - All calibrators in the sequence are applied to produce corrected object collections
    - The original collections are preserved for calibrators that need uncorrected inputs
    - Events are processed through the full analysis chain (preselection, categories, histograms)
    - Events are reset to original state before processing the next variation

* **Built-in Calibrators**
    PocketCoffea provides ready-to-use calibrators for common corrections:
    - **JetsCalibrator**: JEC/JER corrections with uncertainty variations
    - **METCalibrator**: MET corrections after jet calibration
    - **ElectronsScaleCalibrator**: Electron energy scale and smearing
    
See the [Calibrators](./calibrators.md) page for detailed documentation on the calibration system.

### Object cleaning and preselection

* **Objects preselection**
       The user processor **must** define the method `apply_object_preselection()`. In this function the user should clean the NanoAOD collections and create new ones with the selected objects. For example: `Electron` --> `ElectronGood`. The new collections are added to the `self.events` awkward array attribute.

* **Count objects**
       The user processor **must** define the method `count_objects()` to count the number of cleaned objects. The convention is to add to the `self.events` attribute branches called `self.events["n{collection}"]`.

* **Define variables for event preselection**
       The user can define the function `define_common_variables_before_presel()` to compute quantities needed for the event preselection. We suggest to reduce at the minimum the amount of computation in this step, since these variables are computed on all the events passing the skim: it is better to postpone heavy processing after the events preselection. 

* **Extra processing before event preselection**
       The user can define the function `process_extra_before_preselection()` to add any further processing before events preselection.

 **Event preselection**
       The preselection cut requested by the user in the configuration are applied and the events not passing the mask are removed from the rest of the processing.


### Categories, Weights and Histograms

* **Extra processing after preselection**
      After the event preselection the number of events is now reduced and the user can add additional processing in the functions `define_common_variables_after_presel()` and `process_extra_after_presel()`. Classifiers, discriminators and MVA techniques should be applied here, now that the number of events has been reduced.

* **Categories**
      The categories defined by the user in the configuration are computed at this step: each category is a set of `Cut` objects which are applied with an AND. The cut masks are not applied directly, but they are saved to be used later in the histogram filling step.

* **Weights**
     Weights are handled by the `WeightsManager` class. The configuration of the weights for each sample and category is defined in the configuration file. The `WeightsManager` stores the weights for each category and each sample type. This object is recreated for each chunk in the `define_weights()` function, defined by the base processor class.
     If the user wants to add custom weights directly in the code, instead of using the configuration, it can be done by redefining the `compute_weights_extra()` function.

* **Histograms**
     The creation of histograms is handled by the framework mainly through the configuration file. The requested histograms are created for each category and systematic variation and stored in the output accumulator for each chunk. The `HistManager` class is responsible for understanding the configuration and managing the automatic filling of the histograms. The `HistManager` object is created for each chunk in the `define_histograms()` function defined in the Base processor.

* **User histogram customization**
     The user can request the framework to add custom axes to all the histograms for a particular workflow by redefining the function `define_custom_axes_extra()` or by adding `Axis` objects in the `self.custom_axes` attribute in the processor constructor. These axes are added to all the histograms independently by the configuration file.
     E.g. a custom axes to save the dataset `era` attribute can be added only for chunks with Data inside.
     Moreover the user can directly manipolate the HistManager object before the filling by redefining the `define_histograms_extra()` function.

* **Histograms filling**
      The function `fill_histogram()` is than called to automatically fill all the requested histogram from the configuration. The used can redefine the function `fill_histograms_extra()` to handle the filling of custom histograms, or special cases not handled automatically by the `HistManager`.

## Processor output

After all this processing the base processor simply counts the events in all the categories in the function
`count_events()` and stores these metadata in the output `cutflow` and `sumw` attributes. All the output histograms and
metadata coming from each chunk are accumulated by coffea in a single output dictionary. Moreover the metadata about the
processed datasets are also included in the final output. Please refer to [Output format](#output-format) for a reference about the
output format.

* **Postprocessing**
        At the end of the processing of all the chunks the `postprocess()` function, defined in the base processor is called. This function rescale the total weights of the MC histograms to normalize them w.r.t the sum of the genweights. The `scale_genweight` factor is also saved in the output. Doing so, the overall scale of the MC histograms is always correct, also if the processor is run on a partial dataset.
        

## Filtering

We have three steps of "filtering" events in the base workflow

1) **Skim**:
      this step happens before any object correction and preselection in order to remove events at the very beginning of the processing and save CPU time.
    
      Trigger, METFilters, PV selection, goldenJson lumi mask are applied by default in this step.

      N.B,. the skim selection **must be loose** w.r.t of possible systematic variations or object corrections.

2) **Preselections**:
      a set of cuts is applied after the object corrections and object preselections.

      The preselection step is applied **after** the cleaning and preselection of physics object.  The preselection cut is defined by the user in the confiration. It should be a cut removing most of the events that are not needed in the definition of the analysis categories. By removing those events at this step, more expensive operations like MVA evaluation can happen on a smaller set of events. 

      E.g.:   JERC, lepton scales etc have been already applied before this step. A preselection on the number of jets is then applied here.


3) **Categories**:
      groups of cut functions are applied as masks in order to define categories. Events are not removed but the masks are used for the output.

      
## Cut object

In Coffea, cuts are encoded as boolean masks on the `events` awkward array. The boolean masks can be stored in a
[PackedSelector](https://coffeateam.github.io/coffea/notebooks/accumulators.html#PackedSelection) object from coffea,
which can store different masks and perform the **AND** of them efficiently.

In PocketCoffea a small layer is defined to handle as **a single object both the cutting function and its parameters**. This is
implemented in the `Cut` helper class (see [cut_definition](pocket_coffea.lib.cut_definition.Cut).

```python
def NjetsNb(events,params, **kwargs):
    mask =  ((events.njet >= params["njet"] ) &
             (events.nbjet >= params["nbjet"]))
    return mask

cut = Cut(
    name = "4j-2b",
    params = { "njet":4, "nbjet": 2},
    function = NjetsNb
)
```
    

This simple object permits the user to define a parametrized cutting function and then reuse it with different
parameters. Moreover the `Cut` object is technically implemented to be able to dump it's configuration in the most
readable way: have a look at [](#configuration-preservation).

This structure makes possible the creation of **factory methods** to build the `Cut` objects.

```python
def getNjetNb_cut(njet, nb):
    return Cut(
        name=f"{njet}jet-{nb}bjet",
        params ={"njet": njet, "nbjet": nb},
        function=NjetsNb
     )
```

PocketCoffea implements a set of **factory methods** for common cut operations: they are defined in [cut functions](pocket_coffea.lib.cut_functions).


## Analysis configuration preservation
The configuration of each analysis run is preserved by saving along the output all the necessary metadata to reproduce
it.  Once the processing is done, the output folder looks like:

```bash
(pocket-coffea) ➜  output_folder git:(main) ✗ lrt
total 146M
-rw-r--r-- 1 user group  31K Jun 19 11:34 parameters_dump.yaml
-rw-r--r-- 1 user group 126K Jun 19 11:34 config.json
-rw-r--r-- 1 user group  20M Jun 19 11:34 configurator.pkl
-rw-r--r-- 1 user group 144M Jun 19 11:52 output_all.coffea
```
 
Three files are saved along the coffea output file:
- **parameters dump**: this files contains the full [parameters](./parameters.md) set dumped in a yaml file. This file
can be reloaded for another analysis run or can be used to compose other configurations. 

- **configuration dump**: the analysis configuration contained in the `Configurator` instance is saved in two formats:
  - a *human readable* file in json format containing all the `Configurator` info, but not directly usable to rerun the
  analysis. 
  - a *machine readable* pickled file, containing the `Configurator` instance itself, that can be used directly to rerun
  the analysis. 
  
  :::{warning}
  The pickled `Configurator` instance can be used reliably to rerun the analysis **only if** the PocketCoffea and user
  provided code are at the same version used when the pickled file has been created. Using git and storing the git
  commit used for an analysis run is therefore recommended.
  :::
  
The python file defining the configuration analysis is not stored because it can be also built in a more dynamic way:
storing the dumped version of the `Configurator` object, is a much more reliable method.

## Output format
The output of the PocketCoffea processors is standardize by the
[`BaseProcessorABC`](pocket_coffea.workflows.base.BaseProcessorABC) base class. The user can always add to the
`self.output` accumulator  custom objects. 

The default output schema contains the following items:

### Cutflow

It contains the number of MC events (not weighted) passing the skimming, preselection and each category. The
schema is `category:dataset:(sub)sample:N.events.

For example (from the [#Z\rightarrow \mu\mu$ analysis example]):
```python
out["cutflow"] = {
  'initial': {
      'DATA_SingleMuon_2018_EraA': 241608232,
      'DYJetsToLL_M-50_2018': 195510810,
      'DATA_SingleMuon_2018_EraD': 513909894,
      'DATA_SingleMuon_2018_EraC': 109986009,
      'DATA_SingleMuon_2018_EraB': 119918017
      },
  'skim': {
      'DATA_SingleMuon_2018_EraA': 182721650,
      'DYJetsToLL_M-50_2018': 42180665,
      'DATA_SingleMuon_2018_EraD': 416492318,
      'DATA_SingleMuon_2018_EraC': 89702938,
      'DATA_SingleMuon_2018_EraB': 91326404
      },
  'presel': {
      'DATA_SingleMuon_2018_EraA': 10474575,
      'DYJetsToLL_M-50_2018': 24267626,
      'DATA_SingleMuon_2018_EraD': 24042954,
      'DATA_SingleMuon_2018_EraC': 5193039,
      'DATA_SingleMuon_2018_EraB': 5305372
      },
  'baseline': {
      'DATA_SingleMuon_2018_EraA': {'DATA_SingleMuon': 10474575},
      'DYJetsToLL_M-50_2018': {'DYJetsToLL': 24267626},
      'DATA_SingleMuon_2018_EraD': {'DATA_SingleMuon': 24042954},
      'DATA_SingleMuon_2018_EraC': {'DATA_SingleMuon': 5193039},
      'DATA_SingleMuon_2018_EraB': {'DATA_SingleMuon': 5305372}
      }
  }
```
  
If a dataset is split in more subsamples (by configuration), the output shows the number of events in each of the
  subsample
  
```python
out["cutflow"] = {
    ......
   'baseline': {
       'DATA_SingleEle_2018_EraD': {'DATA_SingleEle': 2901},
       'DATA_SingleEle_2018_EraC': {'DATA_SingleEle': 3252},
       'DATA_SingleEle_2018_EraB': {'DATA_SingleEle': 2763},
       'DATA_SingleEle_2018_EraA': {'DATA_SingleEle': 2385},
       'DATA_SingleEle_2017_EraF': {'DATA_SingleEle': 7162},
       'DATA_SingleEle_2017_EraE': {'DATA_SingleEle': 6681},
       'DATA_SingleEle_2017_EraD': {'DATA_SingleEle': 6003},
       'DATA_SingleEle_2017_EraC': {'DATA_SingleEle': 4961},
       'DATA_SingleEle_2017_EraB': {'DATA_SingleEle': 4709},

       'TTToSemiLeptonic_2017': {
           'TTToSemiLeptonic': 253530,
           'TTToSemiLeptonic__=1b': 78524,
           'TTToSemiLeptonic__=2b': 133595,
           'TTToSemiLeptonic__>2b': 29690},
       'TTToSemiLeptonic_2018': {
           'TTToSemiLeptonic': 252951,
           'TTToSemiLeptonic__=1b': 76406,
           'TTToSemiLeptonic__=2b': 133971,
           'TTToSemiLeptonic__>2b': 31414}},

 '1b': {
     'DATA_SingleEle_2018_EraD': {'DATA_SingleEle': 901},
     'DATA_SingleEle_2018_EraC': {'DATA_SingleEle': 984},
     'DATA_SingleEle_2018_EraB': {'DATA_SingleEle': 879},
     'DATA_SingleEle_2018_EraA': {'DATA_SingleEle': 762},
     'DATA_SingleEle_2017_EraF': {'DATA_SingleEle': 2185},
     'DATA_SingleEle_2017_EraE': {'DATA_SingleEle': 2063},
     'DATA_SingleEle_2017_EraD': {'DATA_SingleEle': 1881},
     'DATA_SingleEle_2017_EraC': {'DATA_SingleEle': 1530},
     'DATA_SingleEle_2017_EraB': {'DATA_SingleEle': 1457},

     'TTToSemiLeptonic_2017': {
         'TTToSemiLeptonic': 97865,
         'TTToSemiLeptonic__=1b': 72355,
         'TTToSemiLeptonic__=2b': 23417,
         'TTToSemiLeptonic__>2b': 2093},
     'TTToSemiLeptonic_2018': {
         'TTToSemiLeptonic': 96522,
         'TTToSemiLeptonic__=1b': 70336,
         'TTToSemiLeptonic__=2b': 23890,
         'TTToSemiLeptonic__>2b': 2296}}
 }

```

### Sum of weights

The output key `sumw` contains the total number of weighted MC events in each category of the analysis for each dataset and
(sub)sample. The two levels are necessary since each dataset may be split in multiple subsamples.
  
```python
out["sumw"] = {'baseline': { 'DYJetsToLL_M-50_2018': {'DYJetsToLL': 44614145.4945453}}}                                
```

### Total sum of genweights

The key `sum_genweights` contians the total sum of the generator weights for each dataset. This is used in the base processor
`postprocessing()` function to rescale automatically all the histograms and `sumw`. It is also kept in the output for
later uses.
  
```python
out["sum_genweights"] =  {'DYJetsToLL_M-50_2018': 3323477400000.0}
```


### Variables

Dictionary containing all the histogram objects create by the analysis run. 
The structure is:  histogram name --> (sub)sample --> dataset name --> Hist object.  
This kind of structure is needed because multiple dataset can have the same (sub)sample type, for example when the
analysis is run on multiple analysis periods. The histograms output configuration is documented [here](./configuration.md#histograms-configuration).

:::{note}
The categories and variations are   part of the `Hist` axes, whereas the data taking period is not explicitely
saved. This is because each dataset is uniquely assiociated to a single data taking period. The information can be
reconstructed looking at the `dataset_metadata` output. 
:::

```python 
  >>> out["variables"].keys()
  dict_keys(['MuonGood_eta_1', 'MuonGood_pt_1', 'MuonGood_phi_1', 'nElectronGood', 'nMuonGood', 'nJets', 'nBJets', 'JetGood_eta_1', 'JetGood_pt_1', 'JetGood_phi_1', 'JetGood_btagDeepFlavB_1', 'JetGood_eta_2', 'JetGood_pt_2', 'JetGood_phi_2', 'JetGood_btagDeepFlavB_2', 'mll'])

  >>> out["variables"]["mll"].keys()
  dict_keys(['DATA_SingleMuon', 'DYJetsToLL'])

  >>> out["variables"]["mll"]["DATA_SingleMuon"]
  {'DATA_SingleMuon_2018_EraA':
      Hist(
          StrCategory(['baseline'], name='cat', label='Category'),
          Regular(100, 0, 200, name='ll.mass', label='$M_{\\ell\\ell}$ [GeV]'),
          storage=Weight()) # Sum: WeightedSum(value=1.04425e+07, variance=1.04425e+07) (WeightedSum(value=1.04746e+07, variance=1.04746e+07) with flow),
   'DATA_SingleMuon_2018_EraD': 
      Hist(
          StrCategory(['baseline'], name='cat', label='Category'),
          Regular(100, 0, 200, name='ll.mass', label='$M_{\\ell\\ell}$ [GeV]'),
          storage=Weight()) # Sum: WeightedSum(value=2.39693e+07, variance=2.39693e+07) (WeightedSum(value=2.4043e+07, variance=2.4043e+07) with flow),
   'DATA_SingleMuon_2018_EraC':
      Hist(
          StrCategory(['baseline'], name='cat', label='Category'),
          Regular(100, 0, 200, name='ll.mass', label='$M_{\\ell\\ell}$ [GeV]'),
          storage=Weight()) # Sum: WeightedSum(value=5.17717e+06, variance=5.17717e+06) (WeightedSum(value=5.19304e+06, variance=5.19304e+06) with flow),
   'DATA_SingleMuon_2018_EraB': 
      Hist(
          StrCategory(['baseline'], name='cat', label='Category'),
          Regular(100, 0, 200, name='ll.mass', label='$M_{\\ell\\ell}$ [GeV]'),
          storage=Weight()) # Sum: WeightedSum(value=5.28916e+06, variance=5.28916e+06)
          (WeightedSum(value=5.30537e+06, variance=5.30537e+06) with flow)
  }
 ```


### Columns
The columns configuration is similar to the variables one. The structure is: (sub)sample name --> dataset
name  --> category --> column output name.  The grouping is on the sample and not on the variables given the fact the usually the
column outputs are more frequently used separated by samples. 

```python
>>> out["columns"].keys()
dict_keys(['TTToSemiLeptonic__=1b', 'TTToSemiLeptonic__=2b', 'TTToSemiLeptonic__>2b'])

>>> out["columns"]["TTToSemiLeptonic__=1b"].keys()
dict_keys(['TTToSemiLeptonic_2018', 'TTToSemiLeptonic_2017'])

>>> out["columns"]["TTToSemiLeptonic__=1b"]["TTToSemiLeptonic_2018"].keys()
dict_keys(['baseline', '1b', '2b', '3b', '4b'])

>>> out["columns"]["TTToSemiLeptonic__=1b"]["TTToSemiLeptonic_2018"]["baseline"].keys()
dict_keys(['LeptonGood_N', 'LeptonGood_pt', 'LeptonGood_eta', 'LeptonGood_phi', 'JetGood_N', 'JetGood_pt', 'JetGood_eta', 'JetGood_phi'])

>>> out["columns"]["TTToSemiLeptonic__=1b"]["TTToSemiLeptonic_2018"]["baseline"]["JetGood_pt"]
column_accumulator(array([ 73.79437 ,  60.94935 ,  59.455273, ..., 113.379875,  58.940334,
      57.60932 ], dtype=float32))

```


### Datasets metadata
The dataset metadata is saved in the output to be able to reference the provenance of the
events used to fill the output histograms  and columns in case the analysis run parameters are lost. 
It contains the map (sub)sample <--> datasets, split by data taking period, but also the full dataset list info.

In the following example, note that the **dataset** `TTToSemiLeptonic_2018` is split in 3 different **subsamples**, whereas the
`DATA_SingleEle` sample refers to 5 different **datasets** corresponding to different eras.

:::{warning}
For an in-depth explanation of the differences between **datasets** and **(sub)samples** in the framework, have a look
at the [Datasets handling](./datasets.md) page.
:::
```python 
>>> out["datasets_metadata"]
{'by_datataking_period': {
    '2018': defaultdict(set,
            {'TTToSemiLeptonic__=1b': {'TTToSemiLeptonic_2018'},
             'TTToSemiLeptonic__=2b': {'TTToSemiLeptonic_2018'},
             'TTToSemiLeptonic__>2b': {'TTToSemiLeptonic_2018'},
             'DATA_SingleEle': {'DATA_SingleEle_2018_EraA',
                                'DATA_SingleEle_2018_EraB',
                                'DATA_SingleEle_2018_EraC',
                                'DATA_SingleEle_2018_EraD'}}),
     '2017': defaultdict(set,
            {'TTToSemiLeptonic__=1b': {'TTToSemiLeptonic_2017'},
             'TTToSemiLeptonic__=2b': {'TTToSemiLeptonic_2017'},
             'TTToSemiLeptonic__>2b': {'TTToSemiLeptonic_2017'},
             'DATA_SingleEle': {'DATA_SingleEle_2017_EraB',
                                'DATA_SingleEle_2017_EraC',
                                'DATA_SingleEle_2017_EraD',
                                'DATA_SingleEle_2017_EraE',
                                'DATA_SingleEle_2017_EraF'}})},
'by_dataset': defaultdict(dict,
           {'TTToSemiLeptonic_2018': {
               'das_names': "['/TTToSemiLeptonic_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM']",
               'sample': 'TTToSemiLeptonic',
               'year': '2018',
               'isMC': 'True',
               'xsec': '365.4574',
               'sum_genweights': '143354134528.0',
               'nevents': '476408000',
               'size': '1030792999916'},
            'TTToSemiLeptonic_2017': {
                'das_names': "['/TTToSemiLeptonic_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL17NanoAODv9-106X_mc2017_realistic_v9-v1/NANOAODSIM']",
                'sample': 'TTToSemiLeptonic',
                'year': '2017',
                'isMC': 'True',
                'xsec': '365.4574',
                'sum_genweights': '104129945600.0',
                'nevents': '346052000',
                'size': '766341543050'},
            'DATA_SingleEle_2017_EraB': {
                'das_names': "['/SingleElectron/Run2017B-UL2017_MiniAODv2_NanoAODv9-v1/NANOAOD']",
                'sample': 'DATA_SingleEle',
                'year': '2017',
                'isMC': 'False',
                'primaryDataset': 'SingleEle',
                'era': 'B',
                'nevents': '60537490',
                'size': '50665471331'},
           })
      }
```



