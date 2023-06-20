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
Have a look at the rest of this page for a detailed description of the default processing steps and of the PocketCoffea configuration. 
:::

## Base workflow

The base workflow is defined by the `BaseProcessorABC::process()` function. The processor constructor is called only once for all the processing, whereas the `process()` function is called for each different chunk (piece of a NanoAOD dataset).
 
### Initialization

* Load metadata
     Each chunk of a NanoAOD file is read by coffea along with its metadata, specified in the dataset configuration. This function reads the sample name, year and prepares several files and configurations for the processing: triggers, SF files, JEC files, the goldenJSON. 

* Load metadata extra
      The use can add more metadata preparation by redefining the function `load_metadata_extra()` in the derived processor. 

* Initialize weights
     Compute the sum of the genWeights before any preselection and initialize the configuration of the weights for the sample type of the current chunk.

        
### Skimming of events

The first step in the processing reduces the number of events on which we need to apply object preselections and correction by applying a skimming cut.
* User defined extra processing
    The user can redefine the function `process_extra_before_skim()` and `process_extra_after_skim()` to add additional
    processing before and after the skimming phase, for example to add variables.

* Skim
    The function `skim_events` applied the events flags, primary vertex requirement (at least 1 good PV). Moreover, the
    function applies the skimming functions requested by the user from the configuration (see Configuration chapter for
    more details). 
    Only the events passing the skimming mask are further processed down the chain.
    
* Exporting skimmed files
    Skimmed NanoAODs can be exported at this stage: check the Configurations page for details. 

### Object cleaning and preselection

* Objects preselection
       The user processor **must** define the method `apply_object_preselection()`. In this function the user should clean the NanoAOD collections and create new ones with the selected objects. For example: `Electron` --> `ElectronGood`. The new collections are added to the `self.events` awkward array attribute.

* Count objects
       The user processor **must** define the method `count_objects()` to count the number of cleaned objects. The convention is to add to the `self.events` attribute branches called `self.events["n{collection}"]`.

* Define variables for event preselection
       The user can define the function `define_common_variables_before_presel()` to compute quantities needed for the event preselection. We suggest to reduce at the minimum the amount of computation in this step, since these variables are computed on all the events passing the skim: it is better to postpone heavy processing after the events preselection. 

* Extra processing before event preselection
       The user can define the function `process_extra_before_preselection()` to add any further processing before events preselection.

* Event preselection
       The preselection cut requested by the user in the configuration are applied and the events not passing the mask are removed from the rest of the processing.


### Categories, Weights and Histograms

* Extra processing after preselection
      After the event preselection the number of events is now reduced and the user can add additional processing in the functions `define_common_variables_after_presel()` and `process_extra_after_presel()`. Classifiers, discriminators and MVA techniques should be applied here, now that the number of events has been reduced.

* Categories
      The categories defined by the user in the configuration are computed at this step: each category is a set of `Cut` objects which are applied with an AND. The cut masks are not applied directly, but they are saved to be used later in the histogram filling step.

* Weights
     Weights are handled by the `WeightsManager` class. The configuration of the weights for each sample and category is defined in the configuration file. The `WeightsManager` stores the weights for each category and each sample type. This object is recreated for each chunk in the `define_weights()` function, defined by the base processor class.
     If the user wants to add custom weights directly in the code, instead of using the configuration, it can be done by redefining the `compute_weights_extra()` function.

* Histograms
     The creation of histograms is handled by the framework mainly through the configuration file. The requested histograms are created for each category and systematic variation and stored in the output accumulator for each chunk. The `HistManager` class is responsible for understanding the configuration and managing the automatic filling of the histograms. The `HistManager` object is created for each chunk in the `define_histograms()` function defined in the Base processor.

* User histogram customization
     The user can request the framework to add custom axes to all the histograms for a particular workflow by redefining the function `define_custom_axes_extra()` or by adding `Axis` objects in the `self.custom_axes` attribute in the processor constructor. These axes are added to all the histograms independently by the configuration file.
     E.g. a custom axes to save the dataset `era` attribute can be added only for chunks with Data inside.
     Moreover the user can directly manipolate the HistManager object before the filling by redefining the `define_histograms_extra()` function.

* Histograms filling
      The function `fill_histogram()` is than called to automatically fill all the requested histogram from the configuration. The used can redefine the function `fill_histograms_extra()` to handle the filling of custom histograms, or special cases not handled automatically by the `HistManager`.

## Processor output

After all this processing the base processor simply counts the events in all the categories in the function
`count_events()` and stores these metadata in the output `cutflow` and `sumw` attributes. All the output histograms and
metadata coming from each chunk are accumulated by coffea in a single output dictionary. Moreover the metadata about the
processed datasets are also included in the final output. Please refer to [Output format](#output-format) for a reference about the
output format.

* Postprocessing
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


## Configuration preservation


## Output format
The output of the PocketCoffea processors is standardize by the
[`BaseProcessorABC`](pocket_coffea.workflow.base.BaseProcessorABC.output_format) base class. The user can always add to the
`self.output` accumulator  custom objects. 

The default output schema contains the following items:

- **cutflow**: contains the number of MC events (not weighted) passing the skimming, preselection and each category. The
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

- **sumw**: contains the total number of weighted MC events in each category of the analysis for each dataset and
  (sub)sample. The two levels are necessary since each dataset may be split in multiple subsamples.
  
  ```python
  out["sumw"] = {'baseline': { 'DYJetsToLL_M-50_2018': {'DYJetsToLL': 44614145.4945453}}}                                
  ```

- **sum_genweights**: Total sum of the generator weights for each dataset. This is used in the base processor
  `postprocessing()` function to rescale automatically all the histograms and `sumw`. It is also kept in the output for
  later uses.
  
  ```python
  out["sum_genweights"] =  {'DYJetsToLL_M-50_2018': 3323477400000.0}
  ```

- **variables**

- **columns**

- **datasets_metadata**


