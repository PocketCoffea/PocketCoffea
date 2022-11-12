########
Concepts
########

In PocketCoffea the processing of the NanoAOD events starts from the raw unprocessed dataset up to histograms or ntuples defined in multiple categories.

Each piece of the NanoAOD dataset, called **chunk**, is elaborated by a **Coffea processor**, which defined the operations to skim, add quantities, define categories, create histograms of ntuples.
The engine of all these operations is the **awkward** array library, which make the processor operate on the whole chunk of events all together, in a **columnar approach**.

The form of a Coffea processor is completely free: in PocketCoffea we define a `BaseProcessorABC` class, which covers more rigidly most of the steps are usually done to perform a CMS HEP analysis.

**Flexibility** and **customization** is provided in two ways:

* Workflow customization
    The user defines a custom processor, which derives from the base class `BaseProcessorABC`. In this code the user is free to define the object preselection, custom collections and custom processing steps. The base class provides a series of entrypoints for the derived processor, to modify specific part of the computation, therefore improving a lot the readibility of the custom code, and keeping a more rigid structure.

* Configuration
    The configuration of the categories, weights, systematics and histograms to plot is defined in a configuration file and not in the code. This permits a user-friendly interface if one does not need to modify the processing steps. The user provides small piece of codes (mostly python dictionaries), to customize cuts, weights and histograms. The structure of the configuration is fixed to allow users to build on top of each other setups.


Have a look at the rest of this page for a detailed description of the default processing steps and of the PocketCoffea configuration. 
    
Basic workflow
##############

The basic workflow is defined by the `BaseProcessorABC::process()` function. The processor constructor is called only once for all the processing, whereas the `process()` function is called for each different chunk (piece of a NanoAOD dataset).
 
1. Initialization

   * Load metadata
        Each chunk of a NanoAOD file is read by coffea along with its metadata, specified in the dataset configuration. This function reads the sample name, year and prepares several files and configurations for the processing: triggers, SF files, JEC files, the goldenJSON. 

   * Load metadata extra
         The use can add more metadata preparation by redefining the function `load_metadata_extra()` in the derived processor. 

   * Initialize weights
        Compute the sum of the genWeights before any preselection and initialize the configuration of the weights for the sample type of the current chunk.

        
2. Initial skimming of events
     The first step in the processing reduces the number of events on which we need to apply object preselections and correction by applying a skimming cut.

     * Triggers 
          The requests trigger are linked to the `finalstate` key in the configuration. Trigger are applied as the first step to reduce the number of events are the beginning.  The trigger mask is saved to be applied in the `skim_events` function

     * User defined extra processing
         The user can redefine the function `process_extra_before_skim()` to add additional processing before the skimming phase, for example to add variables.

     * Skim
         The function `skim_events` applied the events filters, primary vertex requirement (at least 1 good PV) and triggers. Moreover, the function applies the skimming functions requested by the user from the configuration (see Configuration chapter for more details).
         Only the events passing the skimming mask are further processed down the chain.

3. Object cleaning and preselection


   

   



Filtering
---------

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


Histogramming
-------------



Configuration
#############

Datasets
--------

Cuts and categories
-------------------

Weights
--------

Variations
----------

Histograms configuration
------------------------
