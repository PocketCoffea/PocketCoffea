Full analysis example
########################

A full example of all the steps needed to run a full analysis with PocketCoffea is reported, starting from the creation of the dataset, the customization of parameters and the production of the final shapes and plots.
As an example, a simplified version of the Drell-Yan analysis targeting the Z->mumu channel is implemented.
The main steps that need to be performed are the following:

* Build the json datasets
* Compute the sum of genweights for Monte Carlo datasets and rebuild json datasets
* Define selections: trigger, skim, object preselection, event preselection and categorization
* Define weights and variations
* Define histograms
* Run the processor
* Make the plots from processor output

Configuration file
================

The parameters specific to the analysis have to be specified in the configuration file. This file contains a pure python dictionary named `cfg` that is read by the `Configurator` module.

Build dataset
================

The datasets include a Drell-Yan Monte Carlo dataset and a `SingleMuon` dataset. We have to look for the DAS key of the 
The list of datasets has to be written in a structured dictionary together with the corresponding metadata in a json file. 
Follow the instructions in the Build dataset example.

Compute the sum of genweights
================

The sum of the genweights of Monte Carlo samples needs to be computed in order to normalize Monte Carlo datasets
To compute the sum of genweights, we need to run a dedicated Coffea processor, `genWeightsProcessor`, that just opens all the files, reads the genweight

Define selections
================

The selections are performed at two levels:
* Object preselection: selecting the "good" objects that will be used in the final analysis (e.g. `JetGood`, `MuonGood`, `ElectronGood`...). These selections include the detector acceptance cuts, the object identification working points, the muon isolation, the b-tagging working point, etc.
* Event selection: selections on the events that enter the final analysis, done in three steps:
   * Skim: loose cut on the events. The following steps of the analysis are performed only on the events passing the skim selection.
   * Preselection: baseline selection for the analysis.
   * Categorization: selection to split the events passing the event preselection into different categories.


Define weights and variations
================


Define histograms
================


Run the processor
================


Produce plots
================
