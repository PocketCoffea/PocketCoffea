# Changelog

This page keeps track of major and minor changes between versions. Breaking changes are also reported

## PocketCoffea 0.9.6

- Minor release: only fixes and small improvements
  - Some fixes in the plotting script
  - Improvements for the INFN analysis facility executor

## PocketCoffea 0.9.5

- Generalize handling of common Weights and user-defined Weights
- Added first integration of LAW tasks to stear an analysis running PocketCoffea workflows
- Many improvements in the plotting scripts
- Added Dataset discovery cli to dynamically query for dataset and build the dataset definition file
- Cleaning up of the default skimming function (**Breaking changes!** see below)
- Added CDCI tests of utils and full configuration tests
- New parameters exploration CLI 
- Added more executors
- Tested the Swan AF and INFN AF


#### Breaking changes

##### Default skim
- Some skimming cuts were included by default in the base workflow and may be unnoticed by the users
[PR#193](https://github.com/PocketCoffea/PocketCoffea/pull/193). For maximum transparency we have removed those cuts
from the base workflow and made the cutting functions available to be used in the configuration.  The functions were: 

  - nPV_good > 0 selection
  - goldenJson
  - event flags for data and MC. 

Users **must now include** the following cut functions in their `skim` configuration to keep the same cutflow in their
analysis:
```python
from pocket_coffea.lib.cut_functions import get_nPVgood, goldenJson, eventFlags

cfg = Configurator(
    skim = [get_nPVgood(1), eventFlags, goldenJson]
)
```

This change is enough to preserve the same cuts applied before 0.9.5.

##### Jet selection function
The signature of the jet cleaning function used often in the object preselection step of processor has been changes to
add explicitely the year argument. 

```python
- def jet_selection(events, jet_type, params, leptons_collection=""):
+ def jet_selection(events, jet_type, params, *year*, leptons_collection=""):
```

This signature change can be unnoticed by users using the `leptons_collection` argument. Please cross-check your
function usage.


## PocketCoffea 0.9.0

-  New executor plugin setup to support multiple sites and analysis facilities
-  New defaults for Run3 corrections
-  Improve CLI interface and unified scripts under pocket-coffea command
-  Added dataset-discovery-cli to build dataset_definition files


## PocketCoffea 0.7

- Improved and generalized executors configuration for runner.py

## PocketCoffea 1.0rc0
The main change is the possibility to completely split the analysis configuration files and parameters from the core of
the framework.

- Parameters configuration now in place with OmegaConf
- Docker and singularity images built with GitLab CD/CI and published on
  `/cvmfs/unpacked.cern.ch/gitlab-registry.cern.ch/cms-analysis/general`
- Improved the handling of datasets and samples
- Added dataset metadata in the output file

