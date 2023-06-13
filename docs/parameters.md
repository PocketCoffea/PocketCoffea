# Parameters

A CMS analysis in PocketCoffea is fully determined by a set of **parameters** and by an analysis **configuration**. 
These two terms, althought they are both referring to metadata,  have a specific meaning inside the framework.

In PocketCoffea **parameters** are all the metadata defining a CMS analysis phasespace in a broader sense:

- Triggers
- Luminosity and event flags
- Object identification, calibration configuration and working points
- Scale factors 
- Jet energy calibration configuration

:::{note}
Analyzers use **parameters** to define a calibrated set of objects, with clearly defined object preselection and a set
of CMS-specific working points. 
:::

On top of this, different **analysis configurations** can be defined to export a set of observables, in specific
categories and produce plots, ntuples, measurements. 

::::{admonition} Goal
:class: important
The goal of PocketCoffea is to track both analyses parameters and configurations to streamline the sharing of common
metadata between groups and make analysis preservation easier.
::::

The **configuration** format is described in details [here](./configuration.md). In this page we discuss the format of
analyses **parameters**. 

## Parameters format
The choosen format for analysis parameters in PocketCoffea is yaml, given is high readability and flexibility. Most of
the CMS metadata can be expressed as list and dictionaries of strings and numbers, therefore the yaml format does not
pose any limitation. 

The **OmegaConf** ([docs](https://omegaconf.readthedocs.io/en/latest/index.html)) package has been chosen to handle the yaml parameters file: this allow us to compose different parameter sets and/or being able to dynamically overwrite part of a configuration.

::::{tip}
PocketCoffea defines a set of default parameters sets for the most common CMS parameters: lumi, jets calibrations, event
flags, btagging working points. 
::::

Let's have a look at one of the default parameters set defined in the PocketCoffea defaults in
[`pocket_coffea/parameters/jet_scale_factors.yaml`](https://github.com/PocketCoffea/PocketCoffea/blob/main/pocket_coffea/parameters/jet_scale_factors.yaml)

```yaml
jet_scale_factors:
  btagSF:
    # DeepJet AK4 tagger shape SF
    '2016_PreVFP':
      file: /cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/BTV/2016preVFP_UL/btagging.json.gz
      name: "deepJet_shape"
    '2016_PostVFP':
      file: /cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/BTV/2016postVFP_UL/btagging.json.gz
      name: "deepJet_shape"
    '2017':
      file: /cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/BTV/2017_UL/btagging.json.gz
      name: "deepJet_shape"
    '2018':
      file: /cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/BTV/2018_UL/btagging.json.gz
      name: "deepJet_shape"

  jet_puId:
      # Jet PU ID SF to be applied only on selected jets (pt<50) that are matched to GenJets
      '2016_PreVFP':
        file: /cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/JME/2016preVFP_UL/jmar.json.gz
        name: PUJetID_eff
      '2016_PostVFP':
        file: /cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/JME/2016postVFP_UL/jmar.json.gz
        name: PUJetID_eff
      '2017':
        file: /cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/JME/2017_UL/jmar.json.gz
        name: PUJetID_eff
      '2018':
        file: /cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/JME/2018_UL/jmar.json.gz
        name: PUJetID_eff

```

The file contains a nested structure splitting the parameters by datataking period. Internally the jet scale factor application
code in PocketCoffea will look for the `jet_scale_factors.btagSF` metadata when applying the btaggging scale factors. 

:::{important}
The parameters format is free and not validated inside the framework. Internally some part of the code expect the
parameter dictionaries to have certain key as `jet_scale_factors`. In case one key is not found the process terminated
with a nice exception telling the user what's missing in the yaml files. 
:::



### Default parameters
PocketCoffea defines a set of default parameters sets for the most common CMS parameters: lumi, jets calibrations, event
flags, btagging working points. The user can get a copy of the default set of parameters programmatically: 

```python
>>> from pocket_coffea.parameters import defaults
>>> default_parameters = defaults.get_default_parameters()

>>> default_parameters.keys()
dict_keys(['pileupJSONfiles', 'event_flags', 'event_flags_data',
           'lumi', 'default_jets_calibration', 'jets_calibration', 
           'jet_scale_factors', 'btagging', 'lepton_scale_factors',
           'systematic_variations'])

>>> default_parameters.jet_scale_factors.btagSF
{'btagSF': {
    '2016_PreVFP': {'file': '/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/BTV/2016preVFP_UL/btagging.json.gz', 'name': 'deepJet_shape'},
    '2016_PostVFP': {'file': '/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/BTV/2016postVFP_UL/btagging.json.gz', 'name': 'deepJet_shape'}, 
    '2017': {'file': '/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/BTV/2017_UL/btagging.json.gz', 'name': 'deepJet_shape'}, 
    '2018': {'file': '/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/BTV/2018_UL/btagging.json.gz',
    'name': 'deepJet_shape'}},
}

```

The `OmegaConf` parameters object behaves like a python dictionary, where keys can be accessed directly as attributes. 
The user can explore programmatically the full parameters set and dinamycally add more keys. 

Parameters set can also be loaded directly from yaml files:

```python

### Using the PocketCoffea interface
>>> from pocket_coffea.parameters import defaults
>>> default.compose_parameters_from_files(["params/triggers.yaml", "params/leptons.yaml"])

## Using directly the OmegaConf interface
>>> pileup = OmegaConf.load('pileup.yaml')
>>> event_flags = OmegaConf.load('event_flags.yaml')
>>> lumi = OmegaConf.load('lumi.yaml')
>>> params = OmegaConf.merge(pileup, event_flags, lumi)
```

### User parameters customization

User must be able to easily and cleanly modify analysis parameters building up from shared configuration sets 






```python

defaults.register_configuration_dir("config_dir", localdir+"/params")

parameters = defaults.merge_parameters_from_files(default_parameters,
                                                  f"{localdir}/params/object_preselection.yaml",
                                                  f"{localdir}/params/triggers.yaml",
                                                  update=True)

```
