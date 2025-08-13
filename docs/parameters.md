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

:::{important}
The `parameters` object is passed to the `Configurator` class (see [Configuration](./configuration.md)), which passes it
inside the Coffea processor and to all the components of the framework. Therefore, the parameters object is the ideal
container for all the the necessary metadata that are not part of the analysis configuration. 
:::

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

::::{tip}
PocketCoffea defines a set of default parameters sets for the most common CMS parameters: lumi, jets calibrations, event
flags, btagging working points. 
::::

The file contains a nested structure splitting the parameters by datataking period. Internally the jet scale factor application
code in PocketCoffea will look for the `jet_scale_factors.btagSF` metadata when applying the btaggging scale factors. 

:::{important}
The parameters format is free and not validated inside the framework. Internally some part of the code expect the
parameter dictionaries to have certain key as `jet_scale_factors`. In case one key is not found the process terminated
with a nice exception telling the user what's missing in the yaml files. 
:::



## Default parameters
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
from pocket_coffea.parameters import defaults
default.compose_parameters_from_files(["params/triggers.yaml", "params/leptons.yaml"])

## Using directly the OmegaConf interface
pileup = OmegaConf.load('pileup.yaml')
event_flags = OmegaConf.load('event_flags.yaml')
lumi = OmegaConf.load('lumi.yaml')
params = OmegaConf.merge(pileup, event_flags, lumi)
```

## User parameters customization

User must be able to easily and cleanly modify analysis parameters building up from shared configuration sets. 
The most direct way to modify parameters is just to load the defaults and manually set attributes in the analysis
configuration file or in a script. 

```python
>>> from pocket_coffea.parameters import defaults
>>> default_parameters = defaults.get_default_parameters()
# Now the user can customize the params as a dictionary
>>> a["custom_param"] = {"2018": 3.45, "2017": [2,3,4,5]}
>>> a.custom_param
{'2018': 3.45, '2017': [2, 3, 4, 5]}
```

A best practice is to save parameters customization in yaml files along the analyses configuration. 
Some methods have been implemented in the `pocket_coffea.parameters.defaults` module to help the user compose the
configuration. 

```python
from pocket_coffea.parameters import defaults
default_parameters = defaults.get_default_parameters()

parameters = defaults.merge_parameters_from_files(default_parameters,
                                                  f"{localdir}/params/object_preselection.yaml",
                                                  f"{localdir}/params/triggers.yaml",
                                                  update=True)
```

The method `defaults.merge_parameters_from_files` loads the additional parameters from the yaml files passed by the user
and merge them with the `default_parameters` object. The `update=True` options means that if a key is already present,
the dictionary is updated and not just replaced (default from OmegaConf).


:::{tip}
The parameters used in each analysis run are dumped together with the analysis configuration in order to always track
all the metadata used to produce plots and ntuples.
:::

## OmegaConf tip and tricks

The OmegaConf library allows some additional dynamic behaviour in the definition of the yaml file which can be quite
useful. 

### Custom resolvers

OmegaConf permits the user to define `resolvers` which get their value resolved during execution. 
For example the location of the default parameters directory depends on the user setup. It can be defined as a 
`${default_params_dir:}` macro.

```yaml
lumi: 
  goldenJSON:
    2016_PreVFP: "${default_params_dir:}/datacert/Cert_271036-284044_13TeV_Legacy2016_Collisions16_JSON.txt"
    2016_PostVFP: "${default_params_dir:}/datacert/Cert_271036-284044_13TeV_Legacy2016_Collisions16_JSON.txt"
    '2017': "${default_params_dir:}/datacert/Cert_294927-306462_13TeV_UL2017_Collisions17_GoldenJSON.txt"
    '2018': "${default_params_dir:}/datacert/Cert_314472-325175_13TeV_Legacy2018_Collisions18_JSON.txt"


```

The macro is defined by default thanks to an helper function in `pocket_coffea.parameters.defaults`.
The user can define additional macros using the helper before loading the parameters files. For example: 

```python
from pocket_coffea.parameters import defaults
default_parameters = defaults.get_default_parameters()
localdir = os.path.dirname(os.path.abspath(__file__))

# Register  a new macro
defaults.register_configuration_dir("config_dir", localdir+"/params")
```

:::{important}
Now the loaded parameters can use the `"${config_dir:}"` macro without contaminating the parameters files with
user-specific hard coded paths. This is very important to be able to share configurations between users without painful
hardcoded changes.
:::

### Cross references

OmegaConf can also build cross references inside the configuration dictionary. 
Existing keys can be referred to just by using the syntax `${other.key.in.the.dictionary}`. **N.B.**: note the missing
semicolumn at the end of the macro syntax, which is reserved for `resolvers` like `${default_dir:}`.

For example, the jet calibration configuration can be built using pieces of the `default_jets_calibration` dictionary 
defined in
[`pocket_coffea/parameters/jets_calibration.yaml`](https://github.com/PocketCoffea/PocketCoffea/blob/main/pocket_coffea/parameters/jets_calibration.yaml). This
helps removing a lot of repetition and boilerplate metadata.

```yaml
# Default jets calibration for the user used by the processor
jets_calibration:
  factory_file: "./jets_calibrator_JES_JER_Syst.pkl.gz"
  jet_types:
    AK4PFchs: "${default_jets_calibration.factory_configuration.AK4PFchs.JES_JER_Syst}" 
    AK8PFPuppi: "${default_jets_calibration.factory_configuration.AK8PFPuppi.JES_JER_Syst}"
  collection:  # this is needed to know which collection is corrected with which jet factory
    AK4PFchs: "Jet"
    AK8PFPuppi: "FatJet"
  jec_name_map: "${default_jets_calibration.jec_name_map}"

```


### Missing values
Missing values which need to be defined can be included with a `???` string: see
[docs](https://omegaconf.readthedocs.io/en/latest/usage.html#id20). If a user runs trying to use these value, an
exception will be raised. 

## Local CVMFS Files Management

PocketCoffea parameters often reference files stored on CVMFS (CernVM File System), such as scale factors, calibrations, and other physics object corrections. While CVMFS provides reliable access to these files on computing centers, there are scenarios where you might need to work offline or ensure consistent file versions across different environments.

### CVMFS File Download Tool

PocketCoffea provides a built-in tool to download and manage CVMFS files locally with versioning support:

```bash
# Download CVMFS files referenced in parameter files
pocket-coffea download-cvmfs-files --tag v1.0 --parameter-files "params/*.yaml"

# Dry run to see what would be downloaded
pocket-coffea download-cvmfs-files --tag v1.0 --dry-run

# Use custom output directory
pocket-coffea download-cvmfs-files --tag v1.0 --output-dir /path/to/storage --parameter-files "params/*.yaml"

# List available versions
pocket-coffea download-cvmfs-files --list-versions
```

### How It Works

The tool automatically scans your parameter files for CVMFS references and supports two formats:

1. **Direct CVMFS paths**: `/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/BTV/2018_UL/btagging.json.gz`
2. **Resolver syntax** (recommended): `${cvmfs:/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/BTV/2018_UL/btagging.json.gz}`

### Features

- **Versioned Storage**: Files are organized by tags/versions in separate directories
- **Hard Link Deduplication**: Unchanged files between versions are hard-linked to save disk space
- **Comprehensive Metadata**: Each version includes metadata about download status, checksums, and changes
- **Parallel Downloads**: Configurable concurrent downloads for faster processing
- **Integrity Verification**: SHA256 checksums ensure file integrity

### Directory Structure

The tool creates a structured directory layout:
```
current_cvmfs_files/
├── v1.0/
│   ├── cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/BTV/2018_UL/btagging.json.gz
│   ├── cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/JME/2018_UL/jmar.json.gz
│   └── metadata.json
├── v1.1/
│   ├── cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/BTV/2018_UL/btagging.json.gz (hard link if unchanged)
│   ├── cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/JME/2018_UL/jmar.json.gz (new version)
│   └── metadata.json
└── versions_index.json
```

### Using Local Files with CVMFS Resolver

The dumping of cvmfs files is handled by PocketCoffea developers. 
Newer tags are pushed to the repository periodically and the community is notified about the differences. Moreover the metadata files saved alongside the dumps stores a complete history of the files. 

A default tag is stored in the PocketCoffea code, and the usage of the dumped cvmfs files is activated by default, to avoid any unexpected change. 

However, **it is always better to refer to a specific tag** when loading the default parameters, in order to avoid any surprise change from the update of PocketCoffea default version. 

```python
from pocket_coffea.parameters import defaults

# Select the default latest tag setup by PocketCoffea
default_parameters = defaults.get_default_parameters(use_cvmfs_dump=True)
# Use a specific tag
default_parameters = defaults.get_default_parameters(use_cvmfs_dump=True, tag="YYYY-MM-DD")

parameters = defaults.merge_parameters_from_files(
    default_parameters,
    "params/object_preselection.yaml"
)
```

