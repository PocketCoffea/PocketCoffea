PocketCoffea by examples
########################

Dataset creation
================


Datasets are collection of samples, their corresponding files, and their metadata. 
Datasets are defined in JSON files following this syntax.

.. code-block:: json

   "ttHTobb_you_specific_version": {
        "sample": "ttHTobb",
        "json_output" : "datasets/signal_ttHTobb.json",
        "storage_prefix": "/pnfs/psi.ch/cms/trivcat/",
        "files": [
            { "das_names": ["/ttHTobb_M125_TuneCP5_13TeV-powheg-pythia8/RunIISummer.....-v2/NANOAODSIM"],
              "metadata":{
                  "year": "2018",
                  "isMC": true
              }
            },
            { "das_names" : [...],
              "metadata": {
                  "year": "2017"
                  "isMC": true
              }
           },
           ...
        ]
    }


The framework uses this definition to build the actual dataset json file. 
    
* The name of the dictionary makes the dataset unique.
* The `sample` key is the one used internally in the framework to identify the sample type.
* The `json_output` defines the output location for the list of files.
* The `storage_prefix` defines the local position of the files on the cluster (if any). 

The same dataset can contain different group of dataset (**DAS**) names, each  with a **separate metadata
dictionary**. When the framework builds the dataset configuration each year (and era for DATA) creates a separate and unique dataset entry.


To build the JSON dataset, run the following script:

.. code-block:: bash

  build_dataset.py -h


  Build dataset fileset in json format

  optional arguments:
  -h, --help            show this help message and exit
  --cfg CFG             Config file with parameters specific to the current run
  -k KEYS [KEYS ...], --keys KEYS [KEYS ...]
                        Dataset keys to select
  -d, --download        Download dataset files on local machine
  -o, --overwrite       Overwrite existing file definition json
  -c, --check           Check file existance in the local prefix
  -s, --split-by-year   Split output files by year
  -l LOCAL_PREFIX, --local-prefix LOCAL_PREFIX
                        Overwrite local prefix


Two version of the JSON dataset will be saved: one with the `root://xrootd-cms.infn.it//` prefix and one with a local prefix passed through the config file (with label `_local.json`) or throught the script parameters.


To download the files locally, run the script with the additional argument `--download`:

>>> build_dataset.y --cfg dataset/dataset_definitions.json -k dataset_key --download

To check if the files are already present in the local cluster run:

>>> build_dataset.y --cfg dataset/dataset_definitions.json -k dataset_key --check




Inspecting output
=================

.. code-block:: python

  from coffea.util import load
  out = load("output/test_run_v42/output.coffea")
  out.keys()

  
