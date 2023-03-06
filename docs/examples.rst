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
                        Local prefix
  -ws WHITELIST_SITES [WHITELIST_SITES ...], --whitelist-sites WHITELIST_SITES [WHITELIST_SITES ...]
                        List of sites in the whitelist
  -bs BLACKLIST_SITES [BLACKLIST_SITES ...], --blacklist-sites BLACKLIST_SITES [BLACKLIST_SITES ...]
                        List of sites in the blacklist
  -rs REGEX_SITES, --regex-sites REGEX_SITES
                        Regex to filter sites


The **DBS** and **Rucio** services are queries to get information about the requested CMS datasets.

More than one version of the JSON dataset is saved:

* one dataset configuration file containing the remote files with an explicit path, without using the AAA xrootd
  redirector (this can help with uproot misbehaviour with the redirector).
* one `_redirector.json` dataset, containing the `root://xrootd-cms.infn.it//` prefix to use the AAA xrootd redirector.
* one with a local prefix (passed with the `-l` options), referring files in the local disk of the machine (no xrootd).

The dataset files can be split by years, to facilitate the bookeeping, with the `--split-by-year` option.

It is recommended to run on local files, if present, or to use the version of the dataset with direct links to files (no
AAA redirector).  One can filter or exclude the desidered sites using the *whitelist*, *blacklist*, and *regex*
options.

For example:

Restricting the dataset source in Europe

.. code-block:: python

   build_dataset.py --cfg datasets/datasets_definitions.json -o -rs 'T[123]_(FR|IT|DE|BE|CH|UK)_\w+' 


Restricting the dataset source to two possible whitelisted sites

.. code-block:: python

   build_dataset.py --cfg datasets/datasets_definitions.json -o -ws T3_CH_PSI T2_CH_CSCS


Blacklisting datasets at CERN and requesting the dataset in CH.

.. code-block:: python

   build_dataset.py --cfg datasets/datasets_definitions.json -o -bs T0_CH_CERN 'T[123]_CH_\w+' 




Inspecting output
=================

.. code-block:: python

  from coffea.util import load
  out = load("output/test_run_v42/output.coffea")
  out.keys()

  
