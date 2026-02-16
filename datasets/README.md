# Datasets handling

In this section is reported a short guide on the usage of the `build_dataset.py` script.
For more details, look at the [full documentation](https://pocketcoffea.readthedocs.io/en/latest/datasets.html).

The datasets are defined in a file `datasets_definitions_example.json`. The framework uses this definition to build the actual json file dataset. 
    
* The user defined label makes the dataset unique.
* The `sample` key is the one used internally in the framework to identify the sample type (see explanation above).
* The `json_output` defines the output location for the list of files.

To build the JSON dataset, run the script `build_dataset.py`:

```bash
   ___       _ __   _____       __               __ 
  / _ )__ __(_) /__/ / _ \___ _/ /____ ____ ___ / /_
 / _  / // / / / _  / // / _ `/ __/ _ `(_-</ -_) __/
/____/\_,_/_/_/\_,_/____/\_,_/\__/\_,_/___/\__/\__/ 
                                                   

usage: build_datasets [--help] [--cfg CFG] [-k KEYS [KEYS ...]] [-d] [-o] [-c] [-s] [-l LOCAL_PREFIX] 
			[-ws WHITELIST_SITE -ws WHITELIST_SITE ...] [-bs BLACKLIST_SITE -bs BLACKLIST_SITES ...] [-ps PRIORITYLIST_SITE -ps PRIORITYLIST_SITES ...] 
			[-rs REGEX_SITES] [-sort SORTING] [-ir] [-p 8]

  Build dataset fileset in json format

Options:
  --cfg TEXT                      Config file with parameters specific to the
                                  current run  [required]
  -k, --keys TEXT                 Keys of the datasets to be created. If None,
                                  the keys are read from the datasets
                                  definition file.
  -d, --download                  Download datasets from DAS
  -o, --overwrite                 Overwrite existing .json datasets
  -c, --check                     Check existence of the datasets
  -s, --split-by-year             Split datasets by year
  -l, --local-prefix TEXT
  -ws, --allowlist-sites TEXT     List of sites in whitelist
  -bs, --blocklist-sites TEXT     List of sites in blacklist
  -ps, --prioritylist-sites TEXT  List of priorities to sort sites (requires
                                  sort: priority)
  -rs, --regex-sites TEXT         example: -rs
                                  'T[123]_(FR|IT|DE|BE|CH|UK)_\w+' to serve
                                  data from sites in Europe.
  -sort, --sort-replicas TEXT     Sort replicas (default: geoip).
  -ir, --include-redirector       Use the redirector path if no site is
                                  available after the specified whitelist,
                                  blacklist and regexes are applied for sites.
  -p, --parallelize INTEGER
  -h, --help                      Show this message and exit.

```

The **DBS** and **Rucio** services are queries to get information about the requested CMS datasets.

More than one version of the JSON dataset is saved:

* one dataset configuration file containing the remote files with an explicit path, without using the AAA xrootd
  redirector (this can help with uproot misbehaviour with the redirector).
* one `_redirector.json` dataset, containing the `root://xrootd-cms.infn.it//` prefix to use the AAA xrootd redirector.
* one with a local prefix (passed with the `-l` options), referring files in the local disk of the machine (no xrootd).

By default, the output .json files storing the file lists are not overridden, to avoid data loss. The output .json files
can be overridden with the `--overwrite` option.
The dataset files output can be split by years, to facilitate the bookeeping, with the `--split-by-year` option.

It is recommended to run on local files, if present, or to use the version of the dataset with direct links to files (no
AAA redirector).  One can filter or exclude the desidered sites using the *whitelist*, *blacklist*, and *regex*
options.

For example:

Restricting the dataset source in Europe (recommended for working from lxplus)

```bash
build_dataset.py --cfg datasets/datasets_definitions.json -o -rs 'T[123]_(FR|IT|DE|BE|CH|UK)_\w+' 
```

Restricting the dataset source to two possible whitelisted sites

```bash
build_dataset.py --cfg datasets/datasets_definitions.json -o -ws T3_CH_PSI T2_CH_CSCS
```

Blacklisting datasets at CERN and requesting the dataset in CH.

```bash 
build_dataset.py --cfg datasets/datasets_definitions.json -o -bs T0_CH_CERN 'T[123]_CH_\w+' 
```

If the datasets are present in the local storage of the computer cluster, one can specify the prefix of the local storage folder:

```bash 
build_dataset.py --cfg datasets/datasets_definitions.json -o --local-prefix /pnfs/psi.ch/cms/trivcat
```


## Datasets building output

The output of the `build_datasets.py` script is the actual input of the coffea processing. It contains metadata and the
explicit list of files to be analyzed. 

Moreover the output contains the total number of events contained in the dataset (from DBS) and the size bytes of the dataset.

```json

{
    "TTToSemiLeptonic_2018": {
        "metadata": {
            "das_names": "['/TTToSemiLeptonic_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM']",
            "sample": "TTToSemiLeptonic",
            "year": "2018",
            "isMC": "True",
            "xsec": "365.4574",
            "nevents": "476408000",
            "size": "1030792999916"
        },
        "files": [
            "root://dcache-cms-xrootd.desy.de:1094//store/mc/RunIISummer20UL18NanoAODv9/TTToSemiLeptonic_TuneCP5_13TeV-powheg-pythia8/NANOAODSIM/106X_upgrade2018_realistic_v16_L1v1-v1/120000/0520A050-AF68-EF43-AA5B-5AA77C74ED73.root",
            "root://dcache-cms-xrootd.desy.de:1094//store/mc/RunIISummer20UL18NanoAODv9/TTToSemiLeptonic_TuneCP5_13TeV-powheg-pythia8/NANOAODSIM/106X_upgrade2018_realistic_v16_L1v1-v1/120000/0E9EA19A-AE0E-3149-88C3-D733240FF5AB.root",
            "root://dcache-cms-xrootd.desy.de:1094//store/mc/RunIISummer20UL18NanoAODv9/TTToSemiLeptonic_TuneCP5_13TeV-powheg-pythia8/NANOAODSIM/106X_upgrade2018_realistic_v16_L1v1-v1/120000/143F7726-375A-3D48-9D53-D6B071CED8F6.root",
            "root://dcache-cms-xrootd.desy.de:1094//store/mc/RunIISummer20UL18NanoAODv9/TTToSemiLeptonic_TuneCP5_13TeV-powheg-pythia8/NANOAODSIM/106X_upgrade2018_realistic_v16_L1v1-v1/120000/15FC5EA3-70AA-B640-8748-BD5E1BB84CAC.root",
            "root://dcache-cms-xrootd.desy.de:1094//store/mc/RunIISummer20UL18NanoAODv9/TTToSemiLeptonic_TuneCP5_13TeV-powheg-pythia8/NANOAODSIM/106X_upgrade2018_realistic_v16_L1v1-v1/120000/1CD61F25-9DE8-D741-9200-CCBBA61E5A0A.root",
            "root://dcache-cms-xrootd.desy.de:1094//store/mc/RunIISummer20UL18NanoAODv9/TTToSemiLeptonic_TuneCP5_13TeV-powheg-pythia8/NANOAODSIM/106X_upgrade2018_realistic_v16_L1v1-v1/120000/1D885366-E280-1243-AE4F-532D326C2386.root",
            "root://dcache-cms-xrootd.desy.de:1094//store/mc/RunIISummer20UL18NanoAODv9/TTToSemiLeptonic_TuneCP5_13TeV-powheg-pythia8/NANOAODSIM/106X_upgrade2018_realistic_v16_L1v1-v1/120000/23AD2392-C48B-D643-9E16-C93730AA4A02.root",
            "root://dcache-cms-xrootd.desy.de:1094//store/mc/RunIISummer20UL18NanoAODv9/TTToSemiLeptonic_TuneCP5_13TeV-powheg-pythia8/NANOAODSIM/106X_upgrade2018_realistic_v16_L1v1-v1/120000/245961C8-DE06-8F4F-9E92-ED6F30A097C4.root",
            "root://dcache-cms-xrootd.desy.de:1094//store/mc/RunIISummer20UL18NanoAODv9/TTToSemiLeptonic_TuneCP5_13TeV-powheg-pythia8/NANOAODSIM/106X_upgrade2018_realistic_v16_L1v1-v1/120000/262EAEE2-14CC-2A44-8F4B-B1A339882B25.root",
            "root://dcache-cms-xrootd.desy.de:1094//store/mc/RunIISummer20UL18NanoAODv9/TTToSemiLeptonic_TuneCP5_13TeV-powheg-pythia8/NANOAODSIM/106X_upgrade2018_realistic_v16_L1v1-v1/120000/2EEEF2A2-D775-764F-8ED6-EF0D5B425739.root",
            "root://dcache-cms-xrootd.desy.de:1094//store/mc/RunIISummer20UL18NanoAODv9/TTToSemiLeptonic_TuneCP5_13TeV-powheg-pythia8/NANOAODSIM/106X_upgrade2018_realistic_v16_L1v1-v1/120000/329FB0B6-F45B-8D4B-A27C-3D61E33C91DC.root"]
    }
}
```
