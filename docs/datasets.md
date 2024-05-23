# Datasets handling


The framework assigns a specific meaning to:  *datasets*, *part*,  *samples*, *sub-samples*.

*  **Dataset**: a dataset is a set of NanoAOD file containing events generated with the same configuration.

    Each dataset has a set of metadata, a unique datataking period, an *isMC* attribute. Each dataset groups files from one or more
    "CMS datasets" names (e.g. `"/ttHTobb_M125_TuneCP5_13TeV-powheg-pythia8/RunIISummer.....-v2/NANOAODSIM`). 
    Dataset can have a **part** metadata, which is usually also part of the name: for example the dataset `WjetsHT200-500_2016APV` should have the metadata
    **part=HT200-500**: inside the framework the part metadata can be used to define customizations. 

::::{important}
The dataset name must be unique inside the framework and it is used to identify the output objects.
The `build_datasets.py` script, defined below, makes sure that the final dataset name is the composition of the user defined label, the datataking period (and the era for data), the part metadata.
::::
  
*  **Sample**: a sample is a set of events representing a common physics process.

    The *sample name*  can be seen as the **category** of events: multiple *datasets* may have the same *sample* name. 
    Inside the framework the sample label is used to categorize the events and to customize weights, variables and
    categories. 
    
*  **Subsample**: a subsample is a subset of a sample: it can be seen as a categorization applied only to events part of
   a specific sample. 

    Subsamples are configured in a specific way using Cut objets: look at the [Configuration](./configuration.md) docs
    for more details. 
    

  
## Datasets definition files

Input datasets for the analyses are defined in a JSON file following the syntax below:

```python
{
    "DYJetsToLL_M-50":{
        "sample": "DYJetsToLL",
        "json_output": "datasets/DYJetsToLL_M-50.json",
        "files":[
            { "das_names": 
                ["/DYJetsToLL_M-50_TuneCP5_13TeV-amcatnloFXFX-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v2/NANOAODSIM"],
              "metadata": {
                  "year":"2018",
                  "isMC": true,
                  "xsec": 6077.22,
                  "part": "M-50"
              }
            }
        ]
    },

    "DATA_SingleMuon": {
        "sample": "DATA_SingleMuon",
        "json_output": "datasets/DATA_SingleMuon.json",
        "files": [
            {
                "das_names": [
                    "/SingleMuon/Run2018A-UL2018_MiniAODv2_NanoAODv9-v2/NANOAOD"
                ],
                "metadata": {
                    "year": "2018",
                    "isMC": false,
                    "primaryDataset": "SingleMuon",
                    "era": "A"
                }
            },
            {
                "das_names": [
                    "/SingleMuon/Run2018B-UL2018_MiniAODv2_NanoAODv9-v2/NANOAOD"
                ],
                "metadata": {
                    "year": "2018",
                    "isMC": false,
                    "primaryDataset": "SingleMuon",
                    "era": "B"
                }
            }
         ]
    }
}
```

The framework uses this definition to build the actual dataset json file. 
    
* The user defined label makes the dataset unique.
* The `sample` key is the one used internally in the framework to identify the sample type (see explanation above).
* The `json_output` defines the output location for the list of files.

The same dataset can contain different group of dataset (**DAS**) names, each  with a **separate metadata
dictionary**. Each group will be interpreted by the `build_datasets.py` script to create unique set of files, with a
unique label build as `{user_defined_label}__{part}__{year}_{Era}`.

To build the JSON dataset, run the following script:

```bash
   ___       _ __   _____       __               __ 
  / _ )__ __(_) /__/ / _ \___ _/ /____ ____ ___ / /_
 / _  / // / / / _  / // / _ `/ __/ _ `(_-</ -_) __/
/____/\_,_/_/_/\_,_/____/\_,_/\__/\_,_/___/\__/\__/ 
                                                   

usage: build_datasets.py [-h] [--cfg CFG] [-k KEYS [KEYS ...]] [-d] [-o] [-c] [-s] [-l LOCAL_PREFIX] [-ws WHITELIST_SITES [WHITELIST_SITES ...]] [-bs BLACKLIST_SITES [BLACKLIST_SITES ...]] [-rs REGEX_SITES]

Build dataset fileset in json format

optional arguments:
  -h, --help            show this help message and exit
  --cfg CFG             Config file with parameters specific to the current run
  -k KEYS [KEYS ...], --keys KEYS [KEYS ...]
                        Dataset keys to select
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

```

The **DBS** and **Rucio** services are queries to get information about the requested CMS datasets.

More than one version of the JSON dataset is saved:

* one dataset configuration file containing the remote files with an explicit path, without using the AAA xrootd
  redirector (this can help with uproot misbehaviour with the redirector).
* one `_redirector.json` dataset, containing the `root://xrootd-cms.infn.it//` prefix to use the AAA xrootd redirector.
* one with a local prefix (passed with the `-l` options), referring files in the local disk of the machine (no xrootd).

The dataset files output can be split by years, to facilitate the bookeeping, with the `--split-by-year` option.

It is recommended to run on local files, if present, or to use the version of the dataset with direct links to files (no
AAA redirector).  One can filter or exclude the desidered sites using the *whitelist*, *blacklist*, and *regex*
options.

For example:

Restricting the dataset source in Europe (recommended for working from lxplus)

```bash
pocket-coffea build-datasets --cfg datasets/datasets_definitions.json -o -rs 'T[123]_(FR|IT|DE|BE|CH|UK)_\w+' 
```

Restricting the dataset source to two possible whitelisted sites

```bash
pocket-coffea build-datasets --cfg datasets/datasets_definitions.json -o -ws T3_CH_PSI T2_CH_CSCS
```

Blacklisting datasets at CERN and requesting the dataset in CH.

```bash 
pocket-coffea build-datasets --cfg datasets/datasets_definitions.json -o -bs T0_CH_CERN 'T[123]_CH_\w+' 
```



## Datasets building output

The output of the `build_datasets.py` script is the actual input of the coffea processing. It contains metadata and the
explicit list of files to be analyzed. 

Moreover the output contains the total number of events contained in the dataset (from DBS) and the size bytes of the dataset.

```json

{
    "DYToMuMu_M-50_2023": {
        "metadata": {
            "das_names": "['/DYToMuMu_M-20_TuneCP5_13p6TeV-pythia8/Run3Winter23NanoAOD-GTv4Digi_GTv4_MiniGTv4_NanoGTv4_126X_mcRun3_2023_forPU65_v4-v2/NANOAODSIM']",
            "sample": "DYToMuMu",
            "year": "2023",
            "isMC": "True",
            "xsec": "6077.22",
            "nevents": "9710000",
            "size": "9115860727"
        },
        "files": [
            "root://eoscms.cern.ch//eos/cms/store/mc/Run3Winter23NanoAOD/DYToMuMu_M-20_TuneCP5_13p6TeV-pythia8/NANOAODSIM/GTv4Digi_GTv4_MiniGTv4_NanoGTv4_126X_mcRun3_2023_forPU65_v4-v2/2820000/069af8a1-258b-4656-b8c8-b4a47bece34d.root",
            "root://eoscms.cern.ch//eos/cms/store/mc/Run3Winter23NanoAOD/DYToMuMu_M-20_TuneCP5_13p6TeV-pythia8/NANOAODSIM/GTv4Digi_GTv4_MiniGTv4_NanoGTv4_126X_mcRun3_2023_forPU65_v4-v2/2820000/0fbaf65c-a211-4008-88ce-404b91839955.root",
            "root://eoscms.cern.ch//eos/cms/store/mc/Run3Winter23NanoAOD/DYToMuMu_M-20_TuneCP5_13p6TeV-pythia8/NANOAODSIM/GTv4Digi_GTv4_MiniGTv4_NanoGTv4_126X_mcRun3_2023_forPU65_v4-v2/2820000/22ba0848-79dc-4bfb-862e-93a1d921e8c7.root",
            "root://eoscms.cern.ch//eos/cms/store/mc/Run3Winter23NanoAOD/DYToMuMu_M-20_TuneCP5_13p6TeV-pythia8/NANOAODSIM/GTv4Digi_GTv4_MiniGTv4_NanoGTv4_126X_mcRun3_2023_forPU65_v4-v2/2820000/33d55337-9da7-484d-94fa-bd188a9db20a.root",
            "root://eoscms.cern.ch//eos/cms/store/mc/Run3Winter23NanoAOD/DYToMuMu_M-20_TuneCP5_13p6TeV-pythia8/NANOAODSIM/GTv4Digi_GTv4_MiniGTv4_NanoGTv4_126X_mcRun3_2023_forPU65_v4-v2/2820000/406df78e-0d1f-475d-8ed6-db3c4521e124.root",
            "root://eoscms.cern.ch//eos/cms/store/mc/Run3Winter23NanoAOD/DYToMuMu_M-20_TuneCP5_13p6TeV-pythia8/NANOAODSIM/GTv4Digi_GTv4_MiniGTv4_NanoGTv4_126X_mcRun3_2023_forPU65_v4-v2/2820000/51c94860-e84d-4f84-89ad-1019d6f88d40.root",
            "root://eoscms.cern.ch//eos/cms/store/mc/Run3Winter23NanoAOD/DYToMuMu_M-20_TuneCP5_13p6TeV-pythia8/NANOAODSIM/GTv4Digi_GTv4_MiniGTv4_NanoGTv4_126X_mcRun3_2023_forPU65_v4-v2/2820000/7d3ba5d2-663d-4bf4-9099-04f980bb5431.root",
            "root://eoscms.cern.ch//eos/cms/store/mc/Run3Winter23NanoAOD/DYToMuMu_M-20_TuneCP5_13p6TeV-pythia8/NANOAODSIM/GTv4Digi_GTv4_MiniGTv4_NanoGTv4_126X_mcRun3_2023_forPU65_v4-v2/2820000/974b9b73-cf67-4a9a-90ef-e58b074dc731.root",
            "root://eoscms.cern.ch//eos/cms/store/mc/Run3Winter23NanoAOD/DYToMuMu_M-20_TuneCP5_13p6TeV-pythia8/NANOAODSIM/GTv4Digi_GTv4_MiniGTv4_NanoGTv4_126X_mcRun3_2023_forPU65_v4-v2/2820000/a394873d-72de-4ba8-93d1-0fd7aede23aa.root",
            "root://eoscms.cern.ch//eos/cms/store/mc/Run3Winter23NanoAOD/DYToMuMu_M-20_TuneCP5_13p6TeV-pythia8/NANOAODSIM/GTv4Digi_GTv4_MiniGTv4_NanoGTv4_126X_mcRun3_2023_forPU65_v4-v2/2820000/a832ed57-84be-403a-a053-c8223ed7fb9e.root",
            "root://eoscms.cern.ch//eos/cms/store/mc/Run3Winter23NanoAOD/DYToMuMu_M-20_TuneCP5_13p6TeV-pythia8/NANOAODSIM/GTv4Digi_GTv4_MiniGTv4_NanoGTv4_126X_mcRun3_2023_forPU65_v4-v2/2820000/ae3a3eaf-9a6b-4d1c-8bcf-12b5eab6d1a1.root",
            "root://eoscms.cern.ch//eos/cms/store/mc/Run3Winter23NanoAOD/DYToMuMu_M-20_TuneCP5_13p6TeV-pythia8/NANOAODSIM/GTv4Digi_GTv4_MiniGTv4_NanoGTv4_126X_mcRun3_2023_forPU65_v4-v2/2820000/beecdf9a-3b95-4910-a1e9-834050bfa8c8.root",
            "root://eoscms.cern.ch//eos/cms/store/mc/Run3Winter23NanoAOD/DYToMuMu_M-20_TuneCP5_13p6TeV-pythia8/NANOAODSIM/GTv4Digi_GTv4_MiniGTv4_NanoGTv4_126X_mcRun3_2023_forPU65_v4-v2/2820000/ce0c9c7c-87ff-44fd-92e4-c02c9e9bcfbc.root",
            "root://eoscms.cern.ch//eos/cms/store/mc/Run3Winter23NanoAOD/DYToMuMu_M-20_TuneCP5_13p6TeV-pythia8/NANOAODSIM/GTv4Digi_GTv4_MiniGTv4_NanoGTv4_126X_mcRun3_2023_forPU65_v4-v2/2820000/d4d4e358-9257-4780-a666-b8d23cd743f5.root",
            "root://eoscms.cern.ch//eos/cms/store/mc/Run3Winter23NanoAOD/DYToMuMu_M-20_TuneCP5_13p6TeV-pythia8/NANOAODSIM/GTv4Digi_GTv4_MiniGTv4_NanoGTv4_126X_mcRun3_2023_forPU65_v4-v2/2820000/ded06c1b-5ae5-40c6-90fb-4a1afd14bd21.root",
            "root://eoscms.cern.ch//eos/cms/store/mc/Run3Winter23NanoAOD/DYToMuMu_M-20_TuneCP5_13p6TeV-pythia8/NANOAODSIM/GTv4Digi_GTv4_MiniGTv4_NanoGTv4_126X_mcRun3_2023_forPU65_v4-v2/2820000/ec57870b-65f9-48a5-a7be-0b098a930975.root",
            "root://eoscms.cern.ch//eos/cms/store/mc/Run3Winter23NanoAOD/DYToMuMu_M-20_TuneCP5_13p6TeV-pythia8/NANOAODSIM/GTv4Digi_GTv4_MiniGTv4_NanoGTv4_126X_mcRun3_2023_forPU65_v4-v2/2820000/ed0b1364-a071-4063-8123-f65097e92e34.root"]
            }
}
```
 
