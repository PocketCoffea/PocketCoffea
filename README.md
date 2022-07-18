# PocketCoffea :coffee:
Generalized framework for ttH(bb) columnar-based analysis with Coffea (https://coffeateam.github.io/coffea/) on centrally produced NanoAOD samples. The study of the dilepton final state is currently implemented and an extension to the semileptonic and fully hadronic final states is foreseen in the future.
## Framework structure
The development of the code is driven by user-friendliness, reproducibility and efficiency.
All the operations on ntuples are executed within Coffea processors, defined in `workflows/`, to save the output histograms in a Coffea file.
This file is then processed through the script `scripts/plot/make_plots.py` to plot the histograms.
The workflows can be run locally or by submitting jobs on a cluster. All the commands for the execution are wrapped up in a runner script `runner.py`.
All the relevant parameters for the execution of the processor such as the input and output files' names, the execution
parameters, the cuts to apply and the histogram settings are contained in a Python dictionary, defined in a
configuration file in `config/`, which is passed to the runner script as an argument.

### Filtering

We have now three modes of "filtering" event in the base workflow

1) **Skim**: before any object correction and preselection to remove events at the very beginning and save processing time.
    
Trigger, METFilters, PV selection, goldenJson lumi mask are applied here.
N.B,. the skim selection must be loose w.r.t of possible systematic variations or object corrections.

2) **Preselections**: a set of cuts is applied after the object corrections and object preselections.

JERC, lepton scales etc have been already applied before this step. The preselection cuts can use the fully corrected events.

3) **Categories**: groups of cut functions are applied as masks in order to define categories. Events are not removed but the masks are used for the output.


## How to run
### Build the dataset definition
Datasets are collection of samples, their corresponding files, and their metadata. 
Datasets are defined in JSON files following this syntax.

~~~
    "ttHTobb_latestver": {
        "sample": "ttHTobb",
        "json_output" : "datasets/signal_ttHTobb.json",
        "storage_prefix": "/pnfs/psi.ch/cms/trivcat/",
        "files": [
            { "das_names": ["/ttHTobb_M125_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v2/NANOAODSIM"],
              "metadata":{
                  "year": "2018",
                  "isMC": true
              }
            }
        ]
    }
~~~
The name of the object makes the dataset unique. The `sample` key is the one used internally in the framework. The
`json_output` defines the output location for the list of files. The `storage_prefix` defines the local position of the
files on the cluster. 
The same dataset can contain different group of **DAS** names with a separate metadata dictionary. They `year` metadata
is added to the final sample name. Doing so one can define in a single location all the files for different data-taking
periods. 


To build the JSON dataset, run the following script:
~~~
python scripts/dataset/build_dataset.py -h

Build dataset file in json format

optional arguments:
  -h, --help            show this help message and exit
  --cfg CFG             Config file with parameters specific to the current run
  -k KEYS [KEYS ...], --keys KEYS [KEYS ...]
                        Dataset keys
  -d, --download        Download dataset files on local machine
  -o, --overwrite       Overwrite existing files
  -c, --check           Check file existance in the local prefix

~~~
Two version of the JSON dataset will be saved: one with the `root://xrootd-cms.infn.it//` prefix and one with a local prefix passed through the config file (with label `_local.json`).
To download the files locally, run the script with the additional argument `--download`:
~~~
python scripts/dataset/build_dataset.py --cfg dataset/dataset_definitions.json  --download
~~~
To check if the files are already present in the local cluster run:
~~~
python scripts/dataset/build_dataset.py --cfg dataset/dataset_definitions.json  --check
~~~

### Execution on local machine with Futures Executor
To run the analysis workflow:
~~~
python runner.py --cfg config/base.py
~~~
The output folder `output/base` will be created, containing the output histograms `output/base/output.coffea` and the config file `output/base/config.py` that was given as an argument to `runner.py`.
### Plots
To produce plots, run the plot script:
~~~
python scripts/plot/make_plots.py --cfg config/base.py
~~~
or use the newly created copy of the config file (__useful for traceability__):
~~~
python scripts/plot/make_plots.py --cfg output/base/config.py
~~~
The output plots will be saved in `output/base/plots` together with the config file `output/base/plots/config.py` that was given as an argument to `scripts/plot/make_plots.py`.

### Config file
The config file in `.py` format is passed as the argument `--cfg` of the `runner.py` script. The file has the following structure:

| Parameter name    | Allowed values               | Description
| :-----:           | :---:                        | :------------------------------------------
| `dataset`         | string                       | Path of .txt file with list of DAS datasets
| `json`            | string                       | Path of .json file to create with NanoAOD files
| `storage_prefix`  | string                       | Path of storage folder to save datasets
| `workflow`        | 'base', 'mem'                | Workflow to run
| `input`           | string                       | Path of .json file, input to the workflow
| `output`          | string                       | Path of output folder
| `executor`        | 'futures', 'parsl/slurm'     | Executor
| `workers`         | int                          | Number of parallel threads (with futures)
| `scaleout`        | int                          | Number of jobs to submit (with parsl/slurm)
| `chunk`           | int                          | Chunk size
| `max`             | int                          | Maximum number of chunks to process
| `skipbadfiles`    | bool                         | Skip bad files
| `voms`            | string                       | Voms parameters (with condor)
| `limit`           | int                          | Maximum number of files per sample to process
| `finalstate`      | 'dilepton'                   | Final state of ttHbb process
| `preselections`   | list                         | List of preselection cuts
| `categories`      | dict                         | Dictionary of categories with cuts to apply
| `variables`       | $VARNAME : {$PARAMETERS}     | Dictionary of variables in 1-D histograms and plotting parameters
| `variables2d`     | n.o.                         | __To be implemented__
| `scale`           | 'linear', 'log'              | y-axis scale to apply to plots

The variables' names can be chosen among those reported in `parameters.allhistograms.histogram_settings`, which contains also the default values of the plotting parameters. If no plotting parameters are specified, the default ones will be used.

The plotting parameters can be customized for plotting, for example to rebin the histograms. In case of rebinning, the binning used in the plots has to be compatible with the one of the input histograms.

The `Cut` objects listed in `preselections` and `categories` have to be defined in `parameters.cuts.baseline_cuts`. A library of pre-defined functions for event is available in `lib.cut_functions`, but custom functions can be defined in a separate file.


### Profiling

#### CPU profiling
For profiling the CPU time of each function please select the *iterative* processor and then run
python as:
~~~
python -m cProfile -o profiling output.prof  runner.py --cfg profiling/mem.py
~~~
Running on a few files should be enough to get stable results.

After getting the profiler output we analyze it with the [Snakeviz](https://jiffyclub.github.io/snakeviz/)
library
~~~
snakeviz output.prof -s 
~~~
    and open on a browser the link shown by the program.

#### Memory profiling

For memory profiling we use the [memray](https://github.com/bloomberg/memray) library: 

~~~
python -m memray run -o profiling/memtest.bin runner.py --cfg config/config.py
~~~

the output can be visualized in many ways. One of the most useful is the `flamegraph`: 
~~~
memray flamegraph profiling/memtest.bin
~~~

then open the output .html file in you browser to explore the peak memory allocation. 

Alternatively the process can be monitored **live** during execution by doing:
~~~
memray run --live  runner.py --cfg config/config.py
~~~
