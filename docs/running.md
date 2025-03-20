# Running the analysis
## CLI interface
Once installed in your software environemnt, either by using an apptainer image or a custom python environment,
PocketCoffea exposes different scripts and utilities with a command-line-interface (CLI)

```bash
$> pocket-coffea 

    ____             __        __  ______      ________
   / __ \____  _____/ /_____  / /_/ ____/___  / __/ __/__  ____ _
  / /_/ / __ \/ ___/ //_/ _ \/ __/ /   / __ \/ /_/ /_/ _ \/ __ `/
 / ____/ /_/ / /__/ ,< /  __/ /_/ /___/ /_/ / __/ __/  __/ /_/ /
/_/    \____/\___/_/|_|\___/\__/\____/\____/_/ /_/  \___/\__,_/


Running PocketCoffea version 0.8.0
- Documentation page:  https://pocketcoffea.readthedocs.io/
- Repository:          https://github.com/PocketCoffea/PocketCoffea

Run with --help option for the list of available commands

```

The commands and their options can be explored directly with the CLI: 
```bash
$> pocket-coffea  --help
Usage: pocket-coffea [OPTIONS] COMMAND [ARGS]...

Options:
  -v, --version BOOLEAN  Print PocketCoffea package version
  --help                 Show this message and exit.

Commands:
  build-datasets      Build dataset fileset in json format
  hadd-skimmed-files  Regroup skimmed datasets by joining different files...
  make-plots          Plot histograms produced by PocketCoffea processors
  run                 Run an analysis on NanoAOD files using PocketCoffea...
```

The `run` command is the one used to execute the analysis workflow on the datasets. This is replacing the previous
`pocket-coffea run` script, but it has the same user interface.


# Executors

The PocketCoffea analysis can be runned in different ways, locally or sending out jobs to a cluster throught the Dask
scheduling system. 

- local iterative processing
    The processor works on one chunk at a time. Useful for debugging and local testing.

- local multiprocessing
    The processor works on the chunks with multiple processes in parallel.
    Fast way to analyze small datasets locally. 

- Dask scale-up
    Scale the processor to hundreds of workers in HTCondor through the Dask scheduler.
    Automatic handling of jobs submissions and results aggregation.
  
Assuming that PocketCoffea is installed (for example inside the singularity machine), to run the analysis just use the
`pocket-coffea run` command:

```bash
$> pocket-coffea run --help

    ____             __        __  ______      ________
   / __ \____  _____/ /_____  / /_/ ____/___  / __/ __/__  ____ _
  / /_/ / __ \/ ___/ //_/ _ \/ __/ /   / __ \/ /_/ /_/ _ \/ __ `/
 / ____/ /_/ / /__/ ,< /  __/ /_/ /___/ /_/ / __/ __/  __/ /_/ /
/_/    \____/\___/_/|_|\___/\__/\____/\____/_/ /_/  \___/\__,_/


Usage: pocket-coffea run [OPTIONS]

  Run an analysis on NanoAOD files using PocketCoffea processors

Options:
  --cfg TEXT                      Config file with parameters specific to the
                                  current run  [required]
  -ro, --custom-run-options TEXT  User provided run options .yaml file
  -o, --outputdir TEXT            Output folder  [required]
  -t, --test                      Run with limit 1 interactively
  -lf, --limit-files INTEGER      Limit number of files
  -lc, --limit-chunks INTEGER     Limit number of chunks
  -e, --executor TEXT             Overwrite executor from config (to be used
                                  only with the --test options)
  -s, --scaleout INTEGER          Overwrite scaleout config
  -c, --chunksize INTEGER         Overwrite chunksize config
  -q, --queue TEXT                Overwrite queue config
  -ll, --loglevel TEXT            Console logging level
  -ps, --process-separately       Process each dataset separately
  --executor-custom-setup TEXT    Python module to be loaded as custom
                                  executor setup
  --help                          Show this message and exit.

```

To run with a predefined executor just pass the `--executor` option string when calling the the `run` command

```bash
$> pocket-coffea run --cfg analysis_config.py -o output --executor dask@lxplus
```
Have a look below for more details about the available executor setups.

### Executors availability

The `iterative` and `futures` executors are available everywhere as they run locally (single thread and multi-processing
respectively).


| Site | Supported executor | Executor string|
|------|--------------------|----------------|
|lxplus| dask               | dask@lxplus    |
|swan| dask                 | dask@swan    |
|T3_CH_PSI| dask            | dask@T3_CH_PSI |
|DESY NAF | dask            | dask@DESY_NAF |
|RWTH Aachen LX-Cluster | parsl, dask         | parsl@RWTH, dask@RWTH |
|RWTH CLAIX | dask         | dask@CLAIX |
|[Purdue Analysis Facility](https://analysis-facility.physics.purdue.edu)| dask | dask@purdue-af |
|[INFN Analysis Facility](https://infn-cms-analysisfacility.readthedocs.io/)| dask | dask@infn-af |
|Brown brux20 cluster | dask | dask@brux |
|Brown CCV Oscar | dask | dask@oscar |
|Maryland rubin cluster | dask, condor | dask@rubin condor@rubin |

---------------------------------------

## Executors setup
The analysis processors is handled by **executors**. The setup of the executor can vary between sites. A set of
predefined executors has been prepared and configured with default options for tested analysis facilties (lxplus,
T3_CH_PSI). More sites are being included, please send us a PR when you have successfully run PocketCoffea at your
facility!

The preconfigured executors are located in the
[`pocket_coffea/executors`](https://github.com/PocketCoffea/PocketCoffea/tree/main/pocket_coffea/executors) module. 
The default options for the running options and different type of executors are stores in
[`pocket_coffea/parameters/executor_options_defaults.yaml`](https://github.com/PocketCoffea/PocketCoffea/tree/main/pocket_coffea/parameters/executor_options_defaults.yaml). 

For example:
```yaml
general:
  scaleout: 1
  chunksize: 150000
  limit-files: null
  limit-chunks: null
  retries: 20
  tree-reduction: 20
  skip-bad-files: false
  voms-proxy: null
  ignore-grid-certificate: false
  group-samples: null

dask@lxplus:
  scaleout: 10
  cores-per-worker: 1
  mem-per-worker: "2GB"
  disk-per-worker: "2GB"
  worker-image: /cvmfs/unpacked.cern.ch/gitlab-registry.cern.ch/cms-analysis/general/pocketcoffea:lxplus-el9-stable
  death-timeout: "3600"
  queue: "microcentury"
  adaptive: false
  performance-report: true
  custom-setup-commands: null
  conda-env: false
  local-virtualenv: false

```


### Executor options
The dataset splitting (chunksize), the number of workers, and the other options, which may be executor-specific, must be
configured by the user passing to `pocket-coffea run` a .yaml file containing options. These options are overwritten over the
default options for the requested executor. 

For example:
```bash

$> cat my_run_options.yaml

scaleout: 400
chunksize: 50000
queue: "espresso"
mem-per-worker: 6GB


$> pocket-coffea run  --cfg analysis_config.py -o output --executor dask@lxplus  --custom-run-options my_run_options.yaml
```

The user can also modify on the fly some run options using arguments of the `pocket-coffea run` script. For example by limiting
the number of files or number of chunks to analyse (for testing purposes)

```bash
$> pocket-coffea run  --cfg analysis_config.py -o output --executor dask@lxplus \
              --limit-files 10  --limit-chunks 10 \
              --chunksize 150000 --queue espresso
```


### Process datasets separately and group samples
By default, the `pocket-coffea run` command will run all the datasets together in one shot and a single output `output_all.coffea` is saved.
In case one wants to save intermediate outputs, it is possible to run with the `--process-separately` option, where each dataset
is processed separately and an independent output `output_{dataset}.coffea` is saved for each dataset.

In case several datasets need to be processed with the `--process-separately` option, there is the additional possibility to group datasets
belonging to the same sample, process them together and save an output `output_{group}.coffea` for each group.
To group samples during processing it is sufficient to add an extra entry to the custom `run_options.yaml` file passed to `pocket-coffea run`,
defining the dictionary `group-samples`. Each key in this dictionary corresponds to the group name, and the values of the dictionary are lists
of samples.

As an example, by adding the following snippet to the `run_options.yaml` file:

```yaml
group-samples:
  signal:
    - "ttHTobb"
    - "ttHTobb_ttToSemiLep"
  TTToSemiLeptonic:
    - "TTToSemiLeptonic"
  TTbbSemiLeptonic:
    - "TTbbSemiLeptonic"
  "TTTo2L2Nu_SingleTop":
    - "TTTo2L2Nu"
    - "SingleTop"
  VJets:
    - "WJetsToLNu_HT"
    - "DYJetsToLL"
  VV_TTV:
    - "VV"
    - "TTV"
  DATA:
    - "DATA_SingleEle"
    - "DATA_SingleMuon"
```
and running the analysis with the command `pocket-coffea run --cfg config.py -ro run_options.yaml --process-separately`, will result in running
the analysis processor sequentially for 7 times, saving 7 independent outputs: `output_signal.coffea`, `output_TTToSemiLeptonic.coffea`, `output_TTbbSemiLeptonic.coffea`,
`output_TTTo2L2Nu_SingleTop.coffea`, `output_VJets.coffea`, `output_VV_TTV.coffea` and `output_DATA.coffea`.
For example, the output file `output_signal.coffea` file will contain the output obtained by processing the datasets of the samples `ttHTobb` and `ttHTobb_ttToSemiLep`,
for all the data-taking years specified in the `datasets["filter"]["year"]` dictionary in the constructor of the Configurator.

### Customize the executor software environment
The software environment where the executor runs the analysis is defined by the python environment where the analysis is
launched but also by the executor options. 

In particular if the user is using a `virtual environment` or `conda` to develop the core PocketCoffea code inside a singularity
image, there is an option to make the remote executors pickup the correct python env.

Just specify  `local-virtualenv: true` in the custom run options for virtualenv inside the singularity or `conda-env:
true` for using the conda (or mamba/micromamba) env activated where the `pocket-coffea run` script is run.

:::{admonition} Local environment support
:class: warning
The local environment propagation to the remote executors has been implemented at the moment for lxplus and some other
sites.  It is dependent of the presence of a shared filesystem to propagate the environment to the workers and activate
it before executing the dask worker jobs. 
:::

Moreover the user can add a list of completely custom setup commands that are run inside a worker job before executing
the analysis processor. Just specify them in the run options:

```yaml
$> cat my_run_options.yaml

custom-setup-commands:
  - echo $HOME
  - source /etc/profile.d/conda.sh
  - export CUSTOM_VARIABLE=1

```

## Dask scheduler on lxplus
The dask scheduler started by the `pocket-coffea run` script needs to stay alive in the user interactive session. 
This means that if you start a runner process directly in the lxplus machine (in a singularity session) you cannot
logout from the session. 

The solution is using the `tmux` program to keep your analysis session in the background. `tmux` allows you to create a
session, detach from it, exit lxplus, and at the next login reattch to the running session. 

This service needs to be activate, only once, for your user with `systemctl --user enable --now tmux.service`. The full
documentation about this (new) feature is available on the [Service
Portal](https://cern.service-now.com/service-portal?id=kb_article\&n=KB0008111).

Once setup you can start a tmux session as:
```bash
tmux new -s your-session-name
# start an apptainer image and launch your analysis

# press `Ctrl+b d` to detach from the session
```
your running session are visible with `tmux ls`. To reconnect do `tmux a -t your-session-name`. Look
[here](https://tmuxcheatsheet.com/) for more info about tmux. 


## Easy debugging

The easiest way to debug a new processor is to run locally on a single process. The `run` command has
the `--test` options which enables the `iterative` processor independently from the running configuration specified in
the configuration file. The processor is run on a file of each input dataset. If you set the `--process-separately` flag, the datasets are processed separately. Otherwise all datasets are processed at once.

```bash
$ pocket-coffea run --cfg config.py --test
```

If you want to run locally with multiple processes for a fixed number of chunks just use the options:

```bash
$ pocket-coffea run --cfg config.py --test -e futures -s 4 --limit-files 10 --limit-chunks 10 
```


## Adding support for a new executor/site

If you want to run PocketCoffea in a analysis environment that is still not centrally implemented you can implement a
custom `ExecutorFactory` and pass it to the `pocket-coffea run` script on the fly. In practice, this means that the user is free
to define from scratch the configuration of its cluster for running with Dask for example. 

Have a look at
[`pocket_coffea/executors/executors_lxplus.py`](https://github.com/PocketCoffea/PocketCoffea/tree/main/pocket_coffea/executors/executors_lxplus.py)
for a full-fledged example. 

The user factory must implement a class deriving from
[ExecutorFactoryABC](https://github.com/PocketCoffea/PocketCoffea/tree/main/pocket_coffea/executors/executors_base.py). 
The factory returns an instance of a Executor that is passed to the coffea Runner. 

```python
## custom executor defined in my_custom_executor.py by the user

from pocket_coffea.executors.executors_base import ExecutorFactoryABD
from .executors_base import IterativeExecutorFactory, FuturesExecutorFactory

from coffea import processor as coffea_processor

class ExecutorFactoryCustom(ExecutorFactorABC):

    def get(self):
        return coffea_processor.dask_executor(**self.customized_args())

    def setup(self):
        '''This function is called by the base class constructor'''
        # do all the needed setup
        self.start_custom_dask_cluster()
        
    def start_custom_dask_cluster(self):
        self.dask_cluster = .......... #custom configuration

    def customized_args(self):
        '''This function customized the args that coffea uses to instantiate 
        the executor class passed by the get() method'''
        args = super().customized_args()
        args["custom-arg"] = "..."
        return args

    def close(self):
        # cleanup
        self.dask_cluster.close()


def get_executor_factory(executor_name, **kwargs):
    if executor_name == "iterative":
        return IterativeExecutorFactory(**kwargs)
    elif executor_name == "futures":
        return FuturesExecutorFactory(**kwargs)
    elif  executor_name == "dask":
        return ExecutorFactoryCustom(**kwargs)

```

The user's module must implement a `get_executor_factory(string, run_options)` method which returns the instantiated Executor. 

The module is then used like this:

```bash
$> pocket-coffea run --cfg analysis_config.py -o output --executor dask  --executor-custom-setup my_custom_executor.py
--run-options my_run_options.py
```


:::{tip}
When the setup is working fine we would highly appreciate a PR to add the executor to the list of centrally supported
sites with default options!
:::


