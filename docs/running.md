# Running the analysis

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
  
Assuming that PocketCoffea is installed (for example inside the singularity machine), to run the analysis just use the `runner.py` script:

```bash
$ runner.py --help

    ____             __        __  ______      ________
   / __ \____  _____/ /_____  / /_/ ____/___  / __/ __/__  ____ _
  / /_/ / __ \/ ___/ //_/ _ \/ __/ /   / __ \/ /_/ /_/ _ \/ __ `/
 / ____/ /_/ / /__/ ,< /  __/ /_/ /___/ /_/ / __/ __/  __/ /_/ /
/_/    \____/\___/_/|_|\___/\__/\____/\____/_/ /_/  \___/\__,_/


usage: runner.py [-h] --cfg CFG [-ro RUN_OPTIONS] -o OUTPUTDIR [-t] [-lf LIMIT_FILES] [-lc LIMIT_CHUNKS] -e EXECUTOR [-s SCALEOUT] [-ll LOGLEVEL] [-f] [--executor-custom-setup EXECUTOR_CUSTOM_SETUP]

Run analysis on NanoAOD files using PocketCoffea processors

optional arguments:
  -h, --help            show this help message and exit
  --cfg CFG             Config file with parameters specific to the current run
  -ro RUN_OPTIONS, --run-options RUN_OPTIONS
                        User provided run options .yaml file
  -o OUTPUTDIR, --outputdir OUTPUTDIR
                        Output folder
  -t, --test            Run with limit 1 interactively
  -lf LIMIT_FILES, --limit-files LIMIT_FILES
                        Limit number of files
  -lc LIMIT_CHUNKS, --limit-chunks LIMIT_CHUNKS
                        Limit number of chunks
  -e EXECUTOR, --executor EXECUTOR
                        Overwrite executor from config (to be used only with the --test options)
  -s SCALEOUT, --scaleout SCALEOUT
                        Overwrite scalout config
  -ll LOGLEVEL, --loglevel LOGLEVEL
                        Console logging level
  -f, --full            Process all datasets at the same time
  --executor-custom-setup EXECUTOR_CUSTOM_SETUP
                        Python module to be loaded as custom executor setup
```

To run with a predefined executor just pass the `--executor` option string when calling the `runner.py` string:

```bash
$> runner.py --cfg analysis_config.py -o output --executor dask@lxplus
```
Have a look below for more details about the available executor setups.

### Executors availability

The `iterative` and `futures` executors are available everywhere as they run locally (single thread and multi-processing
respectively).


| Site | Supported executor | Executor string|
|------|--------------------|----------------|
|lxplus| dask               | dask@lxplus    |
|T3_CH_PSI| dask               | dask@T3_CH_PSI    |


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
  scalout: 1
  chunksize: 100000
  limit-files: null
  limit-chunks: null
  retries: 20
  tree-reduction: 20
  skip-bad-files: false
  voms-proxy: null

dask@lxplus:
  scaleout: 10
  cores-per-worker: 1
  mem-per-worker: "2GB"
  disk-per-worker: "2GB"
  worker-image: /cvmfs/unpacked.cern.ch/gitlab-registry.cern.ch/cms-analysis/general/pocketcoffea:lxplus-cc7-latest
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
configured by the user passing to `runner.py` a .yaml file containing options. These options are overwritten over the
default options for the requested executor. 

For example:
```bash

$> cat my_run_options.yaml

scaleout: 400
chunksize: 50000
queue: "espresso"
mem-per-worker: 6GB


$> runner.py  --cfg analysis_config.py -o output --executor dask@lxplus  --run-options my_run_options.yaml
```

The user can also modify on the fly some run options using arguments of the `runner.py` script. For example by limiting
the number of files or number of chunks to analyse (for testing purposes)

```bash
$> runner.py  --cfg analysis_config.py -o output --executor dask@lxplus \
              --limit-files 10  --limit-chunks 10
```

### Customize the executor software environment
The software environment where the executor runs the analysis is defined by the python environment where the analysis is
launched but also by the executor options. 

In particular if the user is using a `virtual environment` or `conda` to develop the core PocketCoffea code inside a singularity
image, there is an option to make the remote executors pickup the correct python env.

Just specify  `local-virtualenv: true` in the custom run options for virtualenv inside the singularity or `conda-env:
true` for using the conda (or mamba/micromamba) env activated where the `runner.py` script is run.

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


## Easy debugging

The easiest way to debug a new processor is to run locally on a single process. The `runner()` script has
the `--test` options which enables the `iterative` processor independently from the running configuration specified in
the configuration file. The processor is run on a file of each input dataset.

```bash
$ runner.py --cfg config.py --test
```

If you want to run locally with multiple processes for a fixed number of chunks just use the options:

```bash
$ runner.py --cfg config.py --test -e futures -s 4 --limit-files 10 --limit-chunks 10
```


## Adding support for a new executor/site

If you want to run PocketCoffea in a analysis environment that is still not centrally implemented you can implement a
custom `ExecutorFactory` and pass it to the `runner.py` script on the fly. In practice, this means that the user is free
to define from scratch the configuration of its cluster for running with Dask for example. 

Have a look at
[`pocket_coffea/executors/executors_lxplus.py`](https://github.com/PocketCoffea/PocketCoffea/tree/main/pocket_coffea/executors/executors_lxplus.py)
for a full-fledged example. 

The user factory must implement a class deriving from
[ExecutorFactoryABC](https://github.com/PocketCoffea/PocketCoffea/tree/main/pocket_coffea/executors/executors_base.py). 
The factory returns the type of coffea runner that will be instantiated and prepared the dask clusters, configuring it
and starting the jobs. 

```python
## custom executor defined in my_custom_executor.py by the user

from pocket_coffea.executors.executors_base import ExecutorFactoryABD
from .executors_base import IterativeExecutorFactory, FuturesExecutorFactory

from coffea import processor as coffea_processor

class ExecutorFactoryCustom(ExecutorFactorABC):

    def get(self):
        return coffea_processor.dask_executor

    def setup(self):
        '''This function is called by the base class constructor'''
        # do all the needed setup
        self.start_custom_dask_cluster()
        
    def start_custom_dask_cluster(self):
        self.dask_cluster = .......... #custom configuration

    def customize_args(self, args):
        '''This function customized the args that coffea uses to instantiate 
        the executor class passed by the get() method'''
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

The user's module must implement a `get_executor_factory(string, run_options)` method which return the class of executor
to be instantiated by the runner. 

The module is then used like this:

```bash
$> runner.py --cfg analysis_config.py -o output --executor dask  --executor-custom-setup my_custom_executor.py
--run-options my_run_options.py
```


:::{tip}
When the setup is working fine we would highly appreciate a PR to add the executor to the list of centrally supported
sites with default options!
:::


