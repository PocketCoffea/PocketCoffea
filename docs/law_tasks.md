# Running with law

## Introduction

A typical analysis with PocketCoffea consists of several steps, such as e.g. dataset preparation, dataset processing and plotting, which depend on each other. To handle these dependencies the python package [law](https://law.readthedocs.io/en/latest/) can be used to structure the analysis. With law you can define tasks that depend on each other by defining requirements and outputs.
```python
import law


class MyTask(law.Task):
    def requires(self):
        return MyOtherTask.req(self)

    def output(self):
        return law.LocalFileTarget("output.txt")

    def run(self):
        self.output().dump("Hello, World!")
```
law checks wether the output of a tasks exists and runs the task and its dependencies if the output is missing.

The following tasks are available in PocketCoffea (`pocket_coffea/law_tasks/tasks`):
  - `CreateDatasets`: prepare datasets for processing
  - `JetCalibration`: build jet calibration factory
  - `Runner`: run the analysis on the datasets
  - `Plotter`: create histogram plots from the processed datasets
  - `PlotSystematics`: create histogram plots for systematic shifts

## Analysis Setup

For a general setup of an analysis with PocketCoffea see the [analysis example](https://pocketcoffea.readthedocs.io/en/stable/analysis_example.html). Here we focus on the setup with law.

Two additional files are needed to setup the analysis with law:
  - law.cfg: configuration file for law
  - setup.sh: setup script to set environment variables and index law tasks

So the directory structure looks like this:
```
analysis-config
|   config.py
|   law.cfg
|   setup.sh
|
└── datasets
|   |   datasets_definition.json
|
└── parameters
|   |   object_preselection.yaml
|   |   plotting.yaml
|   |   ...
```

The setup file could look like this:
```bash
# setup PATHs and configurations for law

setup_analysis() {
    # get important paths
    orig="${PWD}"
    this_file="$( [ ! -z "${ZSH_VERSION}" ] && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"
    cd "${orig}"

    # === analysis ===
    export ANALYSIS_STORE="/net/scratch/analysis_outputs"

    # === law ===
    export LAW_CONFIG_FILE="${this_dir}/law.cfg"
    export LAW_HOME="${this_dir}/.law"

    if which law &> /dev/null; then
        # source law's bash completion script
        source "$( law completion )" ""
        # index law and check if it was successful
        law index -q
        return_code=$?
        if [ ${return_code} -ne 0 ]; then
            echo "failed to index law with error code ${return_code}"
            return 1
        else
            echo "law tasks were successfully indexed"
        fi
    fi

}

setup_analysis "$@"
```

The `ANALYSIS_STORE` variable is used to specify the output directory for the analysis. Every task creates a subdirectory with its output.

### law configuration

A detailed description of the configuration of law can be found in the [configuration section](https://law.readthedocs.io/en/latest/config.html) of the law documentation.

The important part is the `[modules]` section, where all the modules that you want to use in your analysis are listed.

```bash
[modules]

# must be accessible to python (PYTHONPATH)
pocket_coffea.law_tasks.tasks.datasets
pocket_coffea.law_tasks.tasks.runner
pocket_coffea.law_tasks.tasks.plotting

# custom tasks
custom_tasks.tasks
```
You can also add custom tasks here, which must be accessible via `PYTHONPATH`. So you should add the following to your `setup.sh`:
```bash
export PYTHONPATH="${this_dir}:${PYTHONPATH}"
```

## Running law tasks

### Getting an Overview
You can get an overview of all available tasks if you index law in verbose mode:
```bash
law index -v
```

Tasks have different parameters which control the behavior of the task. The available parameters will be listed if you press `TAB` twice after the task name in the command line.
```bash
law run CreateDatasets <TAB><TAB>
```
To get an extensive list with descriptions of all parameters you can use the `--help` flag:
```bash
law run CreateDatasets --help
```

Tasks depend on each other, so you can use the `--print-deps <DEPTH>` flag to get an overview of the dependencies of a task, where `<DEPTH>` is an integer that specifies the depth of the dependency tree (-1 displays all dependencies).
```bash
law run Plotter --print-deps -1
```
This just prints the dependencies. To check which tasks have already been executed and which are still missing you can use the `--print-status <DEPTH>` flag.
```bash
law run Plotter --print-status -1
```

### Executing Tasks
To execute a task you simply use `law run` followed by the task name and the parameters. Some parameters have defaults, so you don't have to specify them. If you want to overwrite a default parameter you can do this by specifying the parameter with the new value.

Let's assume you have your configuration file in a folder `config/config.py`, you want to run on lxplus with the dask executor and you want to scale out to 50 workers. In the plots you dont want to plot the data and you want the y-axis to be logarithmic. You can run the plotting task like this:
```bash
law run Plotter --cfg config/config.py --version version01 --executor dask@lxplus --scaleout 50 --blind True --log-scale 
```
The version parameter is used to create a new directory in the output directory to for example separate different configurations. The parameters `--blind` and `--log-scale` are both boolean parameters, so to set them to `True` you can just specify them without a value.

If a task has already been executed and you want to rerun it you can use the `--remove-output <DEPTH>` flag, where `<DEPTH>` can be an integer or a tuple. The first integer specifies the depth of the dependency tree. For the second value you can choose between `d` (dry), `i` (interactive) and `a` (all). The third value is a boolean that specifies if the task should be executed after the removal (1) or not (0).
```bash
law run Plotter --cfg config.py --remove-output 0,i,1
```

## File Transfer to WLCG

You can transfer files to the WLCG. For this you have to specify a directory on the WLCG where the files should be transferred to. This can be done in the law configuration file with
```bash
[wlcg_fs]
base: root://eosuser.cern.ch///eos/user/<u>/<user>/analysis_outputs
```

The file transfer requires `gfal2` to be installed (see the [law installation instructions](https://github.com/riga/law?tab=readme-ov-file#installation-and-dependencies)).

You need to have a proxy certificate to be able to transfer the files (the same that is used for getting dataset information). Alternatively you can create a Kerberos ticket with `kinit`.

Currently the following tasks support file transfer:
- DatacardProducer

### DatacardProducer
The `DatacardProducer` task creates a datacard and corresponding shapes file. It requires a python file containing the following information:
- `MCProcesses`: the configuration of Monte Carlo processes that should be included in the datacard
- `Systematics`: the configuration of systematic uncertainties that should be written to the datacard

Furthermore it can contain the following optional information:
- `DataProcesses`: the configuration of data processes that should be included in the datacard
- `mcstat`: the configuration for the automatic statistical uncertainties
- `bins_edges`: the configuration of the bin edges for the histograms
- `bin_prefix`: the prefix for the bin in the datacard
- `suffix`: the suffix for the bin in the datacard

For detailed information check the corresponing section in the documentation about the `Datacard` class.

To run the task, use the following command (with optional transfer to WLCG):
```bash
law run DatacardProducer --cfg config.py --version version01 --stat-config stat_config.py --variable <variable-name> --category <category-name> --years <year1,year2,...> (--transfer)
```