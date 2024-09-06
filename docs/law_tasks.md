# Run pocket-coffea Analysis with law Tasks

## Introduction

[law documentation](https://law.readthedocs.io/en/latest/)

## Analysis Setup

- assume you are familiar with general setup of an analysis config in PocketCoffea (https://pocketcoffea.readthedocs.io/en/stable/analysis_example.html)
- additional files are
  - law.cfg: configuration file for law tasks
  - setup.sh: setup script to set environment variables and index law (optional, but recommended)

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


### law configuration

detailed description in the [configuration section](https://law.readthedocs.io/en/latest/config.html) of the law documentation.

```bash
[modules]

# must be accessible in to python (PYTHONPATH)
pocket_coffea.law_tasks.tasks.datasets
pocket_coffea.law_tasks.tasks.runner
pocket_coffea.law_tasks.tasks.plotting

# custom tasks
custom_tasks.tasks
```

### setup.sh

```bash
# setup PATHs and configurations for law

action() {
    # get important paths
    orig="${PWD}"
    this_file="$( [ ! -z "${ZSH_VERSION}" ] && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"
    cd "${orig}"

    # add directory to pythonpath, so modules can be imported
    export PYTHONPATH="${this_dir}:${PYTHONPATH}"

    # === law ===
    export LAW_CONFIG_FILE="${this_dir}/law.cfg"
    export LAW_HOME="${this_dir}/.law"

    # source law's bash completion script
    if which law &> /dev/null; then
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

action "$@"
```

## Running law tasks

```bash
law run Plotting --cfg config.py --print-status -1
law run Plotting --cfg config.py --remove-output 0,i,1
```