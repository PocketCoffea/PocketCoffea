# Installation

The installation of the PocketCoffea package is very simple and based only on the `python` enviroment tools.

## CERN lxplus with jobs submission
----------------------------
If you are working on CERN lxplus and you will scale your processing on HTCondor, follow this installation
instructions.

The best way to use the package on lxplus is by using **singularity** images. This method is particulary important if you need to run the Dask scheduler on HTCondor.
A **singularity** (also called apptainer) image is just a virtual environment which isolate your code in a well defined environment with the correct python packages and versions.
For more information and for a comprehensive tutorial on singularity have a look at the [HSF
tutorial](https://hsf-training.github.io/hsf-training-docker/10-singularity/index.html).

A Docker image containing a python environment with the PocketCoffea package is automatically build by the GitLab repository CD/CI on the latest version of the
code and for each tagged version. The image registry is
[here](https://gitlab.cern.ch/cms-analysis/general/PocketCoffea/container_registry/16693), and it can be used directly
with Docker. 

The docker image is then **unpacked** to a Singularity image which is available on **cvmfs**. 

:::{tip}
The singularity image is **the preferred way to setup** the environment, both for running user's analysis and for local development. 
:::

### Using Singularity to run the analysis

The singularity environment is activated on **lxplus** with the following command:

```bash
apptainer shell --bind /afs -B /cvmfs/cms.cern.ch \
                --bind /tmp  --bind /eos/cms/ \
    --env KRB5CCNAME=$KRB5CCNAME --bind /etc/sysconfig/ngbauth-submit  \
    /cvmfs/unpacked.cern.ch/gitlab-registry.cern.ch/cms-analysis/general/pocketcoffea:lxplus-cc7-latest 
```

The last part of the command contains the image version on unpacked:
**cms-analysis/general/pocketcoffea:lxplus-cc7-latest**. 

Once inside the environment no installation is needed. The PocketCoffea scripts are globally available and the user's
analysis can be run directly. 

If a specific image is needed for a computing environment, more flavours of the docker/singularity image can be
built. Please get in touch!

### Using Singularity for local development

If the user needs to modify locally the central PocketCoffea code, the singularity image can still be used as a baseline
(for dependencies), but a local
installation of the package is needed. Follow the instructions: 


```bash
#Enter the image
apptainer shell --bind /afs -B /cvmfs/cms.cern.ch \
         --bind /tmp  --bind /eos/cms/ \
         --env KRB5CCNAME=$KRB5CCNAME --bind /etc/sysconfig/ngbauth-submit  \
         /cvmfs/unpacked.cern.ch/gitlab-registry.cern.ch/cms-analysis/general/pocketcoffea:lxplus-cc7-latest 

# Create a local virtual environment using the packages defined in the singularity image
python -m venv --system-site-packages myenv

# Activate the environment
source myenv/bin/activate

# Clone locally the PocketCoffea repo
git clone git@github.com:PocketCoffea/PocketCoffea.git
cd PocketCoffea

# Install in EDITABLE mode
pip install -e .[dev]
```

The next time the user enters in the singularity the virtual environment needs to be activated. 

:::{admonition} Work in progress
:class: warning
**N.B.**: At the moment local changes implemented in this way are not propagated to jobs running on condor through Dask. 
:::

## Vanilla python package
The PocketCoffea package has been published on Pypi. It can be installed with

```bash
$ pip install pocket-coffea
```

Using the singularity image is the recommened way of working with the package (on lxplus). 

## Manual installation in a Python environment

If you don't need to run on batch systems, but only locally, the package can be installed in the python environment of
your choice: **conda**, **virtualenv**, **venv**, LCG**.

1) Clone the repository in your preferred location:

```bash                   
git clone git@github.com:PocketCoffea/PocketCoffea.git
```

2) Define the python environment

   a) It can be LCG:

      ```bash
      source /cvmfs/sft.cern.ch/lcg/views/LCG_103/x86_64-centos7-gcc11-opt/setup.sh
      ```

   b) A python virtual       

      ```bash
      python -m venv --system-site-packages myenv
      ```

      The creation of the `venv` is necessary only the first time, then you can just activate it:
      
      ```bash
      source myenv/bin/activate
      ```

   c) A conda environment:
   [**micromamba**](https://mamba.readthedocs.io/en/latest/user_guide/micromamba.html) is the recommended version of *conda* to use: it is lighter and faster than a conda/miniconda environment.

      ```bash
      # Install micromamba
      curl micro.mamba.pm/install.sh | bash
      micromamba create -n pocket-coffea python=3.9
      micromamba activate pocket-coffea
      ```

3) Install the PocketCoffea package locally, so that you can also edit the package files:

    ```bash
    cd PocketCoffea
    pip install -e .
    # For developers
    pip install -e .[dev,docs]
    ```


:::{admonition} N.B.
:class: warning
Installing the package in a CMSSW environment is not recommended and it is expected to fail.
:::
