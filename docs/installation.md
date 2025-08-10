# Installation

## Apptainer image

The best way to use the PocketCoffea package is using the prebuilt **apptainer** images. This method is particulary important if you
need to run the Dask scheduler on HTCondor.  An **apptainer** (also called singularity) image is just a virtual environment
which isolate your code in a well defined environment with the correct python packages and dependecies.  For more
information and for a comprehensive tutorial on apptainer have a look at the [HSF tutorial](https://hsf-training.github.io/hsf-training-docker/10-apptainer/index.html).

A Docker image containing a python environment with the PocketCoffea package is automatically build by the GitLab
repository CD/CI for different versions of the code. The image registry is
[here](https://gitlab.cern.ch/cms-analysis/general/PocketCoffea/container_registry/16693), and it can be used directly
with Docker.

Maintained versions:
- **main**: the main branch is the freshest code and it is installed in ***-latest** images. 
- **stable**: after more extensive tests the main branch is pushed to the stable branch and installed in ***-stable**
images (recommended). 
- **tagged**: each tagged version of the code is installed in a specific version of the image.

Docker images are created for different computing environment such as lxplus and analysis facilities.
The docker image is then **unpacked** to a Apptainer image which is available on **cvmfs**. 

:::{tip}
The apptainer images on `/cvmfs` is **the preferred way to setup** the environment, both for running user's analysis and for local development. 
:::

### Using Apptainer to run the analysis

If you are working on CERN lxplus and you will scale your processing on HTCondor, follow this installation
instructions.  The same strategy can be used in other computing environment, either with the same image or with
customized ones (please get in touch if you need a specific environment). 

The apptainer environment is activated on **lxplus** with the following command:

```bash
apptainer shell -B /afs -B /cvmfs/cms.cern.ch \
                -B /tmp  -B /eos/cms/  -B /etc/sysconfig/ngbauth-submit \
                -B ${XDG_RUNTIME_DIR}  --env KRB5CCNAME="FILE:${XDG_RUNTIME_DIR}/krb5cc" \
    /cvmfs/unpacked.cern.ch/gitlab-registry.cern.ch/cms-analysis/general/pocketcoffea:lxplus-el9-stable
```

N.B.: The command to start the apptainer image has changed when lxplus moved to the el9 machines by default. The
difference is about the handling of the kerberos ticket necessary to access to the condor scheduler. The new apptainer
shell command above setups correctly the environment.

The last part of the command contains the image version on unpacked:
**cms-analysis/general/pocketcoffea:lxplus-el9-stable**. The stable version is the recommended one to stay up-to-date
with the development without the rought edges of the main branch. 

Once inside the environment no installation is needed. The PocketCoffea scripts are globally available and the user's
analysis can be run directly. 

If a specific image is needed for a computing environment, more flavours of the docker/apptainer image can be
built. Please get in touch!

:::{tip}
You can test if the environment has been properly setup by running the `pocket-coffea` CLI. 

```bash
$> pocket-coffea 
pocket-coffea 

    ____             __        __  ______      ________
   / __ \____  _____/ /_____  / /_/ ____/___  / __/ __/__  ____ _
  / /_/ / __ \/ ___/ //_/ _ \/ __/ /   / __ \/ /_/ /_/ _ \/ __ `/
 / ____/ /_/ / /__/ ,< /  __/ /_/ /___/ /_/ / __/ __/  __/ /_/ /
/_/    \____/\___/_/|_|\___/\__/\____/\____/_/ /_/  \___/\__,_/


Running PocketCoffea version 0.9.6
- Documentation page:  https://pocketcoffea.readthedocs.io/
- Repository:          https://github.com/PocketCoffea/PocketCoffea

Run with --help option for the list of available commands 
```
:::

### Using Apptainer for local development

If the user needs to modify locally the central PocketCoffea code, the apptainer image can still be used as a baseline
(for dependencies), but a local installation of the package is needed. Follow the instructions:


```bash
# Clone locally the PocketCoffea repo
git clone git@github.com:PocketCoffea/PocketCoffea.git
cd PocketCoffea

#Enter the Singularity image
apptainer shell --bind /afs -B /cvmfs/cms.cern.ch \
         --bind /tmp  --bind /eos/cms/ -B /etc/sysconfig/ngbauth-submit \
         -B ${XDG_RUNTIME_DIR}  --env KRB5CCNAME="FILE:${XDG_RUNTIME_DIR}/krb5cc"  \
         /cvmfs/unpacked.cern.ch/gitlab-registry.cern.ch/cms-analysis/general/pocketcoffea:lxplus-el9-stable


# Create a local virtual environment using the packages defined in the apptainer image
python -m venv --system-site-packages myenv

# Activate the environment
source myenv/bin/activate

# Install in EDITABLE mode
pip install -e .[dev]

# Set the PYTHONPATH to make sure the editable PocketCoffea installation is picked up
export PYTHONPATH=`pwd`

# One could also install additional packages if necessary for anaysis, eg:
pip install lightgbm 
```

The next time the user enters in the apptainer the virtual environment needs to be activated and the PYTHONPATH needs to be set. 
```bash
#Enter the image
apptainer shell  -B /afs -B /cvmfs/cms.cern.ch -B /tmp  -B /eos/cms/  \
                 -B /etc/sysconfig/ngbauth-submit  \
                 -B ${XDG_RUNTIME_DIR}  --env KRB5CCNAME="FILE:${XDG_RUNTIME_DIR}/krb5cc" \
                 /cvmfs/unpacked.cern.ch/gitlab-registry.cern.ch/cms-analysis/general/pocketcoffea:lxplus-el9-stable

# Activate the virtual environment
cd PocketCoffea
source myenv/bin/activate
export PYTHONPATH=`pwd`
```


:::{admonition} Setup the job submission with local core changes
:class: warning
**N.B.**: In order to properly propagated the local environment and local code changes to jobs running on condor through
Dask/condor, the user needs to setup the executor option `local-virtualenv: true` or pass `--local-virtualenv` to the runner command.
Checkout the [running instructions](https://pocketcoffea.readthedocs.io/en/stable/running.html) for more details.  
:::

### SWAN
It is also possible to setup PocketCoffea within SWAN at CERN. See [Instructions](https://github.com/PocketCoffea/Tutorials/tree/main/Analysis_Facilities_Setup#cern-swan-analysis-facility) in a separate tutorial.

## Vanilla python package
The PocketCoffea package has been published on Pypi. It can be installed with

```bash
$ pip install pocket-coffea
```

Using the apptainer image is the recommened way of working with the package (on lxplus). 

## Manual installation in a Python environment

If you don't need to run on batch systems, but only locally, the package can be installed in the python environment of
your choice: **conda**, **virtualenv**, **venv**, **LCG**.

1) Clone the repository in your preferred location:

```bash                   
git clone git@github.com:PocketCoffea/PocketCoffea.git
```

2) Now define the python environment using **alternatively**  a virtual env, or conda:

   a) The user can use a LCG environment as a base (to avoid having to download many packages)  with a virtual environment on top:

      ```bash
      source /cvmfs/sft.cern.ch/lcg/views/LCG_103/x86_64-centos7-gcc11-opt/setup.sh
      python -m venv --system-site-packages myenv
      source myenv/bin/activate
      ```

      The creation of the `venv` is necessary only the first time, then you can just activate it:
      
      ```bash
      source /cvmfs/sft.cern.ch/lcg/views/LCG_103/x86_64-centos7-gcc11-opt/setup.sh
      source myenv/bin/activate
      ```

   c) A conda environment:
   [**micromamba**](https://mamba.readthedocs.io/en/latest/user_guide/micromamba.html) is the recommended version of *conda* to use: it is lighter and faster than a conda/miniconda environment.

      ```bash
      # Install micromamba
      "${SHELL}" <(curl -L micro.mamba.pm/install.sh)
      micromamba create -n pocket-coffea python=3.11 -c conda-forge
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
