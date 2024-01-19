# Installation

The installation of the PocketCoffea package is very simple and based only on the `python` enviroment tools.

## CERN lxplus with jobs submission
----------------------------
If you are working on CERN lxplus and you will scale your processing on HTCondor, follow this installation
instructions.

The best way to use the package on lxplus is by using **apptainer** images. This method is particulary important if you
need to run the Dask scheduler on HTCondor.  A **apptainer** (also called apptainer) image is just a virtual environment
which isolate your code in a well defined environment with the correct python packages and versions.  For more
information and for a comprehensive tutorial on apptainer have a look at the [HSF
tutorial](https://hsf-training.github.io/hsf-training-docker/10-apptainer/index.html).

A Docker image containing a python environment with the PocketCoffea package is automatically build by the GitLab
repository CD/CI for different versions of the code. The image registry is
[here](https://gitlab.cern.ch/cms-analysis/general/PocketCoffea/container_registry/16693), and it can be used directly
with Docker.

- **main**: the main branch is the freshest code and it is installed in ***-latest** images. 
- **stable**: after more extensive tests the main branch is pushed to the stable branch and installed in ***-stable**
images (recommended). 
- **tagged**: each tagged version of the code is installed in a specific version of the image.

Docker images are created for different computing environment such as lxplus and analysis facilities.. 

The docker image is then **unpacked** to a Apptainer image which is available on **cvmfs**. 

:::{tip}
The apptainer image is **the preferred way to setup** the environment, both for running user's analysis and for local development. 
:::

### Using Apptainer to run the analysis

The apptainer environment is activated on **lxplus** with the following command:

```bash
apptainer shell --bind /afs -B /cvmfs/cms.cern.ch \
                --bind /tmp  --bind /eos/cms/ \
    --env KRB5CCNAME=$KRB5CCNAME --bind /etc/sysconfig/ngbauth-submit  \
    /cvmfs/unpacked.cern.ch/gitlab-registry.cern.ch/cms-analysis/general/pocketcoffea:lxplus-cc7-stable
```

The last part of the command contains the image version on unpacked:
**cms-analysis/general/pocketcoffea:lxplus-cc7-stable**. The stable version is the recommended one to stay up-to-date
with the development without the rought edges of the main branch. 

Once inside the environment no installation is needed. The PocketCoffea scripts are globally available and the user's
analysis can be run directly. 

If a specific image is needed for a computing environment, more flavours of the docker/apptainer image can be
built. Please get in touch!

### Using Apptainer for local development

If the user needs to modify locally the central PocketCoffea code, the apptainer image can still be used as a baseline
(for dependencies), but a local
installation of the package is needed. Follow the instructions: 


```bash
#Enter the image
apptainer shell --bind /afs -B /cvmfs/cms.cern.ch \
         --bind /tmp  --bind /eos/cms/ \
         --env KRB5CCNAME=$KRB5CCNAME --bind /etc/sysconfig/ngbauth-submit  \
         /cvmfs/unpacked.cern.ch/gitlab-registry.cern.ch/cms-analysis/general/pocketcoffea:lxplus-cc7-stable

# Create a local virtual environment using the packages defined in the apptainer image
python -m venv --system-site-packages myenv

# Activate the environment
source myenv/bin/activate

# Clone locally the PocketCoffea repo
git clone git@github.com:PocketCoffea/PocketCoffea.git
cd PocketCoffea

# Install in EDITABLE mode
pip install -e .[dev]
```

The next time the user enters in the apptainer the virtual environment needs to be activated. 

:::{admonition} Work in progress
:class: warning
**N.B.**: At the moment local changes implemented in this way are not propagated to jobs running on condor through Dask. 
:::

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
      curl micro.mamba.pm/install.sh | bash
      micromamba create -n pocket-coffea python=3.9 -c conda-forge
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
