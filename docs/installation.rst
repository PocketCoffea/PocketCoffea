Installation
============

The installation of the PocketCoffea package is very simple and based only on the `python` enviroment tools.

Only local processing (no job submission)
-----------------------------------------

If you don't need to run on batch systems, but only locally, the package can be installed in the python environment of
your choice: **conda**, **virtualenv**, **venv**, LCG**.

1) Clone the repository in your preferred location:

   .. code-block:: bash
                   
      git clone git@github.com:PocketCoffea/PocketCoffea.git

2) Define the python environment

   a) It can be LCG:
        .. code-block:: bash
                         
         source /cvmfs/sft.cern.ch/lcg/views/LCG_102/x86_64-centos7-gcc11-opt/setup.sh

   b) A python virtual       
       .. code-block:: bash

           python -m venv --system-site-packages myenv

      The creation of the `venv` is necessary only the first time, then you can just activate it:
       .. code-block:: bash

            source myenv/bin/activate

   c) A conda environment:
       .. code-block:: bash

            conda env create --name pocket-coffea python=3.8
            conda activate pocket-coffea

3) Install the PocketCoffea package locally, so that you can also edit the package files:
    .. code-block:: bash

            pip install -e .[dev]

   

**N.B.: The CMSSW environment is not compatible with the package.**


Job submissions: CERN lxplus
----------------------------
If you are working on CERN lxplus and you will scale your processing on HTCondor, follow this installation
instructions.

The best way to use the package on lxplus is by using **singularity** images. This method is particulary important if
you need to run the Dask scheduler on HTCondor.
A **singularity** (also called apptainer) image is just a virtual environment which isolate your code in a well defined
environment with the correct python packages and versions.
For more information and for a comprehensive tutorial on singularity have a look at the `HSF tutorial <https://hsf-training.github.io/hsf-training-docker/10-singularity/index.html>`_ .


1) Clone the repository in your preferred location and move into it:
     .. code-block:: bash
                   
         git clone git@github.com:PocketCoffea/PocketCoffea.git
         cd PocketCoffea

2) To activate the singularity shell in lxplus just run:
     .. code-block:: bash
                   
         singularity shell -B /afs -B /tmp/ -B /cvmfs/cms.cern.ch --env KRB5CCNAME=$KRB5CCNAME \
         /cvmfs/unpacked.cern.ch/gitlab-registry.cern.ch/batch-team/dask-lxplus/lxdask-cc7:latest

3) Now inside the singularity image we create a minimal virtualenv to keep the PocketCoffea package and its dependencies
   (be aware that this virtualenv just works inside the singularity image). N.B. This steps is needed **only the first
   time** :
     .. code-block:: bash
                   
         python -m venv --system-site-packages myenv

4) Activate the `venv`, (needed all the times)
     .. code-block:: bash

        source myenv/bin/activate

5) Now we can install the PocketCoffea package locally, so that you can also edit the package files:
     .. code-block:: bash
                   
         pip install -e .[dev]

