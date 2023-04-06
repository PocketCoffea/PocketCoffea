Full analysis example
########################

A full example of all the steps needed to run a full analysis with PocketCoffea is reported, starting from the creation of the datasets list, the customization of parameters and the production of the final shapes and plots.
As an example, a simplified version of the Drell-Yan analysis targeting the Z->mumu channel is implemented.
The main steps that need to be performed are the following:

* Build the json datasets
* Compute the sum of genweights for Monte Carlo datasets and rebuild json datasets
* Define selections: trigger, skim, object preselection, event preselection and categorization
* Define weights and variations
* Define histograms
* Run the processor
* Make the plots from processor output

Setup PocketCoffea
================

The code of PocketCoffea run in this example is referring to a dedicated `branch <https://github.com/mmarchegiani/PocketCoffea/tree/analysis_example/>`_. Move to the ``analysis_example`` branch of PocketCoffea:

.. code-block:: bash

	cd path/to/PocketCoffea
	git checkout -b analysis_example
	git pull $REMOTE analysis_example

Configuration file
================

The parameters specific to the analysis have to be specified in the configuration file. This file contains a pure python dictionary named ``cfg`` that is read and manipulated by the ``Configurator`` module.
A dedicated `repository <https://github.com/PocketCoffea/AnalysisConfigs>`_ is setup to collect the config files from different analysis. Clone the repository and install it as an editable package in the ``pocket-coffea`` environment:

.. code-block:: bash

	git clone https://github.com/PocketCoffea/AnalysisConfigs
	pip install -e .

The repository contains a pre-existing config file ``configs/base.py`` that can be used as a template to write the custom config file for our analysis.
Create a dedicated folder ``zmumu`` under the ``configs`` folder. This folder will contain all the config files, the datasets definition json file, the workflows files and possibly extra files with parameters that are needed for the analysis:

.. code-block:: bash

	mkdir configs/zmumu
	mkdir configs/zmumu/datasets
	touch configs/zmumu/datasets/datasets_definitions.json
	cp configs/base.py configs/zmumu/config.py

Now the ``configs/zmumu/config.py`` file is ready to be edited and customized for our analysis.


Build dataset
================

The datasets include a Drell-Yan Monte Carlo dataset and a ``SingleMuon`` dataset. We have to look for the corresponding `DAS <https://cmsweb.cern.ch/das/>`_ keys:

::

	/DYJetsToLL_M-50_TuneCP5_13TeV-amcatnloFXFX-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v2/NANOAODSIM
	/SingleMuon/Run2018C-UL2018_MiniAODv2_NanoAODv9-v2/NANOAOD

The list of datasets has to be written in a structured dictionary together with the corresponding metadata in a json file. This json file is then read by the ``build_dataset.py`` script to produce the actual json datasets that are passed as input to the Coffea processor. The steps are the following:

1. Create a json file that contains the required datasets, ``dataset_definitions.json``.
Each entry of the dictionary corresponds to a dataset. Datasets include a list of DAS keys, the output json dataset path and the metadata. In addition a label ``sample`` is specified to group datasets under the same sample (e.g. group QCD datasets from different HT bins in a single sample).
The general idea is the following:

* The **dataset** key uniquely identifies a dataset.
* The **sample** key is the one used internally in the framework to fill histograms with events from the same sample.
* The names of the data samples should all start with the same prefix (e.g. ``DATA_``).
* The **json_output** key defines the path of the destination json file which will store the list of files.
* Several **files** entries can be defined for each dataset, including a list of DAS names and a dedicated metadata dictionary.
* The **metadata** keys should include:
	* For **Monte Carlo**: ``year``, ``isMC`` and ``xsec``.
	* For **Data**: ``year``, ``isMC``, ``era`` and ``primaryDataset``.

When the json datasets are built, the metadata parameters are linked to the files list, defining a unique dataset entry with the corresponding files.
The `primaryDataset` key for Data datasets is needed in order to apply a trigger selection only to the corresponding dataset (e.g. apply the `SingleMuon` trigger only to datasets having `primaryDataset=SingleMuon`).
The structure of the ``datasets_definitions.json`` file after filling in the dictionary with the parameters relevant to our Drell-Yan and SingleMuon datasets should be the following:

.. code-block:: json

   "DYJetsToLL_M-50":{
        "sample": "DYJetsToLL",
        "json_output"    : "datasets/DYJetsToLL_M-50.json",
        "files":[
            { "das_names": ["/DYJetsToLL_M-50_TuneCP5_13TeV-amcatnloFXFX-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v2/NANOAODSIM"],
              "metadata": {
                  "year":"2018",
                  "isMC": true,
		          "xsec": 6077.22,
                  }
            }
        ]
  },
    "DATA_SingleMuon": {
        "sample": "DATA_SingleMuonC",
        "json_output": "datasets/DATA_SingleMuonC.json",
        "files": [
            
            {
                "das_names": [
                    "/SingleMuon/Run2018C-UL2018_MiniAODv2_NanoAODv9-v2/NANOAOD"
                ],
                "metadata": {
                    "year": "2018",
                    "isMC": false,
                    "primaryDataset": "SingleMuon",
                    "era": "C"
                },
                "das_parents_names": [
                    "/SingleMuon/Run2018C-UL2018_MiniAODv2-v2/MINIAOD"
                ]
            }
        ]
    }
    
2. To produce the json files containing the file lists, run the following command:

.. code-block:: bash

	cd zmumu
	build_dataset.py --cfg datasets/dataset_definitions.json

Four ``json`` files are produced as output, two for each dataset: a version includes file paths with a specific prefix corresponding to a site (corresponding to the site that is currently available, e.g. ``dcache-cms-xrootd.desy.de:1094``) while another has a global redirector prefix (e.g. ``xrootd-cms.infn.it``), and is named with the suffix `_redirector.json`
If one has to rebuild the dataset to include more datasets, the extra argument ``--overwrite`` can be provided to the script.

.. code-block:: bash

	ls zmumu/datasets
	datasets_definitions.json DATA_SingleMuonC.json DATA_SingleMuonC_redirector.json DYJetsToLL_M-50.json DYJetsToLL_M-50_redirector.json


Compute the sum of genweights
================

The sum of the genweights of Monte Carlo samples needs to be computed in order to properly normalize Monte Carlo datasets.
To compute the sum of genweights, we need to run a dedicated Coffea processor, ``genWeightsProcessor``, that just opens all the files, reads the genweight.

1. Take the `genWeight.py` configuration file, modify the ``samples`` in ``dataset`` dict
2. Run ``runner.py --cfg configs/$dir/genweights.py`` to get the coffea file contains genweight info. 
3. Embed the ``sum_genweight`` info to ``sample_defintion.json``  

.. code-block:: python

	python ../PocketCoffea/scripts/dataset/append_genweights.py --cfg configs/zmumu/datasets/datasets_definitions_zmm.json -i output/genweights/genweights_2018/output_all.coffea  --overwrite

4. Run ``build_dataset.py --cfg dataset_definitions.json`` again to embed the info


Define selections
================

The selections are performed at two levels:
* Object preselection: selecting the "good" objects that will be used in the final analysis (e.g. `JetGood`, `MuonGood`, `ElectronGood`...). These selections include the detector acceptance cuts, the object identification working points, the muon isolation, the b-tagging working point, etc.
* Event selection: selections on the events that enter the final analysis, done in three steps:
   * Skim: loose cut on the events. The following steps of the analysis are performed only on the events passing the skim selection.
   * Preselection: baseline selection for the analysis.
   * Categorization: selection to split the events passing the event preselection into different categories.


Define weights and variations
================


Define histograms
================
Wrapped in the ``variable`` dictionary under ``config.py``.

- Create custom histogram with ``key:$HistConf_obj`` , create `Axis` in a list (1 element for 1D-hist, 2 elements for 2D-hist)


.. code-block:: python
   
   "variables":
       {
           # 1D plots
           "mll" : HistConf( [Axis(coll="ll", field="mass", bins=100, start=50, stop=150, label=r"$M_{\ell\ell}$ [GeV]")] 
    	}
	
	# coll : collection/objects under events
	# field: fields under collections
	# bins, start, stop: # bins, axis-min, axis-max
	# label: axis label name
.. _hist: http://cnn.com/ https://github.com/PocketCoffea/PocketCoffea/blob/main/pocket_coffea/parameters/histograms.py	

- There are some predefined `hist`_. 

.. code-block:: python

	"variables":
       {
        **count_hist(name="nJets", coll="JetGood",bins=8, start=0, stop=8),
	# Muon kinematics
	**muon_hists(coll="MuonGood", pos=0),
	# Jet kinematics
        **jet_hists(coll="JetGood", pos=0),
    	}

	
Run the processor
================



Produce plots
================