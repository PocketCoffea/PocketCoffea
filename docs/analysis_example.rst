Full analysis example
########################

A full example of all the steps needed to run a full analysis with PocketCoffea is reported, starting from the creation of the datasets list, the customization of parameters and the production of the final shapes and plots.
As an example, a toy example version of the Drell-Yan analysis targeting the Z->mumu channel is implemented.
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

1) Create a json file that contains the required datasets, ``dataset_definitions.json``. Each entry of the dictionary corresponds to a dataset. Datasets include a list of DAS keys, the output json dataset path and the metadata. In addition a label ``sample`` is specified to group datasets under the same sample (e.g. group QCD datasets from different HT bins in a single sample).

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

2) To produce the json files containing the file lists, run the following command:

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

The sum of the genweights of Monte Carlo datasets needs to be computed in order to properly normalize Monte Carlo datasets.
To compute the sum of genweights, we need to run a dedicated Coffea processor, ``genWeightsProcessor``, that just opens all the files, reads the genweight of each event and stores their sum in a dictionary in the output file.

1) Copy the config and workflows file for the genweights from PocketCoffea and modify the ``samples`` in the ``dataset`` dictionary with the names of our samples:

.. code-block:: bash

   cp PocketCoffea/config/genweights/genweights_2018.py zmumu/genweights_2018.py

2) Run the ``genWeightsProcessor`` to get the coffea output containing the sum of genweights:

.. code-block:: bash

   runner.py --cfg zmumu/genweights.py --full

3) Append the ``sum_genweights`` metadata to ``datasets_definitions.json`` using the ``append_genweights.py`` script:

.. code-block:: python

	python ../PocketCoffea/scripts/dataset/append_genweights.py --cfg configs/zmumu/datasets/datasets_definitions.json -i output/genweights/genweights_2018/output_all.coffea --overwrite

4) Run the ``build_dataset.py`` script again to produced the new json datasets updated with the ``sum_genweights`` metadata:

.. code-block:: python build_dataset.py --cfg datasets_definitions.json --overwrite

Now the json datasets contain all the necessary information to run the full analysis.


Define selections
================

The selections are performed at two levels:

* Object preselection: selecting the "good" objects that will be used in the final analysis (e.g. `JetGood`, `MuonGood`, `ElectronGood`...).
* Event selection: selections on the events that enter the final analysis, done in three steps:

   1. Skim and trigger: loose cut on the events and trigger requirements.
   2. Preselection: baseline event selection for the analysis.
   3. Categorization: selection to split the events passing the event preselection into different categories (e.g. signal region, control region).

Object preselection
----------------

To select the objects entering the final analysis, we need to specify a series of cut parameters for the leptons and jets in the file ``PocketCoffea/pocket_coffea/parameters/object_preselection.py``. These selections include the pT, eta acceptance cuts, the object identification working points, the muon isolation, the b-tagging working point, etc.
For the Z->mumu analysis, we just use the standard definitions for the muon, electron and jet objects, that we include as a dictionary under the key ``dimuon``:

.. code-block:: python

   object_preselection = {
      "dimuon": {
         "Muon": {
               "pt": 15,
               "eta": 2.4,
               "iso": 0.25, #PFIsoLoose
               "id": "tightId",
         },
         "Electron": {
               "pt": 15,
               "eta": 2.4,
               "iso": 0.06,
               "id": "mvaFall17V2Iso_WP80",
         },
         "Jet": {
               "dr": 0.4,
               "pt": 30,
               "eta": 2.4,
               "jetId": 2,
               "puId": {"wp": "L", "value": 4, "maxpt": 50.0},
         },
   ...

The ``finalstate`` label has to be changed to ``dimuon`` such that the processor can query the corresponding parameters for the object preselection defined above:

.. code-block:: python

   cfg = {
    ...
    "finalstate" : "dimuon",
    ...
   }


Event selection
----------------

In PocketCoffea, the event selections are implemented with a dedicated `Cut` object, that stores both the information of the cutting function and its input parameters.
Several factory ``Cut`` objects are available in ``pocket_coffea.lib.cut_functions``, otherwise the user can define its custom ``Cut`` objects.


Skim
~~~~~~~~~~~~~~~~~~~~~

The skim selection of the events is performed "on the fly" to reduce the number of processed events. At this stage we also apply the HLT trigger requirements required by the analysis.
The following steps of the analysis are performed only on the events passing the skim selection, while the others are discarded from the branch ``events``, therefore reducing the computational load on the processor.
In the config file, we specify two skim cuts: one is selecting events with at least one 15 GeV muon and the second is requiring the HLT ``SingleMuon`` path.
In the preamble of ``config.py``, we define our custom trigger dictionary, which we pass as an argument to the factory function ``get_HLTsel()``:

.. code-block:: python

   trigger_dict = {
      "2018": {
         "SingleEle": [
               "Ele32_WPTight_Gsf",
               "Ele28_eta2p1_WPTight_Gsf_HT150",
         ],
         "SingleMuon": [
               "IsoMu24",
         ],
      },
   }

   cfg = {
    ...
    "skim": [get_nObj_min(1, 15., "Muon"),
             get_HLTsel("dimuon", trigger_dict, primaryDatasets=["SingleMuon"])],
    ...
   }


Event preselection
~~~~~~~~~~~~~~~~~~~~~

In the Z->mumu analysis, we want to select events with exactly two muons with opposite charge. In addition, we require a cut on the leading muon pT and on the dilepton invariant mass, to select the Z boson mass window.
The parameters are directly passed to the constructor of the ``Cut`` object as the dictionary ``params``. We can define the function ``dimuon`` and the ``Cut`` object ``dimuon_presel`` in the preamble of config:

.. code-block:: python

   def dimuon(events, params, year, sample, **kwargs):

      # Masks for same-flavor (SF) and opposite-sign (OS)
      SF = ((events.nMuonGood == 2) & (events.nElectronGood == 0))
      OS = events.ll.charge == 0

      mask = (
         (events.nLeptonGood == 2)
         & (ak.firsts(events.MuonGood.pt) > params["pt_leading_muon"])
         & OS & SF
         & (events.ll.mass > params["mll"]["low"])
         & (events.ll.mass < params["mll"]["high"])
      )

      # Pad None values with False
      return ak.where(ak.is_none(mask), False, mask)

   dimuon_presel = Cut(
      name="dilepton",
      params={
         "pt_leading_muon": 25,
         "mll": {'low': 25, 'high': 2000},
      },
      function=dimuon,
   )

In a scenario of an analysis requiring several different cuts, a dedicated library of cuts and functions can be defined in a separate file and imported in the config file.
The ``preselections`` field in the config file is updated accordingly:

.. code-block:: python


   cfg = {
      ...
      "preselections" : [dimuon_presel],
      ...
   }


Categorization
~~~~~~~~~~~~~~~~~~~~~

In the toy Z->mumu analysis, no further categorization of the events is performed. Only a ``baseline`` category is defined with the ``passthrough`` factory cut that is just passing the events through without any further selection:

.. code-block:: python

   cfg = {
      ...
      # Cuts and plots settings
      "finalstate" : "dimuon",
      "skim": [get_nObj_min(1, 15., "Muon"),
               get_HLTsel("dimuon", trigger_dict, primaryDatasets=["SingleMuon"])],
      "preselections" : [dimuon_presel],
      "categories": {
         "baseline": [passthrough],
      },
      ...
   }

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