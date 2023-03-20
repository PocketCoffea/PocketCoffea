# Semileptonic single electron trigger SF

The current guide is a documentation of all the steps that need to be followed in order to compute the semileptonic single electron trigger SF for the $ttH(\rightarrow b\bar{b})$ analysis with Ultra Legacy (UL) datasets.

Each data-taking era needs to be processed separately by writing a config file.

## Build dataset

The first step is to build the json files containing the list of files of the UL datasets to process. In our case, we need to include the `TTToSemiLeptonic` and `TTTo2L2Nu` MC datasets and `SingleMuon` as data dataset.

A step-by-step detailed guide on the creation of the dataset file can be found at this [link](https://pocketcoffea.readthedocs.io/en/latest/examples.html).

## Compute the sum of genweights

After building the json datasets, the sum of genweights of MC datasets needs to be computed. A simple processor `genWeightsProcessor` has been [implemented](https://github.com/PocketCoffea/PocketCoffea/blob/main/pocket_coffea/workflows/genweights.py) for this purpose.

The structure of the workflow is simple: it just opens all the files, compute the sum of genweights for each dataset and save them in a dictionary in the output accumulator.

### Write config file

All the parameters specific to the analysis need to be specified in a config file that is passed as an input to the `runner.py` script.

Two examples of config files can be found in `config/genwewights/genweights_2017.py` and `config/genweights/genweights_2018.py`. The config files for the `2016_PreVFP` and `2016_PostVFP` datasets can be written using these config files as a template and modifying the era-specific fields to `2016_PreVFP` and `2016_PostVFP`, where needed.

Naming scheme for 2016 config files:

- `config/genweights/genweights_2016_PreVFP.py`
- `config/genweights/genweights_2016_PostVFP.py`

### Run genWeightsProcessor

In order to run the workflow to produce the sum of MC genweights, run the following command:

```
cd /path/to/PocketCoffea
runner.py --cfg config/genweights/genweights_2016_PreVFP.py --full
runner.py --cfg config/genweights/genweights_2016_PostVFP.py --full
```

N.B.: the argument `--full` will process all the datasets together at once and save the output in a single output file, `output_all.coffea`. Otherwise, the datasets are processed separately and an output file is saved for each dataset.

### Append sum of genweights to datasets definitions

In order to update the `datasets_definitions.json` and include the `sum_genweights` metadata in the datasets definitions, a dedicated script needs to be run with the following command:

```
cd /path/to/PocketCoffea
scripts/dataset/append_genweights.py --cfg dataset/dataset_definitions.json -i output/genweights/genweights_2016_PreVFP/output_all.coffea --overwrite
scripts/dataset/append_genweights.py --cfg dataset/dataset_definitions.json -i output/genweights/genweights_2016_PostVFP/output_all.coffea --overwrite
```

N.B.: the argument `-i output/genweights/genweights_201*/output_all.coffea` should be the path to the Coffea output file where the genweights are stored. The option `--overwrite` automatically updates the config file passed as `--cfg` argument with the genweights, in this case `dataset/dataset_definitions.json`.

### Build dataset with sum of genweights

Now the steps of the [Build dataset](#build-dataset) section need to be repeated to generate the json datasets with the additional metadata `sum_genweights`.

## Include era-dependent parameters

Since the `2016_PreVFP` and `2016_PostVFP` UL datasets have not been processed yet with this framework, a series of era-specific parameters needs to be specified in the framework:

- Add entries to the dictionary `config/semileptonic_triggerSF/parameters/eras.py` corresponding to the 2016 eras
- Check that the reference trigger (`IsoMu`) and the triggers that we want to calibrate are properly defined for the 2016 eras
	- Check that the definitions of triggers in the dictionary `pocket_coffea/parameters/triggers.py` are matching those in `AN2019-008` (pre-UL analysis note)
	- Check the HLT trigger paths available for the single-electron and single-muon triggers in MC and data for the UL datasets (there might be differences with respect to the pre-UL trigger paths)
- Check that the files related to the applied scale factors and corrections are properly specified for 2016:
	- Pileup: `pocket_coffea/parameters/pileup.py`
	- Lepton scale factors: `pocket_coffea/parameters/lepton_scale_factors.py`
	- JEC/JER: Check the dictionary `jet_factory` in the script `scripts/build_jec.py`, which is complete for 2017 and 2018 but not for 2016 datasets

## Write config file

All the parameters specific to the analysis need to be specified in a config file that is passed as an input to the `runner.py` script.

Two examples of config files can be found in `config/semileptonic_triggerSF/semileptonic_triggerSF_2017.py` and `config/semileptonic_triggerSF/semileptonic_triggerSF_2018.py`. The config files for the 2016 datasets can be written using these config files as a template and modifying the era-specific fields to `2016_PreVFP` and `2016_PostVFP`, where needed.

Naming scheme for 2016 config files:

- `config/semileptonic_triggerSF/semileptonic_triggerSF_2016_PreVFP.py`
- `config/semileptonic_triggerSF/semileptonic_triggerSF_2016_PostVFP.py`

## Run the analysis

In order to run the analysis workflow and produce the output histograms, run the following command:

```
cd /path/to/PocketCoffea
runner.py --cfg config/semileptonic_triggerSF/semileptonic_triggerSF_2016_PreVFP.py --full
runner.py --cfg config/semileptonic_triggerSF/semileptonic_triggerSF_2016_PostVFP.py --full
```

N.B.: the argument `--full` will process all the datasets together at once and save the output in a single output file, `output_all.coffea`. Otherwise, the datasets are processed separately and an output file is saved for each dataset.

## Accumulate output files

If the output of the [previous step](#run-the-analysis) has been produced without the argument `--full`, the output files need to be merged in a single output file `output_all.coffea`. If the output has been produced with the argument `--full` and the output file `output_all.coffea` is already existing, skip this step and continue with the [next one](#produce-datamc-plots).

Once the Coffea output files are produced, one needs to merge the files into a single file by using the script `accumulate_files.py` by running this command:

```
cd /path/to/output
python accumulate_files.py -i output1.coffea output2.coffea output3.coffea -o output_all.coffea
```

## Produce data/MC plots

Once the Coffea output has been accumulated, the plots can be produced by executing the plotting script:

```
cd /path/to/PocketCoffea
make_plots.py --cfg config/semileptonic_triggerSF/semileptonic_triggerSF_2016_PreVFP.py -i /path/to/output/output_all.coffea
make_plots.py --cfg config/semileptonic_triggerSF/semileptonic_triggerSF_2016_PostVFP.py -i /path/to/output/output_all.coffea
```

## Run trigger SF script

To run the script that computes the single electron trigger SF and produces the validation plots, run:

```
cd /path/to/PocketCoffea
python scripts/plot/trigger_efficiency.py --cfg config/semileptonic_triggerSF/semileptonic_triggerSF_2017.py --save_plots
```

If the additional argument `--save_plots` is not specified, only the scale factor maps are saved without saving the plots.

The output plots are saved in `/path/to/output/plots/trigger_efficiency` and `/path/to/output/plots/trigger_scalefactor`, while the 1D and 2D scale factor maps are saved in the folder specified in `workflow_options['output_triggerSF']` in the config file.
