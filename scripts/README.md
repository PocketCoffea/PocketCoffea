# PocketCoffea scripts

The current guide is a documentation of how to run the scripts of the PocketCoffea package.

## Merge skimmed files with hadd

In order to merge the skimmed n-tuples, the `hadd` command from ROOT needs to be used.
To source ROOT inside the `pocket_coffea` environment, run the following command:

```
source /cvmfs/sft.cern.ch/lcg/views/LCG_102/x86_64-centos7-gcc11-opt/setup.sh
```

To run the `hadd` script run the following command:

```
cd /path/to/PocketCoffea
python scripts/hadd_skimmed_files.py -fl /path/to/output/output_all.coffea -o /path/to/outputdir
```

## Build dataset

The first step is to build the json files containing the list of files of the UL datasets to process. In our case, we need to include the `TTToSemiLeptonic` and `TTTo2L2Nu` MC datasets and `SingleMuon` as data dataset.

A step-by-step detailed guide on the creation of the dataset file can be found at this [link](https://pocketcoffea.readthedocs.io/en/latest/examples.html).

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

## Run the analysis

In order to run the analysis workflow and produce the output histograms, run the following command:

```
cd /path/to/PocketCoffea
runner.py --cfg /path/to/config/config.py --full
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

To produce plots from a Coffea output file execute the plotting script:

```
cd /path/to/PocketCoffea
make_plots.py --cfg /path/to/config/config.py -i /path/to/output/output_all.coffea
```

## Run trigger SF script

To run the script that computes the single electron trigger SF and produces the validation plots, run:

```
cd /path/to/PocketCoffea
python scripts/plot/trigger_efficiency.py --cfg config/semileptonic_triggerSF/semileptonic_triggerSF_2017.py --save_plots
```

If the additional argument `--save_plots` is not specified, only the scale factor maps are saved without saving the plots.

The output plots are saved in `/path/to/output/plots/trigger_efficiency` and `/path/to/output/plots/trigger_scalefactor`, while the 1D and 2D scale factor maps are saved in the folder specified in `workflow_options['output_triggerSF']` in the config file.
