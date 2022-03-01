# PocketCoffea :coffee:
Framework for accelerated ttH(bb) columnar analysis with Coffea (https://coffeateam.github.io/coffea/) on flat centrally produced nanoAOD samples.
## How to run
### Execution on local machine with Futures Executor
To run the preliminary version of the analysis script:
~~~
python runner.py --workflow dilepton --executor futures --samples datasets/baseline_samples_local.json --year 2017 --cfg config/test.json --output resolved_test.coffea
~~~
### Output files
The output will be stored in two files: in `histograms/test.coffea` the histograms are saved in the `.coffea` format, while in `inputs/test.h5` the arrays are saved to be used as input for the DNN.
### Config file
The histograms to store in the output file can be specified in a config file in `.json` format as the argument `--cfg` of the `runner.py` script. The file has the following structure:
~~~
{
  "variables" : [
    "muon_pt",
    "muon_eta",
    "jet_pt",
    "jet_eta"
  ],
  "variables2d" : [
    
  ]
}
~~~
where the variables' names can be chosen among those reported in `parameters.allhistograms.histogram_settings`.
### Plots
To create plots, run the plot script:
~~~
python scripts/plot/make_plots.py -i histograms/resolved_test.coffea --year 2017 --cfg config/test.json -o resolved_test
~~~
