# PocketCoffea :coffee:
Generalized framework for ttH(bb) columnar-based analysis with Coffea (https://coffeateam.github.io/coffea/) on centrally produced NanoAOD samples. The study of the dilepton final state is currently implemented and an extension to the semileptonic and fully hadronic final states is foreseen in the future.
## Framework structure
The development of the code is driven by user-friendliness, reproducibility and efficiency.
All the operations on ntuples are executed within Coffea processors, defined in `workflows/`, to save the output histograms in a Coffea file.
This file is then processed through the script `scripts/plot/make_plots.py` to plot the histograms.
The workflows can be run locally or by submitting jobs on a cluster. All the commands for the execution are wrapped up in a runner script `runner.py`.
All the relevant parameters for the execution of the processor such as the input and output files' names, the execution parameters, the cuts to apply and the histogram settings are contained in a Python dictionary, defined in a configuration file in `config/`, which is passed to the runner script as an argument.
## How to run
### Execution on local machine with Futures Executor
To run the analysis workflow:
~~~
python runner.py --cfg config/test.json
~~~
### Config file
The config file in `.py` format is passed as the argument `--cfg` of the `runner.py` script. The file has the following structure:
~~~
from lib.cuts import dilepton

cfg =  {
    # Input and output files
    "workflow" : "base",
    "input"    : "datasets/baseline_samples_local.json",
    "output"   : "histograms/test.coffea",
    "plots"    : "test",

    # Executor parameters
    "executor"     : "futures",
    "workers"      : 12,
    "scaleout"     : 6,
    "chunk"        : 50000,
    "max"          : None,
    "limit"        : 1,
    "skipbadfiles" : None,

    # Cuts and variables to plot
    "cuts" : [dilepton],
    "variables" : {
        "muon_pt" : {'binning' : {'n_or_arr' : 200, 'lo' : 0, 'hi' : 2000}, 'xlim' : (0,500),  'xlabel' : "$p_{T}^{\mu}$ [GeV]"},
        "muon_eta" : None,
        "muon_phi" : None,
    },
    "variables2d" : {}
}

~~~
where the variables' names can be chosen among those reported in `parameters.allhistograms.histogram_settings` and `cuts` is a list of functions chosen from `lib.cuts` to apply the desired cuts.
### Plots
To produce plots, run the plot script:
~~~
python scripts/plot/make_plots.py --cfg config/test.json
~~~
