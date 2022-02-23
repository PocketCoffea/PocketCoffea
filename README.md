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
