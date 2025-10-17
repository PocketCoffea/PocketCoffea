# Statistical Analysis

## Processes and Systematic Uncertainties

## From PocketCoffea Output to Combine Datacard

PocketCoffea provides utilities to define processes and systematic uncertainties, that can be used for statistical analyses. Furthermore it is possible to create datacards for the use with the [CMS Combine Tool](https://cms-analysis.github.io/HiggsAnalysis-CombinedLimit/latest/).

- define processes (`MCProcess`, `DataProcess`)
- define systematic uncertainties (`SystematicUncertainty`)
- create datacards (`Datacard`)

```python
from pocket_coffea.utils.stat import MCProcess, DataProcess

# example of a monte carlo process
mc_process = MCProcess(
    name="ttbar",
    samples=["TTTo2L2Nu", "TTToSemiLeptonic"],
    years=["2022_preEE", "2022_postEE"],
    is_signal=False,
    has_rateParam=True,
)

# example of a data process
data_process = DataProcess(
    name="data_obs",
    samples=["DATA_SingleMuon", "DATA_SingleElectron"],
    years=["2022_preEE", "2022_postEE"],
)
```

```python
from pocket_coffea.utils.stat import SystematicUncertainty

syst = SystematicUncertainty(
    name="syst",
    type="lnN",
    processes=["ttbar"],
    value=1.025,
    years=["2022_preEE", "2022_postEE"],
)

# or with asymmetric uncertainties
syst = SystematicUncertainty(
    name="syst",
    type="lnN",
    processes=["ttbar"],
    value=(0.995, 1.015),  # asymmetric log-normal
    years=["2022_preEE", "2022_postEE"],
)

# or with different values for different processes
syst = SystematicUncertainty(
    name="syst",
    type="lnN",
    processes={
        "ttbar": 1.025,
        "dy": (0.9, 1.1),
    },
    years=["2022_preEE", "2022_postEE"],
)
```

```python
from pocket_coffea.utils.stat import Datacard

# load coffea output
output = load("output.coffea")

# define Processes and Systematics
mc_processes = Processes(
    [
        MCProcess(name="ttbar", ...),
        MCProcess(name="dy", ...)
    ]
)

data_processes = Processes(
    [
        DataProcess(name="data_obs", ...),
    ]
)

systematics = Systematics(
    [
        SystematicUncertainty(name="syst1", ...),
        SystematicUncertainty(name="syst2", ...),
    ]
)

datacard = Datacard(
    histograms=histograms,
    datasets_metadata=output["datasets_metadata"],
    cutflow=output["cutflow"],
    years=["2022_preEE", "2022_postEE"],
    mc_processes=mc_processes,
    systematics=systematics,
    category="category_name",
    data_processes=data_processes,
)
```

The utilities can be used to create datacards for separate variables and categories. These datacards can the nbe merged into one datacard with the `combineCards.py` script. Such a script can be generated with the `combine_datacard` function.

```python
from pocket_coffea.utils.stat import combine_datacard

combine_datacards(
    datacards={
        filename: Datacard(...)
    },
    directory=output_directory,
    ...
)
```
