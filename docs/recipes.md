# HOW-TOs for common tasks
:::{alert}
Page under construction! Come back for more common analysis steps recipes.
:::

## HLT trigger selection

HLT triggers are applied as a **skim cut**, before any object correction. The list of
triggers is declared once in the parameters under the `HLT_triggers` key, organized by
data-taking period and *primary dataset*:

```yaml
HLT_triggers:
  "2018":
    SingleEle:
        - Ele32_WPTight_Gsf
        - Ele28_eta2p1_WPTight_Gsf_HT150
    SingleMuon:
        - IsoMu24
```

The selection itself is built with the `get_HLTsel` factory from
`pocket_coffea.lib.cut_functions` and added to the `skim` list of the `Configurator`:

```python
from pocket_coffea.lib.cut_functions import get_HLTsel

skim = [
    get_nPVgood(1), eventFlags, goldenJson,
    get_HLTsel(),                                          # OR of all primary datasets
],
```

Behavior of `get_HLTsel(primaryDatasets=None, invert=False)`:

- **MC**: the OR of all triggers in the configuration is applied.
- **Data**: only the triggers of the primary dataset matching the sample are applied.
- `primaryDatasets=[...]`: restrict to the given primary datasets, **both for Data and
  MC, overriding the automatic behavior**. This is the way to remove the primary-dataset
  overlap in data:

  ```python
  skim = [get_HLTsel(primaryDatasets=["SingleMuon", "SingleEle"])]
  ```

- `invert=True`: keep events *failing* the selection. Useful for cross-cleaning one
  primary dataset against another (see [Primary dataset cross-cleaning](#primary-dataset-cross-cleaning)).

To apply a fixed list of trigger paths irrespective of the parameters file, use
`get_HLTsel_custom(["HLT_IsoMu24", ...])`.

:::{warning}
Trigger cuts read raw NanoAOD branches and therefore belong in `skim`, never in
`preselections` — object corrections happen *after* the skim, inside the calibration loop.
:::

## Define a new cut function

A selection is a `Cut` object (`pocket_coffea.lib.cut_definition.Cut`) wrapping a function
with the signature `function(events, params, year, sample, **kwargs)` that returns a
boolean awkward array (one entry per event). The `params` dictionary is attached to the
`Cut` and passed through at evaluation time, so the same function can be reused with
different thresholds.

```python
import awkward as ak
from pocket_coffea.lib.cut_definition import Cut

def min_jet_function(events, params, year, sample, **kwargs):
    mask = ak.num(events.JetGood) >= params["njet"]
    return mask

min_2jets = Cut(
    name="min_2jets",
    params={"njet": 2},
    function=min_jet_function,
)
```

For simple, one-off selections an inline `lambda` is enough:

```python
even_event = Cut(
    name="even_event",
    function=lambda events, params, **kwargs: events.event % 2 == 0,
    params={},
)
```

### Parametrized factory cuts

The recommended pattern for reusable cuts is a *factory* that returns a configured `Cut`,
so the threshold becomes part of the cut name (and therefore of the output keys). Several
ready-made factories live in `pocket_coffea.lib.cut_functions`:

```python
from pocket_coffea.lib.cut_functions import get_nObj_min, get_nObj_eq, get_nObj_less

get_nObj_min(4, minpt=30., coll="JetGood")   # >= 4 jets with pt > 30
get_nObj_eq(2, coll="BJetGood")              # exactly 2 b-jets
get_nObj_less(3, coll="MuonGood")            # < 3 muons
```

Your own factories follow the same shape:

```python
def get_min_jets(njet, name=None):
    if name is None:
        name = f"min_{njet}_jets"
    return Cut(name=name, params={"njet": njet}, function=min_jet_function)
```

:::{note}
If your cut needs a variable that is not in NanoAOD, compute it in
`define_common_variables_before_presel` in the workflow so it is available when the
preselection runs.
:::

## Categories: standard and Cartesian selections

Categories are the orthogonal regions in which histograms, columns and weights are filled.
The simplest form is a dictionary mapping a category name to a list of `Cut`s (ANDed
together):

```python
from pocket_coffea.parameters.cuts import passthrough

categories = {
    "baseline": [passthrough],
    "1btag":    [get_nObj_min(1, coll="BJetGood")],
    "2btag":    [get_nObj_min(2, coll="BJetGood")],
}
```

When you need the full product of several independent cut sets (e.g. jet multiplicity ×
b-jet multiplicity), use `CartesianSelection` with one `MultiCut` per axis. It avoids
spelling out every combination by hand:

```python
from pocket_coffea.lib.categorization import CartesianSelection, MultiCut

categories = CartesianSelection(
    multicuts = [
        MultiCut(name="Njets",
                 cuts=[get_nObj_eq(1, 30., "JetGood"),
                       get_nObj_eq(2, 30., "JetGood"),
                       get_nObj_min(3, 30., "JetGood")],
                 cuts_names=["1j", "2j", "3j"]),
        MultiCut(name="Nbjet",
                 cuts=[get_nObj_eq(0, 15., "BJetGood"),
                       get_nObj_eq(1, 15., "BJetGood"),
                       get_nObj_min(2, coll="BJetGood")],
                 cuts_names=["0b", "1b", "2b"]),
    ],
    # categories that are NOT part of the cartesian product
    common_cats = {
        "inclusive":   [passthrough],
        "4jets_40pt" : [get_nObj_min(4, 40., "JetGood")],
    }
)
```

This produces the cross-product categories `1j_0b`, `1j_1b`, …, `3j_2b` (the names are the
`cuts_names` joined with `_`), plus the `common_cats` entries `inclusive` and `4jets_40pt`.
`CartesianSelection` is far more memory-efficient than declaring every combination as a
`StandardSelection`, because the per-axis masks are computed once and combined on the fly.

## Skimming events
Skimming NanoAOD events and save the reduced files on disk can speedup a lot the processing of the analysis. The recommended executor for the skimming process is the direct condor-job executor, which splits the workload in condor jobs without using the dask scheduler. This makes the resubmission of failed skim jobs easier. 

Follow these instructions to skim the files on EOS:
1. Add the `save_skimmed_files` argument to the configurator with a suitable folder name: e.g. `  save_skimmed_files = "root://eoscms.cern.ch//eos/cms/store/group/phys_higgs/ttHbb/Run3_semileptonic_skim/"`

2. It is recommended to run the processing on HTCondor at CERN using the new direct condor executor. That will send out standard jobs instead of using dask. Please make sure your dataset list is up-to-date before sending the jobs. 
   ```pocket-coffea run --cfg config_skim.py  -o output_skim_config -e condor@lxplus --scaleout NUMBEROFJOBS --chunksize 200000 --job-dir jobs --job-name skim --queue workday --dry-run``` . Use the `--dry-run` option to check the job splitting configuration and remove it when you are happy to submit the jobs.

3. Check the status of the jobs with `pocket-coffea check-jobs -j jobs-dir/skim`.  Optionally activate the automatic resubmitting option to resubmit failed jobs. 

4. Merge the per-job `.coffea` outputs into a single file with `merge-outputs`:
   ```
   merge-outputs output_skim_config/output_job_*.coffea -o output_total.coffea
   ```
   This step is **mandatory**, not optional: each skim job writes a `.coffea` file
   carrying the cutflow and, crucially, the `sum_genweights` of the chunks it processed.
   Merging accumulates these correctly across all jobs — including the chunks that skimmed
   **0 events** (an event-less chunk still contributes to the generator-weight sum used for
   the cross-section normalization downstream). The skimmed ROOT files alone do not carry
   this information, so without the merge the normalization of the skimmed dataset would be
   wrong. The resulting `output_total.coffea` is the input to the next step.
   :::{warning}
   Merge the **raw** `output_job_*.coffea` files in a single `merge-outputs` call. Re-merging
   an already-merged file double-rescales the histograms (the `sum_genweights` is not reset
   between passes), so never feed a merged output back into `merge-outputs`.
   :::

5. Once done, we usually do an hadd to sum all the small files produced by each saved chunk. An utility script to compute the groups and correctly hadd them is available `pocket-coffea hadd-skimmed-files -fl ../output_total.coffea -o root://eoscms.cern.ch//eos/cms/store/group/phys_higgs/ttHbb/Run3_dileptonic_skim_hadd -e 400000 --dry  -s 6 `
   this script creates some files to be able to send out jobs that runs the hadd for each group of files. Note that it reads the merged `output_total.coffea` produced in the previous step, so the per-chunk event counts and `sum_genweights` are taken from there.

6. From all this process you will get out at the end an updated `dataset_definition_file.json` to be used in your analysis config.

### Minimal skimming configuration

Skimming is enabled simply by setting `save_skimmed_files` on the `Configurator` to an
output folder (local path or `root://…` EOS URL). The skim cuts in the `skim` list define
which events are written; everything downstream (preselections, categories, …) is still
declared but is only used if you keep processing after the skim:

```python
cfg = Configurator(
    ...
    skim = [
        get_nPVgood(1), eventFlags, goldenJson,
        get_HLTsel(),
        get_nObj_min(2, minpt=20., coll="Jet"),   # at least 2 raw jets
    ],
    save_skimmed_files = "./skim/",
    ...
)
```

Each processed chunk produces one ROOT file under `save_skimmed_files`. The generator
weight sum is rescaled (`skimRescaleGenWeight`) so cross sections still match once the
skimmed files are used as inputs downstream. In addition each job writes a `.coffea`
output holding the cutflow and the `sum_genweights` of every chunk it processed (including
chunks that skimmed 0 events); these must be combined with `merge-outputs` — see step 4 of
the [workflow above](#skimming-events) — to get the correct normalization of the skimmed
dataset.

### Skim modes

When `save_skimmed_files` is set, **which events get written** is governed by the
`skim_mode` entry of `workflow_options`. Two modes are available; both *short-circuit* the
processing — once the skimmed chunk is exported the processor returns immediately and does
**not** fill histograms or columns.

| `skim_mode` | events written | calibration / preselection run? |
|---|---|---|
| `"skim"` *(default)* | events passing the `skim` cuts | no — cuts evaluated on raw NanoAOD only |
| `"presel_any_variation"` | events passing the `preselections` in **at least one** calibration variation | yes — but only to build the selection mask |

#### `skim_mode: "skim"` (default)

This is the standard skim: the cuts in the `skim` list are evaluated on the **raw NanoAOD**
(before any object correction) and every event that passes is exported. No calibrators and
no preselections are run, so the skim is fast and depends only on uncorrected quantities.
This is the mode used by the [minimal configuration above](#minimal-skimming-configuration)
— you do not need to set `skim_mode` explicitly.

Use it when your skim selection can be expressed purely on raw branches (trigger bits,
golden JSON, event flags, raw-jet/lepton multiplicities, …). It is the right choice for the
first-pass reduction of a large dataset.

#### `skim_mode: "presel_any_variation"`

In this mode the skim is **systematic-aware**: an event is kept if it passes the
`preselections` in *any* active calibration variation. For example an event that fails the
nominal jet selection but enters it after a JES Up shift is still written, so the skimmed
files remain valid inputs for a later systematics analysis.

```python
cfg = Configurator(
    ...
    workflow_options = {"skim_mode": "presel_any_variation"},

    skim = [get_nPVgood(1), eventFlags, get_HLTsel()],   # loose, on raw NanoAOD
    save_skimmed_files = "./skim_presel_any/",

    preselections = [get_nObj_min(5, minpt=30., coll="JetGood")],  # tighter, on calibrated objects

    calibrators = default_calibrators_sequence,
    variations = {
        "weights": {"common": {"inclusive": [], "bycategory": {}}, "bysample": {}},
        "shape":   {"common": {"inclusive": ["jet_calibration"]}, "bysample": {}},
    },
    ...
)
```

Internally the processor first applies the `skim` cuts, then runs the calibration loop as a
*dry run*: for every variation it applies the object preselection and computes the
preselection mask **without filtering events**, accumulating the logical OR of all the
per-variation masks. The combined mask is finally applied to the (uncalibrated) post-skim
events, which are then exported. The set of variations considered is exactly the one
declared in `variations["shape"]` together with the nominal pass.

Use this mode when the events you ultimately want are defined by a selection on
**calibrated** objects (jets after JEC/JER, etc.) and you must not lose events that only
enter the selection under a systematic shift.

:::{warning}
`skim_mode` only has an effect when `save_skimmed_files` is configured. Setting it without
`save_skimmed_files` triggers a warning and is ignored — the processor then runs the normal
analysis (calibration, preselection, histograms) rather than skimming.
:::

### hadd the skimmed files

The skim produces many small per-chunk files. `hadd-skimmed-files` computes sensible
groups and writes out the jobs to merge them:

```bash
pocket-coffea hadd-skimmed-files -fl output_total.coffea \
    -o root://eoscms.cern.ch//eos/cms/store/group/.../skim_hadd \
    -e 400000 -s 6 --dry
```

After the hadd jobs have run, validate the result with `--check`. This does *not* run hadd
again — it rebuilds the expected workload and verifies, for each output file, that it
exists, opens, has an `Events` tree, and that `GetEntries()` matches the expected sum of
`nskimmed_events`. Any failures are written to `hadd_failed.json` / `.txt` together with a
resubmission `.sub` file:

```bash
pocket-coffea hadd-skimmed-files -fl output_total.coffea \
    -o root://eoscms.cern.ch//eos/cms/store/group/.../skim_hadd --check
```

## Subsamples

A *subsample* splits one declared sample into several mutually-defined sub-categories at
the dataset level, each selected by a list of `Cut`s. Subsamples are declared in the
`datasets` block and produce independent output keys named `<sample>__<subsample>`:

```python
datasets = {
    "jsons": ['datasets/datasets_cern.json'],
    "filter": {
        "samples": ['TTTo2L2Nu', "DATA_SingleMuon"],
        "year": ['2018'],
    },
    "subsamples": {
        "TTTo2L2Nu": {
            "ele": [get_nObj_min(1, coll="ElectronGood"), get_nObj_eq(0, coll="MuonGood")],
            "mu":  [get_nObj_eq(0, coll="ElectronGood"), get_nObj_min(1, coll="MuonGood")],
        }
    }
}
```

This yields `TTTo2L2Nu__ele` and `TTTo2L2Nu__mu` in the output. Subsamples need **not** be
orthogonal or exhaustive — an event can fall into several subsamples (it is duplicated) or
into none (it is dropped). Subsample-specific weights and variations are addressed by the
full `<sample>__<subsample>` key in the `bysample` block:

```python
weights = {
    "common": {"inclusive": ["genWeight", "lumi", "XS", "pileup"]},
    "bysample": {
        "TTTo2L2Nu__ele": {
            "inclusive": ["sf_custom_C"],
            "bycategory": {"B": ["sf_custom_D"]},
        }
    }
}
```

### Primary dataset cross-cleaning

In data the same physics event can be stored in more than one primary dataset (PD). A
di-leptonic event, for instance, may fire both a single-electron and a single-muon
trigger, and would then be present in **both** the `EGamma` and `SingleMuon` PDs. If you
simply OR all triggers on each PD you will count such events twice. Cross-cleaning removes
the overlap by assigning each event to exactly one PD.

The strategy is an ordered priority among the primary datasets: the first PD keeps every
event that fires *its own* triggers; each subsequent PD keeps events that fire its own
triggers **and** do *not* fire the triggers of any higher-priority PD. The veto is built
with `get_HLTsel(primaryDatasets=[...], invert=True)`.

This is done at the **subsample** level, one subsample per PD, so that the trigger logic is
isolated per data sample. A common idiom is to give the
single subsample the **same name as the sample**, so the cleaned output keeps a clean,
self-describing name:

```python
datasets = {
    "filter": {
        "samples": ["DATA_EGamma", "DATA_SingleMuon", ...],
        "year": ['2022_preEE', '2022_postEE', '2023_preBPix', '2023_postBPix', '2024'],
    },
    "subsamples": {
        # Priority 1: EGamma keeps all events firing the SingleEle triggers
        "DATA_EGamma": {
            "DATA_EGamma": [get_HLTsel(primaryDatasets=["SingleEle"])],
        },
        # Priority 2: SingleMuon keeps events firing SingleMuon triggers
        #             but NOT the (higher-priority) SingleEle triggers
        "DATA_SingleMuon": {
            "DATA_SingleMuon": [
                get_HLTsel(primaryDatasets=["SingleMuon"]),
                get_HLTsel(primaryDatasets=["SingleEle"], invert=True),
            ],
        },
    },
}
```

With this ordering:

- an event firing only SingleEle triggers → kept by `DATA_EGamma` only;
- an event firing only SingleMuon triggers → kept by `DATA_SingleMuon` only;
- an event firing **both** → kept by `DATA_EGamma` (priority 1) and explicitly vetoed in
  `DATA_SingleMuon` by the inverted SingleEle selection.

so every data event is counted exactly once.

:::{note}
The cross-cleaning cuts go in the **subsample** definitions, not in the `skim`. The skim
typically applies the inclusive OR of all the primary-dataset triggers so that no useful
event is dropped before the cleaning step:

```python
skim = [
    get_nPVgood(1), eventFlags, goldenJson,
    ...
    get_HLTsel(["SingleEle", "SingleMuon"]),   # inclusive OR; cleaning happens per-subsample
]
```
:::

:::{warning}
Apply the cross-cleaning only to **data**. The MC samples should keep the inclusive
trigger OR (the default `get_HLTsel()` behavior), because there is no double-counting to
remove in simulation — each MC event exists only once regardless of which triggers it fires.
:::

The same mechanism extends to more than two primary datasets: order them by priority and,
for the `i`-th PD, add one inverted `get_HLTsel` per higher-priority PD (or a single
inverted selection listing all the higher-priority primary datasets).

## Define a custom weight

Custom event weights are created with `WeightLambda.wrap_func`, then registered by passing
them in the `weights_classes` list (concatenated with the built-in `common_weights`).
The wrapped function has the signature
`function(params, metadata, events, size, shape_variations)` and must return an array of
length `size`.

A constant or nominal-only weight sets `has_variations=False` and returns a single array:

```python
import numpy as np
from pocket_coffea.lib.weights.weights import WeightLambda
from pocket_coffea.lib.weights.common import common_weights

my_custom_weight = WeightLambda.wrap_func(
    name="my_custom_weight",
    function=lambda params, metadata, events, size, shape_variations: np.ones(size) * 2.0,
    has_variations=False,
)
```

Once defined, register the wrapper and reference it by name in the `weights` block. Weights
can be applied to all samples (`common`) or to specific ones (`bysample`), and either
inclusively or only in specific categories (`bycategory`):

```python
cfg = Configurator(
    ...
    weights = {
        "common": {
            "inclusive": ["genWeight", "lumi", "XS", "pileup"],
            "bycategory": {"2jets_B": ["my_custom_weight"]},
        },
        "bysample": {
            "TTTo2L2Nu": {"bycategory": {"2jets_D": ["sf_btag"]}},
        },
    },
    weights_classes = common_weights + [my_custom_weight],
    ...
)
```

### Define a custom weight with custom variations

To attach **a single Up/Down systematic** to a weight, set `has_variations=True` and return
the triple `(nominal, up, down)`:

```python
my_custom_weight_var = WeightLambda.wrap_func(
    name="my_custom_weight_withvar",
    function=lambda params, metadata, events, size, shape_variations: (
        np.ones(size) * 2.0,   # nominal
        np.ones(size) * 3.0,   # up
        np.ones(size) * 0.5,   # down
    ),
    has_variations=True,
)
```

For **several named variations** on the same weight, declare them in `variations=[...]` and
return `(nominal, [names], [ups], [downs])`:

```python
my_custom_weight_multivar = WeightLambda.wrap_func(
    name="my_custom_weight_multivar",
    function=lambda params, metadata, events, size, shape_variations: (
        np.ones(size) * 2.0,                          # nominal
        ["stat", "syst"],                             # variation names
        [np.ones(size) * 3.0, np.ones(size) * 4.0],   # up for each
        [np.ones(size) * 0.5, np.ones(size) * 0.25],  # down for each
    ),
    has_variations=True,
    variations=["stat", "syst"],
)
```

A weight only produces systematic templates if it is *also* listed in the `variations`
block of the `Configurator`. The structure mirrors the `weights` block, so a variation can
be enabled for all samples or only for some, inclusively or per-category:

```python
variations = {
    "weights": {
        "common": {
            "inclusive": ["pileup", "sf_ele_id", "sf_ele_reco"],
            "bycategory": {"1btag": ["sf_btag"]},
        },
        "bysample": {
            "TTTo2L2Nu": {"bycategory": {"2jets_D": ["my_custom_weight_multivar"]}},
        },
    },
}
```

The resulting histogram `variation` axis will contain one `<name>Up`/`<name>Down` pair per
declared variation (e.g. `my_custom_weight_multivar_statUp`, `..._systDown`).

:::{note}
Weights can also be applied to **data** (e.g. data-driven correction factors): list them in
the `weights` block exactly as for MC. See
`tests/test_full_configs/test_custom_weights/config_weights_data.py`.
:::

## promptMVA lepton selection and on-the-fly re-evaluation

The promptMVA (`mvaTTH`) is a BDT-based lepton ID used in ttH analyses to suppress
non-prompt leptons. For Run3 2022–2023 datasets produced with NanoAODv12/v13, the
`mvaTTH` branch stored in NanoAOD was trained on a different data-taking period and
must be **re-evaluated on the fly** before applying the working-point cut. For 2024
datasets (NanoAODv15) the value is correct out of the box.

PocketCoffea provides:
- `ElectronMVAEvaluator` / `MuonMVAEvaluator` — XGBoost wrappers in
  `pocket_coffea.lib.xgboost_evaluator` that reproduce the TMVA score in `[-1, 1]`.
- `lepton_selection_promptMVA` — drop-in replacement for `lepton_selection` in
  `pocket_coffea.lib.leptons` that applies the full preselection and optional MVA WP cut.
- Pre-configured model weights and scale factors in
  `pocket_coffea/parameters/lepton_scale_factors.yaml` under
  `lepton_scale_factors.{electron,muon}_sf.promptMVA_jsons`.

### Object preselection parameters

The `lepton_selection_promptMVA` function requires several additional keys under
`object_preselection` compared to the standard `lepton_selection`. Add these to your
`params/object_preselection.yaml`:

```yaml
object_preselection:
  Electron:
    pt: 15.0
    eta: 2.5
    sip3d: 8.0          # 3D impact-parameter significance
    lostHits: 1         # max number of lost tracker hits
    dxy: 0.05           # |dxy| < dxy [cm]
    dz: 0.1             # |dz| < dz [cm]
    id:                 # NanoAOD field used as MVA score (per year)
      "2022_preEE":    mvaTTH_redo
      "2022_postEE":   mvaTTH_redo
      "2023_preBPix":  mvaTTH_redo
      "2023_postBPix": mvaTTH_redo
      "2024":          mvaTTH           # read directly from NanoAOD in v15
    mva_wp:             # MVA working-point threshold (per year)
      "2022_preEE":    0.90
      "2022_postEE":   0.90
      "2023_preBPix":  0.90
      "2023_postBPix": 0.90
      "2024":          0.90
    btag_cut:           # DeepFlavB score veto on the closest jet (per year)
      2022_preEE: 0.3086
      2022_postEE: 0.3196
      2023_preBPix: 0.2431
      2023_postBPix: 0.2435
      "2024": 999.

  Muon:
    pt: 10.0
    eta: 2.4
    iso: 0.4
    sip3d: 8.0
    dxy: 0.05
    dz: 0.1
    base_id: looseId    # NanoAOD field for baseline muon ID
    id:
      "2022_preEE":    mvaTTH_redo
      "2022_postEE":   mvaTTH_redo
      "2023_preBPix":  mvaTTH_redo
      "2023_postBPix": mvaTTH_redo
      "2024":          mvaTTH
    mva_wp:
      "2022_preEE":    0.64
      "2022_postEE":   0.64
      "2023_preBPix":  0.64
      "2023_postBPix": 0.64
      "2024":          0.64
    btag_cut:
      2022_preEE: 0.3086
      2022_postEE: 0.3196
      2023_preBPix: 0.2431
      2023_postBPix: 0.2435
      "2024": 999.
```

### Workflow implementation

In `apply_object_preselection`, first compute scores for 2022–2023 years and attach
them as `mvaTTH_redo`, then call `lepton_selection_promptMVA` with `apply_mva_cut=True`:

```python
import awkward as ak
from pocket_coffea.workflows.base import BaseProcessorABC
from pocket_coffea.lib.objects import jet_selection, btagging
from pocket_coffea.lib.leptons import lepton_selection_promptMVA
from pocket_coffea.lib.xgboost_evaluator import ElectronMVAEvaluator, MuonMVAEvaluator

REDO_MVA_YEARS = ["2022_preEE", "2022_postEE", "2023_preBPix", "2023_postBPix"]


class MyProcessor(BaseProcessorABC):

    def apply_object_preselection(self, variation):
        # Re-evaluate promptMVA on the fly for 2022/2023 datasets
        if self._year in REDO_MVA_YEARS:
            ele_evaluator = ElectronMVAEvaluator(
                self.params.lepton_scale_factors.electron_sf.promptMVA_jsons[self._year].model_weights
            )
            muon_evaluator = MuonMVAEvaluator(
                self.params.lepton_scale_factors.muon_sf.promptMVA_jsons[self._year].model_weights
            )

            _, presel_ele_mask = lepton_selection_promptMVA(
                self.events, "Electron", self.params, self._year, apply_mva_cut=False
            )
            self.events["Electron"] = ak.with_field(
                self.events["Electron"],
                ele_evaluator.evaluate(self.events["Electron"], self.events["Jet"], mask=presel_ele_mask),
                "mvaTTH_redo",
            )

            _, presel_muon_mask = lepton_selection_promptMVA(
                self.events, "Muon", self.params, self._year, apply_mva_cut=False
            )
            self.events["Muon"] = ak.with_field(
                self.events["Muon"],
                muon_evaluator.evaluate(self.events["Muon"], self.events["Jet"], mask=presel_muon_mask),
                "mvaTTH_redo",
            )

        # Apply final selection using the (re-)computed MVA score
        self.events["ElectronGood"], _ = lepton_selection_promptMVA(
            self.events, "Electron", self.params, self._year,
            apply_mva_cut=True,
            mva_var=self.params.object_preselection.Electron.id[self._year],
        )
        self.events["MuonGood"], _ = lepton_selection_promptMVA(
            self.events, "Muon", self.params, self._year,
            apply_mva_cut=True,
            mva_var=self.params.object_preselection.Muon.id[self._year],
        )

        leptons = ak.with_name(
            ak.concatenate((self.events.MuonGood, self.events.ElectronGood), axis=1),
            name="PtEtaPhiMCandidate",
        )
        self.events["LeptonGood"] = leptons[ak.argsort(leptons.pt, ascending=False)]
        self.events["JetGood"], self.jetGoodMask = jet_selection(
            self.events, "Jet", self.params, self._year, leptons_collection="LeptonGood"
        )
        self.events["BJetGood"] = btagging(
            self.events["JetGood"],
            self.params.btagging.working_point[self._year],
            wp=self.params.object_preselection.Jet["btag"]["wp"],
        )
```

### Scale factors

The corresponding promptMVA lepton ID scale factors are provided as the
`sf_ele_promptMVA` and `sf_mu_promptMVA` weight wrappers, already included in
`common_weights`. Add them to the `weights` section of your configurator:

```python
weights = {
    "common": {
        "inclusive": [
            "genWeight", "lumi", "XS",
            "sf_ele_promptMVA",
            "sf_mu_promptMVA",
        ],
    },
}
```

:::{note}
For 2022–2023 the scale factors are derived from the TTH multilepton team and stored
under `pocket_coffea/parameters/custom_SFs/{electron,muon}_promptMVA/`. For 2024
(NanoAODv15) central POG scale factors are used automatically.
:::

## Systematic shape variations

Shape (a.k.a. "up/down template") systematics come from object calibrations — JEC/JER,
MET, electron scale & smearing, etc. They are enabled by listing the relevant *calibrator*
in the `shape` block of `variations`, while the calibrators themselves are activated
through the `calibrators` argument of the `Configurator`:

```python
from pocket_coffea.lib.calibrators.common.common import default_calibrators_sequence

cfg = Configurator(
    ...
    calibrators = default_calibrators_sequence,   # JetsCalibrator, METCalibrator, ElectronsScaleCalibrator, ...
    variations = {
        "weights": {"common": {"inclusive": [...]}},
        "shape":   {"common": {"inclusive": ["jet_calibration"]}},
    },
    ...
)
```

During processing the calibration loop re-runs the full per-event selection once per
variation, so each shape systematic produces its own entry on the histogram `variation`
axis. For the jet calibration the axis labels are built as `<jet_type>_<variation>Up/Down`,
e.g. `AK4PFchs_JES_TotalUp`, `AK4PFchs_JERDown` for Run2, or `AK4PFPuppi_JES_TotalUp` for
Run3. Which individual JES/JER sources are produced is controlled by the
`jets_calibration.variations` parameter (see
[Systematic Variations](#systematic-variations) under Jet calibration).

### Subsample-specific shape variations

Shape variations declared as `common` apply to every (sub)sample. To restrict an
*additional* shape variation to a single subsample, address it by its `<sample>__<subsample>`
key in the `bysample` block, exactly as for weights. A subsample that is not listed gets
only the common variations, so its `variation` axis will not contain the subsample-specific
labels. Two subsamples with identical selection cuts will produce identical nominal
histograms, while only the configured one carries the extra variation templates (see
`tests/test_full_configs/test_shape_variations/config_shape_var_bysubsample.py`).

When shape and weight variations are both active, the weights applied during a shape pass
are the **nominal** subsample weights — so a subsample-specific normalization factor
multiplies both the nominal and the JES-shifted templates consistently.

### Electron scale & smearing

The `ElectronsScaleCalibrator` (part of `default_calibrators_sequence`) applies the
electron scale & smearing corrections and their variations. When the corrected
`ElectronGood_pt` is exported alongside the uncorrected `ElectronGood_pt_original`, the two
differ for every event, confirming the correction was applied (see
`config_eleSS_Run3.py`).

## Apply corrections
Here we describe how to apply certain corrections recommended by CMS POGs.

### MET-xy
From a purely physical point of view, the distribution of the $\phi$-component of the missing transverse momentum (a.k.a. MET) should be uniform due to rotational symmetry. However, for a variety of detector-related reasons, the distribution is not uniform in practice, but shows a sinus-like behavior. To correct this behavior, the x- and y-component of the MET can be altered in accordance to the recommendation of JME. In the PocketCoffea workflow, these corrections can be applied using the `met_xy_correction()` function:

```
from pocket_coffea.lib.jets import met_xy_correction
met_pt_corr, met_phi_corr = met_xy_correction(self.params, self.events, self._year, self._era)
```  
Note, that this shift also alters the $p_\mathrm{T}$ component! Also, the corrections are only implemented for Run2 UL (thus far).

### Jet calibration configuration

PocketCoffea provides a flexible jet calibration system that handles Jet Energy Corrections (JEC), Jet Energy Resolution (JER), and systematic variations. The calibration is configured through the `jets_calibration.yaml` parameters file and applied using specialized calibrator classes.

#### Core Concepts

The jet calibration system is built around three main concepts:

1. **Jet Types**: Abstract jet algorithm/configuration identifiers (e.g., `AK4PFchs`, `AK4PFPuppi`, `AK8PFPuppi`)
2. **Jet Collections**: Actual NanoAOD collections that store jet objects (e.g., `Jet`, `FatJet`)
3. **Factory Configurations**: JEC/JER correction files and settings associated with each jet type

The system maps jet types to collections and applies the appropriate corrections based on data-taking period, MC/Data status, and systematic variation requests.

#### Available Calibrators

**JetsCalibrator**: Standard JEC/JER calibrator for regular jet collections
- Applies JEC (L1FastJet, L2Relative, etc.) and JER corrections
- Handles systematic variations (JES uncertainties, JER variations)
- Works with both MC and Data
- Can apply the pT regression to some jets collections before calibration.


#### Configuration Structure

The configuration is organized in several sections:

##### Factory Configuration
Defines the correction files for each jet type, data-taking period, and correction level:

```yaml
default_jets_calibration:
  factory_config_clib:
    AK4PFchs:
      2016_PreVFP:
        json_path: ${cvmfs:Run2-2016preVFP-UL-NanoAODv9,JME,jet_jerc.json.gz}
        jec_mc: Summer19UL16APV_V7_MC
        jec_data:
          B: Summer19UL16APV_RunBCD_V7_DATA
          C: Summer19UL16APV_RunBCD_V7_DATA
          D: Summer19UL16APV_RunBCD_V7_DATA
          E: Summer19UL16APV_RunEF_V7_DATA
          F: Summer19UL16APV_RunEF_V7_DATA
        jer: Summer20UL16APV_JRV3_MC
        level: L1L2L3Res

      2016_PostVFP:
        json_path: ${cvmfs:Run2-2016postVFP-UL-NanoAODv9,JME,jet_jerc.json.gz}
        jec_mc: Summer19UL16_V7_MC
        jec_data: Summer19UL16_RunFGH_V7_DATA
        jer: Summer20UL16_JRV3_MC
        level: L1L2L3Res

    ....
    AK4PFPuppi:
      2022_preEE:
        json_path: ${cvmfs:Run3-22CDSep23-Summer22-NanoAODv12,JME,jet_jerc.json.gz}
        jec_mc: Summer22_22Sep2023_V3_MC
        jec_data: Summer22_22Sep2023_RunCD_V3_DATA
        jer: Summer22_22Sep2023_JRV1_MC
        level: L1L2L3Res

      2022_postEE:
        json_path: ${cvmfs:Run3-22EFGSep23-Summer22EE-NanoAODv12,JME,jet_jerc.json.gz}
        jec_mc: Summer22EE_22Sep2023_V3_MC
        jec_data:
          E: Summer22EE_22Sep2023_RunE_V3_DATA
          F: Summer22EE_22Sep2023_RunF_V3_DATA
          G: Summer22EE_22Sep2023_RunG_V3_DATA
        jer: Summer22EE_22Sep2023_JRV1_MC
        level: L1L2L3Res

```

##### Collection Mapping
Maps jet types to actual NanoAOD collections for each data-taking period:

```yaml
jets_calibration:
  collection:
    2022_preEE:
      AK4PFPuppi: "Jet"
      AK8PFPuppi: "FatJet"
      # AK4PFPuppiPNetRegression: "Jet"
```

Here, the `AK4PFPuppi`, `AK8PFPuppi` and `AK4PFPuppiPNetRegression` are the jet types used as internal labels in the PocketCoffea configuration to link various pieces of the jets configuration together. The `Jet` and `FatJet` are the names of the branches in NanoAOD.

:::{warning}
All the collections defined in the `jets_calibration.collection` entry will be calibrated by the configured JetsCalibrator if included in the calibrators sequence. It is not allowed to match the same jet collection to multiple jet types: an error will be raised.
:::

##### Collection Name Aliases
In order to use the same calibration settings on different jet collections, it is possible to define aliases for the collection names with the `collection_name_alias` key. For example, if you have a jet collection called `JetCustom` that you want to calibrate in the same way as you do for the `Jet` collection, which is mapped to the `AK4PFPuppi` jet type, your configuration would look like this:

```yaml
jets_calibration:
  collection:
    2022_preEE:
      AK4PFPuppi: "Jet"
      AK4PFPuppiCustom: "JetCustom"
  collection_name_alias:
      2022_preEE:
        AK4PFPuppiCustom : "AK4PFPuppi"
```

:::{warning}
In order to merge the variations of the `JetCustom` collection, you need to define a `merge_collections_for_variations` entry as specified in section [Merging Systematic Variations](#merging-systematic-variations).
:::

##### Calibration Control Flags

Here is how one can enable/disable different correction types per jet type and period:

```yaml
# Apply JEC to MC
  apply_jec_MC:
    2022_preEE:
      AK4PFPuppi: True
      AK4PFPuppiPNetRegression: True

# Apply JER to MC
  apply_jer_MC:
    2022_preEE:
      AK4PFPuppi: True
      AK4PFPuppiPNetRegression: True

# Apply JEC to Data
  apply_jec_Data:
    2022_preEE:
      AK4PFPuppi: True

# Apply pT regression to MC
  apply_pt_regr_MC:
    2022_preEE:
      AK4PFPuppi: False
      AK4PFPuppiPNetRegression: True

  sort_by_pt:
    2016_PreVFP:
      AK4PFchs: True
      AK8PFPuppi: True
    2016_PostVFP:
      AK4PFchs: True
      AK8PFPuppi: True
```

##### Systematic Variations
Different sets of JEC systematic variations are available in the default parameter set. 

```yaml
default_jets_calibration:
  variations:
    full_variations:      # All individual JES sources
      AK4PFPuppi:
        2022_preEE:
          - JES_Regrouped_Absolute
          - JES_Regrouped_FlavorQCD
          - JER
    total_variation:      # Total JES uncertainty
      AK4PFPuppi:
        2022_preEE:
          - JES_Total
          - JER
```

The set of variations to be used has to be setup in the `jets_calibration.variation` key. For example: 

```yaml
jets_calibration:
  variations: "${default_jets_calibration.variations.total_variation}"

```

The user can also enable only some of the variations for different data taking periods

```yaml
jets_calibration:
  variations:
    2022_preEE:
      AK4PFPuppi:
        - JES_Total  # Only total JES uncertainty
        - JER        # JER variations
```

##### Merging Systematic Variations
By default, each systematic variation produces a separate collection of calibrated jets. To reduce the number of collections, variations can be merged into a single collection per jet type. This is configured using the `merge_collections_for_variations` key, which specifies which jet types should have their variations merged and under which new name:, e.g.:

```yaml
jets_calibration:
  merge_collections_for_variations:
    2022_preEE:
      AK4Jet:
        - AK4PFPuppi
        - AK4PFPuppiPNetRegression
        - AK4PFPuppiCustom
```

This will create variation with the name `AK4Jet_{variation}_[up|down]` that merges the variations from the specified jet types.  
ToDo: better describe how this merging is done. What if AK4PFPuppi and AK4PFPuppiCustom have different up_variation for example.

#### MET Recalibration
Configure MET corrections that propagate jet calibration changes:

```yaml
  rescale_MET_config:
    2022_preEE:
      apply: True
      MET_collection: "PuppiMET"
      Jet_collection: "Jet"
```

#### Usage in Analysis

The jet calibration is typically applied automatically when using the standard calibrator sequence:

```python
from pocket_coffea.lib.calibrators.common import default_calibrators_sequence

# Default sequence includes: JetsCalibrator, METCalibrator, ElectronsScaleCalibrator
calibrators = default_calibrators_sequence
```

For custom configurations, modify the `jets_calibration` section in your parameters, for example:

```yaml
jets_calibration:
  # Use a different variation set
  variations: "${default_jets_calibration.variations.full_variations}"

  # Enable specific jet types
  collection:
    2022_preEE:
      AK4PFPuppiCustomSetOfCorrections: "Jet"
```


### Jet energy regression
Starting from Run3 datasetes the ParticleNet jet energy regression corrections are part of the `Jet` object in NanoAOD. But they are not applied by default. In PocketCoffea the regression is implemented in `JetPtRegressionCalibrator` in the calibration sequence and can be turned On/Off via configuration parameters as show in the example below:

```yaml
jets_calibration:
  collection:
    2022_preEE:
      AK4PFPuppi: null
      AK4PFPuppiPNetRegression: "Jet"
      #AK4PFPuppiPNetRegressionPlusNeutrino: "Jet"

  apply_pt_regr_MC:
    2022_preEE:
      AK4PFPuppiPNetRegression: True
      #AK4PFPuppiPNetRegressionPlusNeutrino: True

  apply_pt_regr_Data:
    2022_preEE:
      AK4PFPuppiPNetRegression: True
      #AK4PFPuppiPNetRegressionPlusNeutrino: True
```
Note that there are two versions of regression are available in PNet: with and without neutrinos used in the training.  
In the example above, the default `jets` configuration is overwritten to assign the `Jet` collection to the `AK4PFPuppiPNetRegression` tag, and to activate the pt regression for data and MC for that tag. Note the line `AK4PFPuppi: null` -- it is needed to remove the association of the `AK4PFPuppi` to the `Jet`, which is default pocket-coffea setting.

However, this is not all. We also need to apply JEC and JER on the regressed jets. The dedicated corrections, derived for PNet regressed jets, have been recently releasesd by JME, see [https://cms-jerc.web.cern.ch/ExpJEC/](https://cms-jerc.web.cern.ch/ExpJEC/) (as of 19 May 2026). The corrections are located at CERN EOS (not CVMFS!). One has to specify a path to them and set the tags, like so for *2022_preEE*:  

```yaml
jets_calibration:
  ...
  # set up collection, apply_pt_regr_MC and apply_pt_regr_Data fields
  ...

  # Path to JERCS at CERN EOS:
  path_to_JERC_PNetReg: '/eos/cms/store/group/phys_jetmet/ExpJEC/json_files_Reg/'
  # If running outside CERN without access to EOS: copy the files to your cluster and use the corresponding  path.

  jet_types:
    AK4PFPuppiPNetRegression:
      2022_preEE:
        json_path: "${jets_calibration.path_to_JERC_PNetReg}/Run3Summer22_22Sep2023/regJet_jerc.json.gz"
	    jec_mc: Summer22_22Sep2023_V4_MC
	    jec_data: Summer22_22Sep2023_V4_DATA
	    jer: Summer22_22Sep2023_JRV1_MC
        level: L1L2L3Res

  variations:
    AK4PFPuppiPNetRegression:
      2022_preEE:
        - JES_Total
        - JER
```  

An example of a full config can be found [here](https://gitlab.cern.ch/cms-analysis/hig/vhcc-run3/VHccPoCo/-/blob/main/params/jet_regression.yaml?ref_type=heads).


If the user needs to apply regression only to a subset of jets, then the best strategy is to define a copy of the Jet collection and calibrate that. 

An example configuration for this:

```yaml
jets_calibration:
  collection:
    2022_preEE:
      AK4PFPuppiPNetRegression: "JetPtReg"
      #AK4PFPuppiPNetRegressionPlusNeutrino: "JetPtReg"

object_preselections:
    Jet:
        pt: 20
        eta..

    JetPtReg:
        pt: 20
        eta: ...
        btag:
          wp:
            M
```

For this to work a clone of the `Jet` collection (called `JetPtReg`) needs to be defined in the `process_extra_after_skim` function of the user's workflow:

```python
from pocket_coffea.workflows.base import BaseProcessorABC


class PtRegrProcessor(BaseProcessorABC):
    def __init__(self, cfg) -> None:
        super().__init__(cfg=cfg)

    def process_extra_after_skim(self):
        # Create extra Jet collections for testing
        self.events["JetPtReg"] = ak.copy(self.events["Jet"])
        # self.events["JetPtRegPlusNeutrino"] = ak.copy(self.events["Jet"])
```  

Now it is up to the user to deal with two collections in their workflow: `Jet` and `JetPtReg`.
  

Further references:  
* The analysis note: [AN-2022/094](https://cms.cern.ch/iCMS/jsp/db_notes/noteInfo.jsp?cmsnoteid=CMS%20AN-2022/094)
* Measuring response in Z+b events: [presentation](https://indico.cern.ch/event/1451196/contributions/6181213/attachments/2949253/5183620/cooperstein_HH4b_oct162024.pdf)
* JME page with *experimental* JECs: [https://cms-jerc.web.cern.ch/ExpJEC/](https://cms-jerc.web.cern.ch/ExpJEC/)

#### Merge regressed and standard jet pT
In some cases, e.g. PNet regression in NanoAODv12, the regression can be applied only to a subset of jets (e.g. cutting on pT and eta of the jet). In this case, one may want to merge the regressed pT values with the standard pT values for jets failing the regression criteria. In order to do this, your configuration would look like this:

```yaml
jets_calibration:
  collection:
    2022_preEE:
      AK4PFPuppi: "Jet"
      AK4PFPuppiPNetRegression: "JetPtReg"
      #AK4PFPuppiPNetRegressionPlusNeutrino: "JetPtReg"

```

The merging of the two jet collections should be done in the user's workflow, e.g. in the `apply_object_preselection` section:

```python
from pocket_coffea.workflows.base import BaseProcessorABC


class PtRegrProcessor(BaseProcessorABC):
    def __init__(self, cfg) -> None:
        super().__init__(cfg=cfg)

    def process_extra_after_skim(self):
        # Create extra Jet collections for testing
        self.events["JetPtReg"] = ak.copy(self.events["Jet"])
        #self.events["JetPtRegPlusNeutrino"] = ak.copy(self.events["Jet"])

    def apply_object_preselection(self, variation):
        # Use the regressed jet from PNet collection if available,
        # otherwise use the standard, JEC corrected collection.
        # This way we consider correctly all fields which change depending on
        # the pt definition, namely the pt, mass and the associated systematic variations:
        self.events["Jet"] = ak.where(
            self.events["JetPtReg"].pt > 0,
            self.events["JetPtReg"],
            self.events.Jet,
        )
```

:::{warning}
In order to merge the variations of the `Jet` and `JetPtReg` collections, you need to define a `merge_collections_for_variations` entry as specified in section [Merging Systematic Variations](#merging-systematic-variations).
:::

:::{warning}
When merging the collections like this, make sure to set the `sort_by_pt` option to `False` for the jet type in the jets calibration configuration, otherwise the jet ordering will be changed and the merging will fail.
:::


## Create a custom executor to use `onnxruntime`

This example shows running on CERN lxplus and assumes a prior understanding of how to load and use an ML model with onnxruntime. For more examples see the executors in the ttHbb analysis [here](https://github.com/PocketCoffea/AnalysisConfigs/tree/main/configs/ttHbb/semileptonic/common/executors)

At the time of writing, `onnxruntime` is not installed in the singularity container, which means that you will need to run with a custom environment. Instructions for this are given in [Running the analysis](./running.md)

The following code is a custom executor which is meant to be filled in with details such as the path to the `model.onnx` file and options used in the `InferenceSession`.
```python
from pocket_coffea.executors.executors_lxplus import DaskExecutorFactory
from dask.distributed import WorkerPlugin, Worker, Client


class WorkerInferenceSessionPlugin(WorkerPlugin):
    def __init__(self, model_path, session_name):
        super().__init__()
        self.model_path = model_path
        self.session_name = session_name

    async def setup(self, worker: Worker):
        import onnxruntime as ort

        session = ort.InferenceSession(
            self.model_path,
            # Whatever other options you use
            providers=["CPUExecutionProvider"],
        )
        worker.data["model_session_" + self.session_name] = session


class OnnxExecutorFactory(DaskExecutorFactory):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def setup(self):
        super().setup()
        self.Model = "absolute/path/to/your/model.onnx"
        inference_session_plugin = WorkerInferenceSessionPlugin(self.Model, "ModelName")
        self.dask_client.register_plugin(inference_session_plugin)

    def close(self):
        self.dask_client.close()
        self.dask_cluster.close()


def get_executor_factory(executor_name, **kwargs):
    return OnnxExecutorFactory(**kwargs)
```

To use the model to process events in the `workflow.py` file, one would do something like this. See [here](https://github.com/PocketCoffea/AnalysisConfigs/blob/main/configs/ttHbb/semileptonic/sig_bkg_classifier/workflow_test_spanet.py) for another example.
```python
# import the get_worker function
from dask.distributed import get_worker

# Rest of workflow


# Suppose you want to apply your model after preselection. You would do e.g.
def process_extra_after_presel():
    try:
        worker = get_worker()
    except ValueError:
        worker = None

    # Whatever needs to be done to prepare the inputs to the model

    if worker is None:
        # make it work running locally too
        import onnxruntime as ort

        session = ort.InferenceSession(
            self.model_path,
            # Whatever other options you use
            providers=["CPUExecutionProvider"],
        )
    else:
        session = worker.data["model_session_ModelName"]

    # Continue as you normally would when using an ML model with onnxruntime, e.g.
    model_output = session.run(
        # inputs and options
    )
```
To run with the custom executor, assuming the file is called `custom_executor.py`, one replaces `--executor dask@lxplus` with `--executor-custom-setup custom_executor.py` for example:
```
pocket-coffea run --cfg myConfig.py -o outputDir -ro run_options.yaml --executor-custom-setup custom_executor.py
```

Lastly, the custom executor will print a lot of `INFO` level log messages which are suppressed when running with the built-in pocket-coffea executors. To suppress these for all executors, create a file `~/.config/dask/logging.yaml` and add the following:
```yaml
logging:
  distributed: warning
  distributed.client: warning
  distributed.worker: warning
  distributed.nanny: warning
  distributed.scheduler: warning
```

## Split large outputs into categories

If your configuration contains a large number of categories, variables, and systematics, the number of histograms can grow very large. Although on disk, the size of the *merged* `output_all.coffea` is usually less than O(10 GB), the full accumulation process can consume around O(100 GB) of RAM. This seems unavoidable as coffea accumulation necessarily happens on memory. This can be addressed in one of two ways:

- The `merge-output` script dumps partial `.coffea` outputs whenever memory usage exceeds 50% of the available RAM on the machine. However, this means one still has to use a different large-memory machine to merge them into one `.coffea` file and/or read them all into memory during plotting. The fragmented `.coffea` dumps consume less space on disk and are fewer in number, so it is easier to `scp` them to other machines using this approach.

- A more efficient solution is to split outputs into "category groups" (i.e. channels or regions of the analysis) and merge/process only one group of categories at one time. Since plots are typically made per channel, this lets one do everything without loading multiple category-grouped files into the memory.

Currently, the second solution is implemented only for the `condor@lxplus` executor. It can be utilized as follows:
  * `runner`: Pass `--split-by-category` to `runner` (actually gets passed to the executor parameters). The output from each job is then further split to contain 8 categories per output file, so each job produces `n_groups = n_categories/8` output files. This is handled through the `split-output` command, which in turn calls `utils.filter_output.filter_output_by_category`.
  * `merge-outputs`: Handles the merging per category automatically (no extra flag needed) if `--split-by-category` was passed to `runner`. This will produce `n_groups` merged outputs, containing mutually exclusive groups of categories.
  * `make-plots`: Pass `--split-by-category` flag to handle only the category-wise merged outputs.

## Export columns (arrays)

Besides histograms, PocketCoffea can export selected object fields as flat arrays
("columns"), stored in the output under `output["columns"]`. Columns are declared with
`ColOut` objects in the `columns` block of the `Configurator`, using the same
`common` / `bysample` / `bycategory` structure as weights:

```python
from pocket_coffea.lib.columns_manager import ColOut

columns = {
    "common": {
        "inclusive": [
            ColOut("JetGood", ["pt", "eta"]),
        ],
    },
    "bysample": {
        "TTTo2L2Nu": {
            "bycategory": {
                "2btag": [ColOut("BJetGood", ["pt", "eta", "phi"])],
                "4jets": [ColOut("JetGood", ["pt", "eta", "phi"])],
            }
        }
    },
}
```

`ColOut(collection, columns, flatten=True, store_size=True, fill_none=True,
fill_value=-999.0, pos_start=None, pos_end=None)`:

- `flatten=True` (default) flattens the jagged per-event arrays into a single flat array;
  the per-event multiplicity is recoverable from the companion size column written when
  `store_size=True`. Set `flatten=False` to keep the jagged (per-event) structure — needed
  if you intend to dump to parquet (see below).
- `fill_none` / `fill_value` control how missing entries are padded.
- `pos_start` / `pos_end` export only a slice of the collection (e.g. the leading jet).

### Dump columns to parquet per chunk

By default the columns accumulate into the `.coffea` output. For large column dumps it is
more efficient to write one parquet file per processed chunk, keeping the jagged structure
(`flatten=False`). Enable this with the `dump_columns_as_arrays_per_chunk` workflow option:

```python
cfg = Configurator(
    ...
    workflow_options = {"dump_columns_as_arrays_per_chunk": "./columns"},
    columns = {
        "common": {"inclusive": [ColOut("JetGood", ["pt", "eta"], flatten=False)]},
    },
    ...
)
```

Each chunk writes its arrays under `./columns/...`, organized by sample and category,
instead of being kept in memory until the end of the job.

## Delayed branches (lazy event variables)

Sometimes a derived per-event quantity is needed in cuts, weights or histograms but you
want it computed **once per nominal event** and reused across all calibrator variations,
without it being recomputed in every pass. Register such a quantity as a *delayed branch*
in the workflow `__init__` via `self.delayed_branches.register`:

```python
from pocket_coffea.workflows.base import BaseProcessorABC

class BasicProcessor(BaseProcessorABC):
    def __init__(self, cfg):
        super().__init__(cfg)
        # compute event parity once; events that are masked out get the default value
        self.delayed_branches.register(
            "parity",
            lambda events: events.event % 2,
            default_value=-1.0,
        )
```

The registered branch then behaves like any other event field: it can be referenced by
name in cut functions, in custom weights, and as a histogram axis:

```python
variables = {
    "Parity": HistConf([Axis(field="parity", bins=3, start=-1, stop=2, label="Parity")],
                       variations=True),
}
```

`register(name, compute_fn, default_value=1.0)`:

- `compute_fn(events)` returns an array with a leading event axis. The value is evaluated
  on the nominal events and mapped to each variation, so it is computed only once.
- `default_value` fills events that are not part of the current selection (the example uses
  `-1.0` so unselected events are distinguishable from real parities `0`/`1`).
- To register several branches at once, pass a list of names and a `compute_fn` returning a
  dict `{name: array}`; `default_value` may then be a dict mapping each name to its default.
- Dense multi-dimensional outputs are supported as long as the trailing shape matches the
  shape of the default value.
