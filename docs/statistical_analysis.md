# Statistical Analysis

PocketCoffea ships a small toolkit (`pocket_coffea.utils.stat`) to turn the
histograms stored in a `.coffea` output into [CMS Combine](https://cms-analysis.github.io/HiggsAnalysis-CombinedLimit/latest/)
datacards. The workflow is:

1. **Declare processes** — group the analysis samples into the physics
   processes that become columns of the datacard (`MCProcess`, `DataProcess`,
   collected in `MCProcesses` / `DataProcesses`).
2. **Declare systematic uncertainties** — `lnN` normalization terms and `shape`
   terms mapped to the histogram variations stored in the output
   (`SystematicUncertainty`, collected in `Systematics`).
3. **Build one `Datacard` per category** — each datacard slices one category
   from one variable histogram and writes a `.txt` card plus a `.root` file with
   the nominal and varied templates.
4. **Combine the cards** — `combine_datacards` writes a shell script that runs
   `combineCards.py` and `text2workspace.py` to merge all categories into a
   single workspace.

The classes are importable from two places:

```python
from pocket_coffea.utils.stat import (
    MCProcess, DataProcess, MCProcesses, DataProcesses,
    SystematicUncertainty, Systematics, Datacard,
)
from pocket_coffea.utils.stat.combine import Datacard, combine_datacards
```

(`combine_datacards` lives in `pocket_coffea.utils.stat.combine`; `Datacard` is
re-exported from the package root.)

> **Key convention — processes are split per year.** Every `(process, year)`
> pair becomes its own datacard column, named `"{process}_{year}"`. Data is the
> single exception: it is written as one `data_obs` column summed over years.
> This is what lets you correlate/decorrelate systematics across years simply by
> choosing which years a `SystematicUncertainty` covers.

The snippets below use generic, illustrative names (`signal`, `ttbar`,
`singletop`, …, categories `SR` / `CR1` / `CR2`). Replace them with your own
sample, process and variable names.

---

## 1. Loading the output

Everything starts from a merged PocketCoffea output. You need the histogram for
each variable you want to fit, the `datasets_metadata` (maps datasets → samples
→ data-taking periods) and the `cutflow` (used for the `data_obs` observation
and to skip empty datasets).

```python
from coffea.util import load

df = load("output_all.coffea")
datasets_metadata = df["datasets_metadata"]
cutflow           = df["cutflow"]

years = ["2022_preEE", "2022_postEE", "2023_preBPix", "2023_postBPix", "2024"]
label = "run3"
```

Pick the variables to fit and, optionally, the rebinning. Each fit category is
associated with one variable histogram:

```python
hist_dict = {
    "SR":  df["variables"]["discriminant"],
    "CR1": df["variables"]["control_var_1"],
    "CR2": df["variables"]["nJets"],
}

# Optional per-category rebinning; `None` keeps the histogram's native binning.
bins_edges_dict = {
    "SR":  [0.0, 0.2, 0.4, 0.6, 0.8, 1.0],
    "CR1": None,
    "CR2": [4, 5, 6, 12],
}
```

The keys of `hist_dict` are the *categories* — the values of the histogram's
category axis. One `Datacard` is built per key below.

---

## 2. Processes

### MC processes

An `MCProcess` maps one datacard process to a list of analysis **samples** (the
sample names as they appear in `datasets_metadata`), for a set of **years**:

```python
mc_processes = MCProcesses([
    MCProcess(name="signal",    samples=["SignalSampleA", "SignalSampleB"], years=years, is_signal=True),
    MCProcess(name="ttbar",     samples=["TTbar"],     years=years, is_signal=False, has_rateParam=True),
    MCProcess(name="singletop", samples=["SingleTop"], years=years, is_signal=False),
    MCProcess(name="vjets",     samples=["Vjets"],     years=years, is_signal=False),
    MCProcess(name="diboson",   samples=["VV"],        years=years, is_signal=False),
])
```

`MCProcess` fields:

| field           | meaning                                                                                       |
|-----------------|-----------------------------------------------------------------------------------------------|
| `name`          | datacard process name (the per-year columns become `name_year`).                              |
| `samples`       | list of analysis sample names summed into this process.                                       |
| `years`         | years for which the process is defined.                                                        |
| `is_signal`     | `True` → process id ≤ 0 in Combine (signal). Required (no default).                           |
| `has_rateParam` | `True` → a free `rateParam` (`SF_<name>`, range `[0,5]`) floats this process. Default `False`.|
| `label`         | optional display label; defaults to `name`.                                                    |

`MCProcesses` is an ordered, name-indexable `dict` subclass. Its
`signal_processes` / `background_processes` / `n_processes` helpers all expand to
the per-year column names. Because it is a dict, you can build processes
programmatically — handy when splitting one physics process into several
template bins (e.g. by jet multiplicity or a categorisation index), each as its
own (optionally floating) process:

```python
template_bins = [
    ("bin_lo", ["BkgSample__bin_lo_a", "BkgSample__bin_lo_b"]),
    ("bin_mid", ["BkgSample__bin_mid_a", "BkgSample__bin_mid_b"]),
    ("bin_hi", ["BkgSample__bin_hi_a", "BkgSample__bin_hi_b"]),
]

for suffix, samples in template_bins:
    name = f"bkg_{suffix}"
    mc_processes[name] = MCProcess(
        name=name, samples=samples, years=years,
        is_signal=False, has_rateParam=True,
    )
```

### Data process

`DataProcess` declares the observation. Combine expects the name `data_obs`, and
**exactly one** data process is supported per datacard:

```python
data_processes = DataProcesses([
    DataProcess(
        name="data_obs",
        samples=["DATA_EGamma", "DATA_SingleMuon"],
        years=years,
    )
])
```

If `data_processes` is omitted, the card is written with `observation -1` (an
Asimov/blinded placeholder).

---

## 3. Systematic uncertainties

A `SystematicUncertainty` has these fields:

| field               | meaning                                                                                                       |
|---------------------|---------------------------------------------------------------------------------------------------------------|
| `name`              | internal identifier; also the default coffea variation name to look up.                                       |
| `typ`               | `"lnN"` or `"shape"`. **Note the spelling — the field is `typ`, not `type`.**                                  |
| `processes`         | list of process names the uncertainty applies to, **or** a dict `{process: value}` for per-process values.    |
| `years`             | years the uncertainty applies to (controls correlation — see below).                                          |
| `value`             | `lnN`: the κ factor (scalar, or `(down, up)` tuple). `shape`: conventionally `1.0`. Omit only when `processes` is a dict. |
| `datacard_name`     | optional override for the nuisance name written in the card (defaults to `name`).                             |
| `coffea_name_alias` | optional override for the variation name looked up in the histogram (string, or `{process: variation}` dict).  |

`Systematics` collects them; it keys on `datacard_name`, so each nuisance written
to a card must have a unique `datacard_name`.

### lnN — normalization

A flat log-normal. Scalar, asymmetric tuple, or per-process dict:

```python
# symmetric
SystematicUncertainty(name="lumi_2024", typ="lnN",
                      processes=[p.name for p in mc_processes.values()],
                      years=["2024"], value=1.016)

# asymmetric (down, up)
SystematicUncertainty(name="some_unc", typ="lnN", processes=["ttbar"],
                      years=years, value=(0.98, 1.05))

# per-process values in one nuisance (no `value` argument)
SystematicUncertainty(name="xsec", typ="lnN",
                      processes={"ttbar": 1.06, "diboson": 1.05},
                      years=years)
```

**Correlation is controlled by `years`.** Several `SystematicUncertainty`
objects that share the same nuisance `datacard_name` but cover different `years`
stay correlated within a group and uncorrelated across groups. A common pattern
is to correlate luminosity across eras of the same main year but decorrelate
different main years:

```python
lumi_systematics = {
    "2022_preEE": 1.014, "2022_postEE": 1.014,
    "2023_preBPix": 1.013, "2023_postBPix": 1.013,
    "2024": 1.016,
}

# group eras by main year (2022 / 2023 / 2024) ...
lumi_by_main_year = {}
for era, value in lumi_systematics.items():
    main_year = era.split("_")[0]
    lumi_by_main_year.setdefault(main_year, {"eras": [], "value": value})["eras"].append(era)

# ... and emit one correlated nuisance per main year
for main_year, data in lumi_by_main_year.items():
    sys_unc.append(SystematicUncertainty(
        name=f"lumi_{main_year}", typ="lnN",
        processes=[p.name for p in mc_processes.values()],
        years=data["eras"], value=data["value"],
    ))
```

Per-process cross-section normalizations, correlated across years but
uncorrelated between processes:

```python
norm = {"singletop": 1.05, "diboson": 1.05, "vjets": 1.05}

for process, value in norm.items():
    sys_unc.append(SystematicUncertainty(
        name=f"process_norms_{process}", typ="lnN",
        processes=[process], value=value, years=years,
    ))
```

### shape — template variations

A `shape` systematic points at an Up/Down histogram variation that the processor
already stored on the histogram's `variation` axis (`value=1.0` by convention).
For each process and shift the code looks up the coffea variation
`f"{coffea_name}{shift}"`; the variation **written to the card** is named
`f"{datacard_name}{shift}"`.

- `name` is the default coffea variation name.
- `datacard_name` renames the nuisance in the card.
- `coffea_name_alias` overrides the variation name read from the histogram —
  most usefully as a **dict**, when the same physical uncertainty is stored under
  different variation names for different processes.

If a requested variation is missing from a sample's histogram, the code falls
back to the nominal template (and prints a notice); if any variation differs
from nominal by more than 100% in some bin, a warning is printed
(`_check_shapes`).

Variations **correlated across years** use one nuisance covering all years:

```python
shapes_corr_by_year = [
    "sf_ele_reco", "sf_mu_id", "ele_scale", "muon_scale", "met_unclustered",
    # ...
]

for syst in shapes_corr_by_year:
    sys_unc.append(SystematicUncertainty(
        name=syst, typ="shape",
        processes=all_process_names, years=years, value=1.0,
    ))
```

Variations **decorrelated by year** are emitted once per year, folding the year
into the nuisance name while pointing back at the same coffea variation via
`coffea_name_alias`:

```python
shapes_split_by_year = ["pileup", "btag_stat"]

for syst in shapes_split_by_year:
    for year in years:
        sys_unc.append(SystematicUncertainty(
            name=f"{syst}_{year}", typ="shape",
            processes=all_process_names, years=[year], value=1.0,
            datacard_name=f"{syst}_{year}",   # per-year nuisance in the card
            coffea_name_alias=syst,           # but read the year-agnostic variation
        ))
```

**Per-process variation names** use `coffea_name_alias` as a dict. This is the
right tool when the same physical uncertainty is stored under different variation
names for different process groups (for example a flavour-split scale factor
where some processes carry a group-specific variation while the rest use a
generic one):

```python
# {process: variation_name_as_stored_in_the_histogram}
alias = {
    "ttbar":     "sf_btag_groupA",
    "singletop": "sf_btag_groupA",
    "vjets":     "sf_btag_groupB",
    "diboson":   "sf_btag_groupB",
}

sys_unc.append(SystematicUncertainty(
    name="sf_btag", typ="shape",
    processes=list(alias.keys()), years=years, value=1.0,
    coffea_name_alias=alias,
))
```

Multiple independent shape sources (e.g. the regrouped JES sources) are usually
generated from a dict mapping each variation name to the list of years it applies
to — the list encodes the correlation:

```python
jes_sources = {
    "JES_Absolute":      years,            # correlated across years
    "JER":               years,
    "JES_Absolute_2024": ["2024"],         # per-year, decorrelated
    # ...
}

for syst, syst_years in jes_sources.items():
    sys_unc.append(SystematicUncertainty(
        name=syst, typ="shape",
        processes=all_process_names, years=syst_years, value=1.0,
    ))
```

Collect everything:

```python
systematics = Systematics(sys_unc)
print("lnN:",   [s.name for s in systematics.values() if s.typ == "lnN"])
print("shape:", [s.name for s in systematics.values() if s.typ == "shape"])
```

---

## 4. Building the datacards

One `Datacard` is built per category. The constructor signature is:

| argument            | meaning                                                                                                |
|---------------------|--------------------------------------------------------------------------------------------------------|
| `histograms`        | the histogram for this category's variable (a value of `hist_dict`).                                    |
| `datasets_metadata` | `df["datasets_metadata"]`.                                                                              |
| `cutflow`           | `df["cutflow"]`; used for `data_obs` and to skip datasets empty in `presel`.                            |
| `years`             | years to include.                                                                                       |
| `mc_processes`      | the `MCProcesses` container.                                                                            |
| `systematics`       | the `Systematics` container.                                                                            |
| `category`          | the category (key of `hist_dict`) to slice from the histogram's category axis.                          |
| `data_processes`    | the `DataProcesses` container; default `None` (no observation row).                                     |
| `mcstat`            | `True` (default) adds an `autoMCStats` line; or pass a dict with `threshold` / `include_signal` / `hist_mode`. |
| `bins_edges`        | optional rebinning edges; `None` keeps native binning.                                                  |
| `bin_prefix`        | optional prefix for the Combine bin name.                                                               |
| `bin_suffix`        | optional suffix; defaults to `"_".join(years)`. The bin name is `[prefix_]category[_suffix]`.           |
| `verbose`           | `True` (default) prints diagnostics about skipped datasets / missing variations.                        |

```python
datacards = {}

for cat, histograms in hist_dict.items():
    datacard = Datacard(
        histograms=histograms,
        datasets_metadata=datasets_metadata,
        cutflow=df["cutflow"],
        years=years,
        mc_processes=mc_processes,
        data_processes=data_processes,
        systematics=systematics,
        category=cat,
        bin_suffix=label,
        bins_edges=bins_edges_dict[cat],
    )

    card_name   = f"datacard_{cat}_{label}.txt"
    shapes_name = f"shapes_{cat}_{label}.root"
    datacard.dump(directory=output_dir, card_name=card_name, shapes_name=shapes_name)
    datacards[card_name] = datacard
```

`Datacard.dump(directory, card_name, shapes_name)` writes the text card and a
ROOT file holding every `process_year_nominal` and `process_year_<nuis>{Up,Down}`
template into `directory`. Useful attributes/properties: `datacard.bin` (the
Combine bin name), `datacard.observation`, `datacard.rate(process)`.

`autoMCStats` is enabled by default with `threshold=0`, `include_signal=0`,
`hist_mode=1` (see the [Combine bin-wise stats docs](https://cms-analysis.github.io/HiggsAnalysis-CombinedLimit/latest/part2/bin-wise-stats/)).
Pass `mcstat=False` to disable it, or a dict to override the values.

A `rateParam` is emitted automatically for every **non-signal** process declared
with `has_rateParam=True`, as `SF_<name> rateParam * <name>_<year> 1 [0,5]`.

> **Formatting caveat.** The card columns are padded by process-name length. When
> processes carry long per-year suffixes the `rate`/systematic columns can render
> tightly; you may want to post-process the written card to widen the spacing.
> This is cosmetic — `combineCards.py`/`text2workspace.py` parse the card
> regardless.

<!-- ### Shape-only systematics for rateParam processes (extended feature)

> **Availability:** the `shape_only_for_rateparam` / `rateparam_norm_categories`
> arguments seen in some configs are **not present in the `Datacard` constructor
> on this branch** — passing them here raises `TypeError`. They belong to a newer
> revision of `utils/stat/combine.py`. The description below documents the
> intended behaviour for when that revision is in use.

When a process carries a free `rateParam`, its overall normalization already
floats, so the *normalization component* of any `shape` systematic on it is
degenerate with the rateParam and only adds redundant nuisance freedom. With
`shape_only_for_rateparam=True`, every Up/Down template of a
`has_rateParam=True` process is rescaled so its integral matches nominal —
keeping only the bin-to-bin shape and the migration *between* regions. The
normalization factor `Σnominal / Σvaried` is computed per
`(process, datacard_name, shift)`, summed flow-inclusive over
`rateparam_norm_categories` × the process's applicable years. Pass the explicit
list of fit categories as `rateparam_norm_categories` when the coffea output also
contains non-fit categories. -->

---

## 5. Combining categories

`combine_datacards` writes a shell script that runs `combineCards.py` over all
per-category cards and then `text2workspace.py` to produce the workspace:

```python
combine_datacards(
    datacards,                               # {filename: Datacard}
    directory=output_dir,
    path=f"combine_datacards_{label}.sh",    # script to write (must end in .sh)
    card_name=f"datacard_combined_{label}.txt",
    workspace_name=f"workspace_{label}.root",
    channel_masks=False,
)
```

The generated script combines the cards using each datacard's `bin` name as the
channel label (`combineCards.py <bin>=<filename> ... > combined.txt`) and then
builds the workspace. Set `channel_masks=True` to append `--channel-masks` to
`text2workspace.py` (useful for blinding or fitting subsets of categories).

Run the generated `.sh` inside a CMSSW + Combine environment to obtain the final
workspace, which is then passed to `combine`.
