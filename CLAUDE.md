# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

PocketCoffea is a configuration framework for CMS NanoAOD analyses built on top of [Coffea](https://github.com/CoffeaTeam/coffea/). Analyses are declared in Python configuration files; customization beyond configuration is done by subclassing the base processor.

Documentation: https://pocketcoffea.readthedocs.io

## Install & dev environment

```bash
pip install -e .[dev,test,docs]
```

Pre-commit is configured (`black`, `flake8`, `isort`, `pyupgrade`, mypy, codespell, shellcheck, and a local `disallow-caps` hook that bans `PyBind|Numpy|Cmake|CCache|Github|PyTest`). The CI container is `gitlab-registry.cern.ch/cms-analysis/general/pocketcoffea:lxplus-el9-*`; tests run against EOS datasets via a grid proxy, so most full-config tests will not work locally without `voms-proxy-init`.

## Common commands

```bash
# Run an analysis (main entry point)
pocket-coffea run --cfg config.py -o output_dir
# or equivalently
runner --cfg config.py -o output_dir

# Quick local test (limit-files=1, iterative executor)
runner --cfg config.py -o output_dir --test

# Build datasets JSON from DAS / Rucio
pocket-coffea build-datasets --cfg datasets/dataset_definitions.yaml
pocket-coffea dataset-discovery-cli   # interactive

# Plotting / merging / post-processing
make-plots ...
hadd-skimmed-files ...
merge-outputs ... / split-output ...
check-jobs ...
print-parameters ...           # dump the merged OmegaConf parameter tree

# Tests
pytest tests                                              # full suite
pytest tests/test_calibrators.py                          # one file
pytest tests/test_full_configs/test_full_configs.py::test_new_weights -x
nox -s tests        # run via nox in an isolated venv
nox -s lint         # pre-commit on all files
nox -s docs         # build sphinx docs into docs/_build

# Docs (manual)
cd docs && make html
```

All CLI scripts are declared in `pyproject.toml` under `[project.scripts]` and the top-level `pocket-coffea` command (defined in `pocket_coffea/__main__.py`) aggregates them as `click` subcommands.

## Architecture

The processing pipeline is centered on **`BaseProcessorABC`** in `pocket_coffea/workflows/base.py`, a `coffea.processor.ProcessorABC` driven by a **`Configurator`** (`pocket_coffea/utils/configurator.py`). A user analysis = a config file that instantiates a `Configurator` with a workflow class plus declarations for skims, preselections, categories, weights, variations, variables, datasets, calibrators, columns.

Per-chunk `process()` flow (see `workflows/base.py`):

1. `load_metadata` — pulls dataset/sample/year/isMC/era/nano_version from `events.metadata`. Subclasses override `load_metadata_extra` to add more.
2. `skim_events` — runs `self._skim` `Cut`s on **raw NanoAOD before any object correction**; triggers belong here. Counts go into `cutflow.skim`. If `save_skimmed_files` is set, `export_skimmed_chunk` writes a ROOT file (rescaling `skimRescaleGenWeight` so cross sections still match downstream).
3. **Calibration loop** — `CalibratorsManager.calibration_loop()` (`pocket_coffea/lib/calibrators/`) iterates systematic variations. For each variation: apply object corrections → `apply_object_preselection` (abstract) → `count_objects` (abstract) → `define_common_variables_*` hooks → `apply_preselections` → categorize → fill weights & histograms.
4. `skim_mode: "presel_any_variation"` (in `workflow_options`) is a special mode: keep events that pass preselection in *any* active calibrator variation (OR of per-variation masks), and skim accordingly. `skip_processing_after_skim` short-circuits the rest. See `PLAN.md` for design notes.
5. Histograms & weights → `HistManager` (`lib/hist_manager.py`) and `WeightsManager` (`lib/weights/weights_manager.py`); columns dumped via `ColumnsManager`.

**Extensibility hooks** — `BaseProcessorABC` exposes `*_extra` methods (`process_extra_*`, `fill_histograms_extra`, `define_common_variables_before_presel`, etc.) so subclasses inject custom logic without rewriting the pipeline. Concrete processors live alongside the base in `pocket_coffea/workflows/` (e.g. `tthbb_base_processor.py`, `semileptonic_triggerSF.py`).

### Key modules

- `pocket_coffea/workflows/` — `BaseProcessorABC` and concrete subclasses.
- `pocket_coffea/utils/configurator.py` — the `Configurator` that wires everything together; `utils/utils.py::load_config` loads a config `.py` script.
- `pocket_coffea/lib/`
  - `calibrators/` — `CalibratorsManager` and per-object calibrators (JEC/JER, MET, Rochester, etc.); legacy txt-based JEC under `calibrators/legacy/`.
  - `weights/` — `WeightsManager` and the registry of weight classes; new weights subclass `Weight` in `lib/weights/weights.py`.
  - `hist_manager.py`, `columns_manager.py`, `categorization.py`, `cut_definition.py`, `cut_functions.py`.
  - `objects.py`, `jets.py`, `leptons.py`, `triggers.py`, `scale_factors.py`, `reconstruction.py`, `parton_provenance.py`.
- `pocket_coffea/executors/` — one module per site (`executors_lxplus.py`, `executors_DESY_NAF.py`, etc.) providing a factory; pick via `--executor` or the config. `executors_base.py` defines the iterative/futures executors used by tests. `executors_manual_jobs.py` (`ExecutorFactoryManualABC`) is the base for HTCondor "manual" executors (`condor@lxplus`, `condor@rubin`) — see "Manual-job executors" below.
- `pocket_coffea/parameters/` — YAML+OmegaConf defaults (lumi, btagging, jets calibration, trigger, plotting style, etc.); merged with user overrides through `parameters/defaults.py`. Executor option defaults live in `parameters/executor_options_defaults.yaml`.
- `pocket_coffea/scripts/` — CLI implementations (`runner.py`, `dataset/`, `plot/`, `merge_outputs.py`, `check_jobs.py`, ...).
- `pocket_coffea/utils/site_rewrite.py` — helpers (`find_other_file`, `rewrite_fileset_blocklist`, `_query_replicas`) used by recreate-jobs / check-jobs to migrate file URLs between CMS sites. Uses Rucio (`rucio.Client.list_replicas`) — **not** `dasgoclient`, which isn't always on PATH. Rucio is lazy-imported so the module stays unit-testable without it.
- `pocket_coffea/law_tasks/` — optional [law](https://github.com/riga/law) workflow tasks.

### Manual-job executors

`ExecutorFactoryManualABC` (`executors/executors_manual_jobs.py`) submits one self-contained HTCondor job per chunk-group instead of streaming work through a Dask scheduler:

1. `prepare_splitting()` slices the fileset. Two modes: **uniform** (single `max-events-per-job` scalar, jobs may mix datasets) or **per-sample** (dict with optional `default`, each dataset is split independently so every job carries one sample). `scaleout: N` is a third path that derives the budget from total events. **Files are atomic** in both `_split_uniform` and `_split_per_sample`: a single file with more events than the per-job limit becomes one oversized job (coffea's `chunksize` still bounds memory inside it, but not wall-time). Splitting across condor jobs at sub-file granularity would require encoding entry ranges via coffea's `steps`; not implemented today.
2. `prepare_jobs()` writes `jobs_dir/config_job_{i}.pkl` (a cloudpickled `Configurator` with that job's fileset slice) plus `jobs_dir/jobs_config.yaml` summarising every job's slice/config/output paths.
3. `submit_jobs()` writes `job_{i}.sub` and the wrapper `job.sh`, then `condor_submit`. The wrapper toggles per-job flag files: `.idle → .running → .done | .failed`.
4. **Resubmission** has two entrypoints: `pocket-coffea run --recreate-jobs <ids|auto>` (one-shot, manual; supports `--blocklist-sites`, `--recreate-queue`) and `pocket-coffea check-jobs --resubmit` (long-running babysitter that reacts to `.failed` flags + log analysis). Both rewrite the per-job pickle in place when needed — they never re-read the dataset JSON.

Concrete subclasses live in `executors_lxplus.py` (`ExecutorFactoryCondorCERN`) and `executors_rubin.py`. Anything depending on `+JobFlavour` / lxplus queues lives in `executors_lxplus.py` (`update_queue`, `set_queue`, the `queues` ladder).

### Tests

- `tests/test_full_configs/<name>/config.py` — full mini-analyses run via `coffea.processor.Runner` with the `iterative` executor. Each test compares the new output against a reference `.coffea` baseline (`reference_commit` constant in `test_full_configs.py`); update the baseline only when behavior changes intentionally. Helpers `compare_outputs` and `compare_totalweight` live in `tests/utils.py`.
- Unit-style suites: `test_calibrators.py`, `test_categorization.py`, `test_cut_functions.py`, `test_hlt_cut.py`, `test_runner.py`, `test_weights/`, plus `test_manual_jobs_splitting.py` and `test_recreate_jobs_blocklist.py` for the manual-job paths (no rucio/dask needed — they exercise the dependency-free helpers directly).
- Many tests require xrootd/EOS access (grid proxy). They're designed to run inside the GitLab CI image.
- **Testing convention for helpers that talk to external services** (Rucio, DAS, xrootd, HTCondor): factor the network call into a small monkey-patchable function (e.g. `site_rewrite._query_replicas`) and patch *that*, not the underlying library. The host modules should lazy-import the heavy dependency inside the function so the module remains importable on a bare CI runner.

## Conventions / gotchas

- **Skim cuts must not depend on corrected objects.** Object corrections happen inside the calibration loop, after skimming.
- `_isMC`, `_isSkim`, etc. come from `events.metadata` and may arrive as strings — see `load_metadata` for the canonical coercion.
- For MC, `nano_version` defaults from `params.default_nano_version[year]` if metadata is missing, with heuristic detection from the filename (`NanoAODv12` / `NanoAODv15`).
- Configs are saved (pickled with cloudpickle) into the output dir at run start; passing a `.pkl` to `runner` reuses a saved config.
- **`--process-separately`, `--resubmit-failed`, and `--group-samples` are silently ignored by manual-job (`condor@*`) executors.** Those flags only fire after `runner.py:264` exits early on `executor_factory.handles_submission == True`. The manual-job equivalent of `--resubmit-failed` is `--recreate-jobs auto`.
- **Merging already-merged `.coffea` files double-rescales histograms.** `BaseProcessorABC.postprocess` runs `rescale_sumgenweights` unconditionally (`histo *= 1/sum_genweights`), but `sum_genweights` itself isn't touched, so a second `merge-outputs` pass re-divides already-rescaled histograms. Merge raw `output_job_*.coffea` files in a single call.
- **`hadd-skimmed-files --check` validates an existing hadd run.** It does not run hadd — it iterates the workload it just rebuilt and validates each expected output file (existence + `TFile.Open` + `Events` tree + `GetEntries` vs. the expected sum from `df["nskimmed_events"]`; falls back to existence-only if ROOT is unimportable). Output paths that start with `root://host/` are stripped via `_strip_xrootd_prefix` so the existence probe hits the FUSE-mounted EOS path; ROOT.TFile.Open still receives the original URL. On failure it writes the triple `hadd_failed.json` / `hadd_failed.txt` / `hadd_failed_splitbyfile.sub` and (re)writes `do_hadd_job_splitbyfile.py`. The wrapper accepts an optional 3rd argv naming the JSON file (defaults to `hadd.json`), and the resubmit sub passes `hadd_failed.json` as that 3rd arg — so the original submission is untouched and a stale on-disk wrapper from before this change is overwritten automatically.
- **Shape-only shape systematics for rateParam processes** (`utils/stat/combine.py`). A free `rateParam` already floats a process's normalization, so the normalization component of any `shape` systematic on that process is degenerate with it and just adds redundant nuisance freedom. Opt in with `Datacard(shape_only_for_rateparam=True, rateparam_norm_categories=[...])`: every Up/Down template of a `MCProcess` with `has_rateParam=True` is rescaled so its total yield matches nominal, keeping only the bin-to-bin shape and the migration *between* regions. The factor `Σnominal/Σvaried` is computed per `(process, systematic.datacard_name, shift)` in `Datacard.compute_rateparam_shape_scales()` (called from `dump()`), summed with `flow=True` over `rateparam_norm_categories` × the process years where the systematic applies — restricted by the same `process in systematic.processes and year in systematic.years` guard used when writing. Flow-inclusive totals are observable-independent, so each per-category `Datacard` derives the *same* factor without cross-card coordination: `rearrange_histograms` copies with `flow=True`, so content that a sub-range rebinning (e.g. spanet ∈ [0.8, 1]) pushed into under/overflow still counts in the factor (written templates and rates remain in-range only). Requires only that the histograms used for the totals are filled in every norm category: by default the card's own, or pass `rateparam_norm_histograms=df["variables"]["nJets"]` (any variable filled everywhere, never rebinned) when the card's variable uses `only_categories` and lacks some norm category — a missing category raises a `ValueError` naming that option instead of a bare `KeyError`. Regression tests: `tests/test_stat_datacards.py::test_rateparam_scale_consistent_across_rebinned_cards` and `::test_rateparam_scale_from_norm_histograms`. `rateparam_norm_categories` defaults to every category on the input axis; pass the explicit fit-category list when the coffea output holds non-fit categories. Default off — existing datacards unchanged.
- **Artificial variations injected into histograms** (`utils/stat/shape_manipulation.py`). Both helpers add an extra `{name}Up`/`{name}Down` entry to the `variation` StrCategory axis of the coffea histograms (`{sample: {dataset: hist}}`) *before* the `Datacard` is built, to be declared as a regular `shape` `SystematicUncertainty` with the same name; samples of the process that don't carry the variation fall back to nominal in `rearrange_histograms`, and both return a new dict (untouched samples shared by reference). `add_norm_variation`: Up = nominal × per-category scalar factor (pure normalization, flow bins included). `add_binwise_variation`: Up = nominal reweighted bin-by-bin along the last (variable) axis with per-category weight arrays — length `axis.size` (flow keeps weight 1) or `axis.extent` (flow weighted too); scalars behave like the norm case; weights refer to the binning *before* `bins_edges` rebinning. Down = nominal by default (one-sided); `down_mode="mirror"` divides instead. `preserve_norm=True` rescales each varied template per dataset+category (flow included) so its yield matches nominal — a pure within-category shape variation with no normalization component (scalar entries become no-ops); this also makes the rateParam shape-only factor exactly 1 in every card, so the variation need not be injected into other cards' variables. Both helpers take `datasets=[...]` to restrict the injection (e.g. one year's datasets from `datasets_metadata["by_datataking_period"][year][sample]`): call once per year with different weights — same `variation_name` composes across disjoint dataset sets, overlaps raise, uncovered datasets fall back to nominal in the Datacard. Caveats with `has_rateParam=True` + `shape_only_for_rateparam=True`: only *relative* per-category differences survive (equal scalar factors everywhere = no-op), and for per-bin weights the cross-card factor is only identical if the arrays given for each card's variable represent the same event-level reweighting (same varied total per category) — see the warning in the `add_binwise_variation` docstring. `rescale_histograms(histograms, samples, scale, datasets=None)` is different: it multiplies the selected histograms themselves by one scalar (all categories, nominal and every variation, flow included; variances × scale²) — a plain yield change, no new variation entry — with the same `samples`/`datasets` selection rules (shared `_select_datasets` helper).
- **`clean_failed_hadd_inputs.py`** is a small helper that reads a hadd manifest (`hadd.json` or `hadd_failed.json`) and runs `clean_skim_branches.clean_file` directly on each output, mirroring `<output_dir>/<dataset>/<basename>`. Despite the name, it operates on the hadd *outputs* — one cleaned file per group, no per-chunk fan-out.
- The repository contains many stale backup files (`*.py~`, `*.yaml~`, `*.json~`, `out_test/`, `output_tests/`, `myenvtest/`, `venv_local_docs/`) — ignore them; the canonical files are the un-tilded ones.
- Mirror: this repo is mirrored to `https://gitlab.cern.ch/cms-analysis/general/PocketCoffea` and CI/Docker images are built there, not on GitHub.
