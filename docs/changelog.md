# Changelog

This page keeps track of major and minor changes between versions. Breaking changes are also reported

## PocketCoffea 0.9.11

### New features

- **Weights by subsamples** ([#294](https://github.com/PocketCoffea/PocketCoffea/pull/294)): per-subsample weights, inclusive and by-category, with full variation (up/down) support. Subsample-specific weight variations are correctly isolated per subsample in the histogram variation axis.
- **Shape variations by subsamples** ([#330](https://github.com/PocketCoffea/PocketCoffea/pull/330)): subsample-specific shape (calibrator) variations. Each subsample can carry a different set of shape variations; histogram axes are built per-subsample so the variation labels are correctly isolated.
- **Fix histogram filling for subsample weight and shape variations** ([#491](https://github.com/PocketCoffea/PocketCoffea/pull/491)): several bugs in `HistManager` were fixed — wrong variation axis used in the fill loop when subsamples are configured, subsample-specific shape variations silently skipped during filling, and subsample nominal weights not applied during shape-variation passes. Comprehensive integration tests added.
- **Variations-aware skimming** ([#467](https://github.com/PocketCoffea/PocketCoffea/pull/467)): the skim step now retains events that pass at least one preselection variation (not just the nominal), ensuring shape-variation passes do not lose events skimmed away by the nominal selection.
- **MET Type-1 correction calibrator** ([#472](https://github.com/PocketCoffea/PocketCoffea/pull/472)): new calibrator for MET Type-1 propagation, including unclustered energy systematics. Extended to Run2.
- **Multi-dimensional variables** ([#374](https://github.com/PocketCoffea/PocketCoffea/pull/374)): `HistConf` now supports multi-dimensional delayed-evaluation variables.
- **Jet pT regression** ([#461](https://github.com/PocketCoffea/PocketCoffea/pull/461)): jet pT regression support in the jet calibrator, with `merge_collections_for_variations` and `collection_name_alias` options to merge regressed and standard jet collections.
- **JetID from correctionlib** ([#421](https://github.com/PocketCoffea/PocketCoffea/pull/421), [#395](https://github.com/PocketCoffea/PocketCoffea/pull/395)): JetID delivered via correctionlib, replacing hardcoded values.
- **msoftdrop correction** ([#420](https://github.com/PocketCoffea/PocketCoffea/pull/420)): soft-drop mass correction for AK8 jets.
- **Default JEC via correctionlib** ([#401](https://github.com/PocketCoffea/PocketCoffea/pull/401), [#418](https://github.com/PocketCoffea/PocketCoffea/pull/418)): JEC/JER now use correctionlib as the default backend.
- **Read unpublished remote datasets over XRootD** ([#461](https://github.com/PocketCoffea/PocketCoffea/pull/461)): dataset builder can now discover and read files from unpublished XRootD endpoints.
- **`merge_columns` CLI command** ([#482](https://github.com/PocketCoffea/PocketCoffea/pull/482)): new `pocket-coffea merge-columns` sub-command to merge columnar outputs.
- **CCsorted function** ([#456](https://github.com/PocketCoffea/PocketCoffea/pull/456)): utility to sort collections by a given field with stable ordering.

### Scale factors and corrections

- Muon promptMVA scale factors, including ISO SF for 2024 ([#470](https://github.com/PocketCoffea/PocketCoffea/pull/470))
- Electron promptMVA scale factors with promptMVA ID ([#474](https://github.com/PocketCoffea/PocketCoffea/pull/474))
- Updated BTV scale factors for 2024 ([#473](https://github.com/PocketCoffea/PocketCoffea/pull/473))
- Lepton (electron and muon) scale factors for 2024 ([#466](https://github.com/PocketCoffea/PocketCoffea/pull/466))
- Electron trigger scale factor added to common weights for Run3 ([#476](https://github.com/PocketCoffea/PocketCoffea/pull/476)); `sf_ele_trigger` renamed for Run2 consistency ([#481](https://github.com/PocketCoffea/PocketCoffea/pull/481))
- Updated electron scale factors and electron SS energy scale/smearing ([#444](https://github.com/PocketCoffea/PocketCoffea/pull/444), [#414](https://github.com/PocketCoffea/PocketCoffea/pull/414), [#419](https://github.com/PocketCoffea/PocketCoffea/pull/419))
- Muon calibrator update ([#432](https://github.com/PocketCoffea/PocketCoffea/pull/432))
- **Rochester muon momentum corrections** ([#475](https://github.com/PocketCoffea/PocketCoffea/pull/475)): new `MuonsRochesterCalibrator` for Run 2 Ultra-Legacy muon pT corrections using the RoccoR library; correction files included for 2016 pre/post-VFP, 2017, and 2018.
- Muon SF lower pT bound for Run3 set to 26 GeV; electron to 25 GeV ([#476](https://github.com/PocketCoffea/PocketCoffea/pull/476))
- Jets for NanoAODv12 ([#355](https://github.com/PocketCoffea/PocketCoffea/pull/355))
- JetVetoMap fix ([#430](https://github.com/PocketCoffea/PocketCoffea/pull/430))
- FatJet ID fix and 2024 AK8 support ([#429](https://github.com/PocketCoffea/PocketCoffea/pull/429), [#440](https://github.com/PocketCoffea/PocketCoffea/pull/440))
- Updated JEC data tags for 2022/2023 ([#489](https://github.com/PocketCoffea/PocketCoffea/pull/489)); JEC configuration for 2023 preBPix MC
- Lepton mvaTTH discriminant added to lepton objects ([#455](https://github.com/PocketCoffea/PocketCoffea/pull/455))
- UParT tagger scores added to `jet_taggers_hists()` ([#460](https://github.com/PocketCoffea/PocketCoffea/pull/460))
- Updated luminosity values for 2023 and 2024; golden JSON updated for 2022, 2023, 2024 and added for 2025 ([#441](https://github.com/PocketCoffea/PocketCoffea/pull/441), [#490](https://github.com/PocketCoffea/PocketCoffea/pull/490))
- Updated correctionlib tags ([#415](https://github.com/PocketCoffea/PocketCoffea/pull/415))

### Plotter and output improvements

- Plotter: correct handling of subsamples and weight/shape variations ([#442](https://github.com/PocketCoffea/PocketCoffea/pull/442))
- Plotter: fix systematic band rendering ([#408](https://github.com/PocketCoffea/PocketCoffea/pull/408))
- Save shape variation arrays to output ([#368](https://github.com/PocketCoffea/PocketCoffea/pull/368))
- Cutflow: major rework with subsample support and various fixes ([#369](https://github.com/PocketCoffea/PocketCoffea/pull/369)); fix for cutflow with subsamples in plot scripts; fix handling of variations in sum-of-weights
- Datacard output support ([#407](https://github.com/PocketCoffea/PocketCoffea/pull/407))
- New analysis metadata stored in output ([#409](https://github.com/PocketCoffea/PocketCoffea/pull/409))

### LAW integration

- Updated LAW task parameters and `skipbadfiles` option ([#464](https://github.com/PocketCoffea/PocketCoffea/pull/464))
- LAW tasks for plotting ([#393](https://github.com/PocketCoffea/PocketCoffea/pull/393))
- Dask scheduler support in runner ([#403](https://github.com/PocketCoffea/PocketCoffea/pull/403), [#411](https://github.com/PocketCoffea/PocketCoffea/pull/411))
- Updated dataset-definition parameter handling ([#464](https://github.com/PocketCoffea/PocketCoffea/pull/464))

### Bug fixes

- Fixed priority list not being passed to dataset builder ([#485](https://github.com/PocketCoffea/PocketCoffea/pull/485))
- NanoAOD v15 compatibility for Run 2 UL ([#457](https://github.com/PocketCoffea/PocketCoffea/pull/457)): `rho` field access, PU jet ID, and JER now use the correct NanoAOD-version-aware branch paths (`Rho.fixedGridRhoFastjetAll` for v12+, `fixedGridRhoFastjetAll` for ≤v9); PUid selection correctly disabled for NanoAOD v15.
- Fixed jets variations for AK8Puppi collection
- Fixed jets variations and config when using `collection_name_alias`
- Fixed legacy calibrator raw-factor NaN ([#428](https://github.com/PocketCoffea/PocketCoffea/pull/428))
- Fixed filesets configuration exception message with filter details ([#448](https://github.com/PocketCoffea/PocketCoffea/pull/448))
- Fixed histogram filling for `only_variations` histograms ([#392](https://github.com/PocketCoffea/PocketCoffea/pull/392))
- Fixed calibrators variations handling ([#389](https://github.com/PocketCoffea/PocketCoffea/pull/389))
- Fixed muon SF for Run3 ([#397](https://github.com/PocketCoffea/PocketCoffea/pull/397))
- Fixed `dump_ak_array` for XRootD mkdir ([#448](https://github.com/PocketCoffea/PocketCoffea/pull/448))
- Improved `hadd_skimmed_files`, `check_jobs`, and `merge_output` scripts

---

## PocketCoffea 0.9.6

- Minor release: only fixes and small improvements
  - Some fixes in the plotting script
  - Improvements for the INFN analysis facility executor

## PocketCoffea 0.9.5

- Generalize handling of common Weights and user-defined Weights
- Added first integration of LAW tasks to stear an analysis running PocketCoffea workflows
- Many improvements in the plotting scripts
- Added Dataset discovery cli to dynamically query for dataset and build the dataset definition file
- Cleaning up of the default skimming function (**Breaking changes!** see below)
- Added CDCI tests of utils and full configuration tests
- New parameters exploration CLI 
- Added more executors
- Tested the Swan AF and INFN AF


#### Breaking changes

##### Default skim
- Some skimming cuts were included by default in the base workflow and may be unnoticed by the users
[PR#193](https://github.com/PocketCoffea/PocketCoffea/pull/193). For maximum transparency we have removed those cuts
from the base workflow and made the cutting functions available to be used in the configuration.  The functions were: 

  - nPV_good > 0 selection
  - goldenJson
  - event flags for data and MC. 

Users **must now include** the following cut functions in their `skim` configuration to keep the same cutflow in their
analysis:
```python
from pocket_coffea.lib.cut_functions import get_nPVgood, goldenJson, eventFlags

cfg = Configurator(skim=[get_nPVgood(1), eventFlags, goldenJson])
```

This change is enough to preserve the same cuts applied before 0.9.5.

##### Jet selection function
The signature of the jet cleaning function used often in the object preselection step of processor has been changes to
add explicitely the year argument. 

```diff
- def jet_selection(events, jet_type, params, leptons_collection=""):
+ def jet_selection(events, jet_type, params, year, leptons_collection=""):
```

This signature change can be unnoticed by users using the `leptons_collection` argument. Please cross-check your
function usage.


## PocketCoffea 0.9.0

-  New executor plugin setup to support multiple sites and analysis facilities
-  New defaults for Run3 corrections
-  Improve CLI interface and unified scripts under pocket-coffea command
-  Added dataset-discovery-cli to build dataset_definition files


## PocketCoffea 0.7

- Improved and generalized executors configuration for runner.py

## PocketCoffea 1.0rc0
The main change is the possibility to completely split the analysis configuration files and parameters from the core of
the framework.

- Parameters configuration now in place with OmegaConf
- Docker and singularity images built with GitLab CD/CI and published on
  `/cvmfs/unpacked.cern.ch/gitlab-registry.cern.ch/cms-analysis/general`
- Improved the handling of datasets and samples
- Added dataset metadata in the output file

