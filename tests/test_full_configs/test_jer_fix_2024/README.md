# JER fix: before/after comparison on 2024 data and ttbar MC

Configuration to compare the jet calibration before and after the JER gen-matching fix
(branch `fix/jer-gen-match`, see `docs/jerc_chain_comparison.md`) on one 2024 data sample
(EGamma, era F, NanoAODv15) and one 2024 ttbar MC sample (Summer24 NanoAODv15).

Jets are selected with pt > 20 GeV and |eta| < 5 (see `params/object_preselection.yaml`),
because the fix acts mostly on low-pt forward jets. Only JES_Total and JER are varied.

`config_central.py` is the same configuration with the analysis-like jet preselection
(pt > 30 GeV, |eta| < 2.4, `params/object_preselection_central.yaml`): use it to see the
effect on the central jets only (expected: percent level, mostly the dR < R/2 migration).
Run it with the same commands, replacing `config.py` and the output directories.

## 1. Build the dataset files (once, on lxplus with a grid proxy)

Check the DAS names in `datasets/datasets_definitions.json` first (`dasgoclient -query="dataset=..."`),
then:

```bash
cd tests/test_full_configs/test_jer_fix_2024
pocket-coffea build-datasets --cfg datasets/datasets_definitions.json -o -rs 'T[123]_(FR|IT|DE|BE|CH|UK)_\w+'
```

This writes `datasets/datasets_2024.json`. The cross section of the MC sample is only used
as a weight and does not matter for the comparison.

## 2. Run before and after

Same commands on `main` (before) and on `fix/jer-gen-match` (after); one file and a few
chunks per sample are enough:

```bash
# before
git checkout main
pocket-coffea run --cfg config.py -o out_before --executor iterative --limit-files 1 --limit-chunks 4 --chunksize 50000
# after
git checkout fix/jer-gen-match
pocket-coffea run --cfg config.py -o out_after  --executor iterative --limit-files 1 --limit-chunks 4 --chunksize 50000
```

Both outputs contain, for the nominal and for the JES_Total / JER up/down variations:

* histograms: `JetGood_pt`, `JetGood_eta`, `Jet1_*`, `Jet2_*`, `nJetGood`,
  `JetGood_pt_eta` (2D), `JetGood_pt_over_raw_eta` (2D: JEC x JER factor vs eta),
  `MET_pt`, `MET_phi`;
* columns per event: `event`, `run`, `luminosityBlock`;
* columns per jet (`Jet` = all jets, `JetGood` = selected jets): `pt`, `eta`, `phi`,
  `mass`, `pt_raw`, `pt_gen` (-1 = no gen match after the fix), `pocket_sortidx`
  (position of the jet in the NanoAOD collection: use it with `event` to match the same
  jet between the two runs, whose pt ordering can differ).

## 3. Compare

Same jet in both runs: match on (`event`, `pocket_sortidx`). Expected differences:

* MC: jets with `pt_gen = -1` after the fix and a large resolution (forward, low pt) change
  from `pt = SF * pt_JES` to a stochastic smearing; jets whose gen match had
  0.2 < dR < 0.4 move from the scaling to the stochastic method. The JES_Total shifts
  change by the (small) difference between delta(pt_JES) and delta(pt_smeared).
* Data: no change at all (no JER on data) — this is the control.

## 4. Comparison script

```bash
python compare_outputs.py out_before/output_all.coffea out_after/output_all.coffea -o cmp_jer_fix
```

It matches the same jet in the two runs on (`event`, `pocket_sortidx`) and prints, per
sample and variation, the fraction of jets whose pt changed and the pt ratio per |eta|
region, the migration of the gen-match flag, and the sum / max deviation of every
histogram; it saves before/after/ratio plots (1D histograms, and the 2D ones projected per
eta bin) in the output directory.
