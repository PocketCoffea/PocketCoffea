# JES/JER chain: PocketCoffea vs the JERC reference and pepper

Comparison of the jet energy scale (JES) and resolution (JER) chain implemented in
`pocket_coffea/lib/jets.py::jet_correction_corrlib` with:

* the JERC application tutorial, the reference implementation of the JME POG:
  <https://gitlab.cern.ch/cms-analysis/jme/jerc-application-tutorial> (`JecApplication.cpp`,
  `ApplyOnNanoAOD/*.hpp`, `jer_smear.json.gz`);
* pepper: <https://gitlab.cern.ch/cms-analysis/general/pepper/pepper>
  (`pepper/processor_basic.py`, `compute_jec_factor` / `compute_junc_factor` / `compute_jer_factor`).

PocketCoffea line numbers refer to `main` at `436e57d3` (before the fixes described in the
last section); the reference code is the `master` of both repositories (September 2026).

## 1. The chain, step by step

| Step | JERC tutorial | pepper | PocketCoffea (before the fixes) |
|---|---|---|---|
| Raw pt | `pt·(1−rawFactor)` (`JecApplication.cpp:78-79`) | same (`compute_jec_factor`) | same (`jets.py:31`) |
| JES nominal | L1FastJet → L2Relative → L2L3Residual (data only), each level evaluated at the pt updated by the previous one (`JecApplication.cpp:84-116`) | correctionlib compound, chained (`CorrLibCompoundSFs.__call__`) | correctionlib compound `cset.compound[<tag>_L1L2L3Res_<jet_type>]` (`jets.py:766-778`). Same as the reference; the residual is 1 in MC. |
| JES inputs | area, eta, pt, rho; phi for 2023Post and later; run for the data residual | same | same, through `eval_dict` (`jets.py:754-761`) |
| JES uncertainty δ(η, pt) | evaluated at the pt **after the nominal JES**, applied **before** the JER (`ApplicationHelper.hpp:136-141`, `applyJecAndJvm.C:252-271`) | evaluated at the NanoAOD pt (`compute_junc_factor`, `pt=None`) | evaluated at the pt **after the JER smearing** (`jets.py:840`), applied on the smeared pt |
| JER inputs σ(η, pt, ρ), SF(η, pt) | pt after JES (`JecApplication.cpp:156-160`) | NanoAOD pt | pt after JES (`jets.py:793`) — same as the reference |
| JER SF uncertainty | `SF·(1 ± unc)` (`JecApplication.cpp:171-178`) | same | same (`jets.py:673-679`) |
| Gen-jet match (scaling vs stochastic) | `genJetIdx` **and** `dR < 0.2` (AK4) **and** `|pt − pt_gen| < 3·σ·pt` (`JecApplication.cpp:189-201`, `CollectJetMet.hpp:245`) | `matched_gen` **and** `dR < R/2` **and** 3σ (`find_matched_genjet`) | `matched_gen` **and** 3σ (`jets.py:36`, `805-815`). **No dR < R/2 requirement.** |
| No-match encoding | `hasGen` flag; `GenPt = −1` (`JecApplication.cpp:190-196`) | `is_none` mask | `pt_gen = 0` for no match (`jets.py:36-38`) |
| Random number | `hashprng(JetPt, JetEta, Rho, EventID)` (`jer_smear.json.gz`) | numpy RNG, seed from a file | `hashprng`, same inputs and same formulas as the tutorial (`jets.py:604-637`) |
| Guard on the smear factor | `smear ≤ 0` or not finite → 1 (`JecApplication.cpp:214`) | none | none (`jets.py:820-831`) |
| Mass | pt only (tutorial) | same factor as the pt | same factor as the pt (`jets.py:778, 821, 862`) |
| Re-sort by pt | n/a | yes | yes (`JetsCalibrator.calibrate`) |

Steps that agree with the reference: raw pt, the chained nominal JES, the JES inputs, the
JER inputs, the JER SF uncertainty, the random number generator and the smearing formulas.

The tutorial also ships a python implementation (`ApplyOnNanoAOD/ForPythonUser/JecApplication.py`,
correctionlib python bindings, vectorized). It has the same chain as the C++ with two
details worth noting: the AK8 gen-match radius is 0.4 (R/2, as in PocketCoffea; the C++
NanoAOD helper uses 0.6), and there is **no guard** on the smear factor
(`out = pt_in * scale`). The C++ replaces a non-finite or non-positive factor by 1;
PocketCoffea follows the C++.

## 1b. What σ (the "JER") is

In the formulas of this note, and in `eval_dict["JER"]` of `jet_correction_corrlib`, σ is the
**relative jet pt resolution of the simulation**, σ(pt)/pt, taken from the JME
`<jer_tag>_PtResolution_<jet_type>` correction of the JERC JSON (e.g.
`Summer23BPixPrompt23_RunD_JRV3_MC_PtResolution_AK4PFPuppi`). It is evaluated per jet at
(η, pt after the nominal JES, ρ):

```python
jer_ptres_tag = f"{jer_tag}_PtResolution_{jet_type}"
jer_ptres = ceval_jer[jer_ptres_tag].evaluate(JetEta, JetPt, Rho)   # jets.py, jet_correction_corrlib
eval_dict.update({"JER": jer_ptres})
```

The reference does the same (`reso = ptResolution->evaluate({eta, pt, rho})`,
`JecApplication.cpp:156`). σ is a dimensionless number: σ = 0.1 means a 10 % pt resolution.
It is the width of the (pt_reco − pt_gen)/pt_gen distribution in MC, parametrised by the
JME group as a function of pt, η and ρ; typical values are 5–10 % for pt > 100 GeV in the
barrel and 20–40 % for pt < 30 GeV in the forward region (|η| > 3).

σ enters the JER chain in two places:

1. **The matching condition.** A gen match is used for the scaling method only if
   `|pt − pt_gen| < 3·σ·pt` (three resolutions); otherwise the jet is smeared stochastically.
2. **The stochastic smearing.** `factor = 1 + sqrt(max(SF² − 1, 0))·σ·N(0, 1)`: the MC
   resolution σ is inflated to the data resolution SF·σ by adding a Gaussian of width
   `sqrt(SF² − 1)·σ`. The scaling method, `1 + (SF − 1)·(pt − pt_gen)/pt`, does not use σ:
   it rescales the actual reco−gen difference of the jet.

SF (`eval_dict["JERsf"]`, `<jer_tag>_ScaleFactor_<jet_type>`) is the data/MC resolution
ratio, and `SFUncertainty` its uncertainty; they are distinct from σ.

## 2. Findings

### 2.1 Gen-jet match without the dR < R/2 requirement

`add_jec_variables` (`jets.py:36`) uses `jets.matched_gen.pt`, that is the NanoAOD
`Jet_genJetIdx` association. NanoAOD matches within `dR < 0.4` for AK4 jets and `dR < 0.8`
for AK8 jets. The JME prescription (and the tutorial, `JecApplication.h:59` and
`CollectJetMet.hpp:245`, and pepper, `find_matched_genjet`) accepts the match only for
`dR < R/2`. PocketCoffea therefore applies the scaling method
`1 + (SF − 1)·(pt − pt_gen)/pt` to jets that the reference smears stochastically.

### 2.2 Unmatched jets with σ > 1/3 get factor = SF (detailed)

This is a genuine bug. It comes from three pieces of code that are individually reasonable
but inconsistent with each other.

**(a) The no-match value is 0.** `add_jec_variables`, `jets.py:34-38`:

```python
if isMC:
    try:
        jets["pt_gen"] = ak.values_astype(ak.fill_none(jets.matched_gen.pt, 0), np.float32)
    except AttributeError:
        jets["pt_gen"] = ak.zeros_like(jets.pt, dtype=np.float32)
```

A jet without a gen match (`matched_gen` is `None`) gets `pt_gen = 0`. A collection without
`matched_gen` at all (`CorrT1METJet`, used for the type-1 MET) gets `pt_gen = 0` for every
jet. The value 0 is the convention of the legacy coffea `CorrectedJetsFactory`
(`legacy_jet_correction.py`), where "matched" means `pt_gen > 0`.

**(b) The 3σ check only demotes a match; it cannot promote 0 to "no match".**
`jet_correction_corrlib`, `jets.py:805-815`:

```python
eval_dict.update({
    "GenPt": np.where(
        np.abs(eval_dict["JetPt"] - eval_dict["GenPt"]) < 3 * eval_dict["JetPt"] * eval_dict["JER"],
        eval_dict["GenPt"],
        -1.0,
    ),
})
```

For an unmatched jet, `GenPt = 0`, so the condition is `pt < 3·pt·σ`, i.e. `σ > 1/3`.

* If `σ ≤ 1/3` the condition is false and `GenPt` becomes `−1`. Correct by accident.
* If `σ > 1/3` the condition is true and `GenPt` **stays 0**.

**(c) The JERSmear node treats GenPt = 0 as a matched jet.** `get_jer_correction_set`,
`jets.py:604-637` (identical to `jer_smear.json.gz` of the tutorial):

```python
"data": {
    "nodetype": "binning",
    "input": "GenPt",
    "edges": [-1, 0, 1],
    "flow": "clamp",
    "content": [
        # bin [-1, 0): stochastic, GenPt is replaced by a hashprng normal random number
        {"nodetype": "transform", ... "expression": "1+sqrt(max(x*x - 1, 0)) * y * z", ...},
        # bin [0, 1) and above (clamp): scaling with the matched gen pt
        {"nodetype": "formula", "expression": "1+(x-1)*(y-z)/y", "variables": ["JERsf", "JetPt", "GenPt"]},
    ],
}
```

The method is selected by the bin of `GenPt`: `GenPt = −1` is in `[−1, 0)` → stochastic;
`GenPt = 0` is in `[0, 1)` → scaling. Any `GenPt ≥ 1` is clamped into the scaling bin. So
`GenPt = 0` is not "no match" for this node: it is a matched gen jet with `pt_gen = 0`, and
the scaling formula returns

```
1 + (SF − 1)·(pt − 0)/pt = SF
```

**Consequence.** An unmatched jet with `σ > 1/3` is not smeared: its pt is multiplied by the
bare scale factor (typically 1.0–1.3), deterministically, and the same factor is applied
to the JER up/down variations (`SF·(1 ± unc)`). The reference applies the stochastic
smearing `1 + sqrt(SF² − 1)·σ·N(0,1)` to these jets, i.e. a factor with mean 1 and a spread
of `sqrt(SF² − 1)·σ` — for `σ = 0.35`, `SF = 1.2` that is a 23 % spread. The affected jets
are the low-pt forward jets (`|η| > 3`, pt below ~30 GeV, where the resolution reaches
30–40 %), and every `CorrT1METJet` in that regime, which enters the type-1 MET.

The reference implementation avoids this by construction: it carries an explicit `hasGen`
flag and sets `genPtForSmear = −1.0` unless the match passes both the dR and the 3σ
check (`JecApplication.cpp:189-196`). Pepper uses an `ak.is_none` mask
(`compute_jer_factor`). The correctionlib route of PocketCoffea inherited the legacy `0`
convention while switching to a node whose "no match" convention is `−1`.

**Why the unit tests did not catch it.** `tests/test_calibrators.py` checks the
ordering and that the varied pt differs from the nominal, not the value of the smear
factor per jet; and the affected jets (σ > 1/3) are a small subset of a ttbar sample.

**Check.** `tests/test_jer_gen_match.py::test_jersmear_node_genpt_zero_is_scaling` builds the
node from `JERSMEAR_SCHEMA` and asserts `smear(GenPt=0) == SF`, `smear(GenPt=−1) != SF`.

### 2.3 No guard on a non-positive smear factor

`jets.py:820-831` multiplies the pt by the smear factor as returned by the node. With
`1 + sqrt(SF² − 1)·σ·N`, a random number `N < −1/(sqrt(SF² − 1)·σ)` gives a negative
factor and a negative pt (then `rawFactor` and the pt ordering are corrupted). The reference
replaces a non-finite or non-positive factor by 1 (`JecApplication.cpp:214`). Pepper has
no guard either.

### 2.4 JES uncertainty evaluated at the smeared pt

`jets.py:840` sets `eval_dict["JetPt"] = jets.pt` after `jets.pt` has been replaced by the
smeared pt (`jets.py:834`). The reference evaluates δ(η, pt) at the pt after the nominal
JES (`ApplicationHelper.hpp:136-141`). δ is a smooth function of pt, so the effect is
small, but the input is not the one the uncertainty was derived for.

## 3. Differences that are design choices (no change)

* **JES variations and JER.** The tutorial applies the JER *after* the JES shift, at the
  shifted pt; because `JetPt` seeds the `hashprng`, the JES-shifted jet gets a *different*
  random number than the nominal jet. PocketCoffea (and pepper) multiply the nominal smeared
  pt by `(1 ± δ)`, so the nominal and the JES variations share the same random number. The
  PocketCoffea choice removes random noise from the JES shape variations.
* **JER up/down.** PocketCoffea and the tutorial use the same random number for the nominal,
  up and down variations (same `hashprng` inputs). Pepper draws a new random number for
  each variation.
* **Pepper** evaluates δ, σ and SF at the NanoAOD pt instead of the recomputed JES pt.
  PocketCoffea follows the tutorial here.
* **Type-1 MET.** The tutorial recomputes the MET from `CorrT1METJet` with the JES shift
  and the JER applied per variation; in PocketCoffea the `CorrT1METJet` contribution is not
  varied (see the TODO in `METCalibrator`). Out of the scope of this note.

## 4. Fixes (this branch)

All in `pocket_coffea/lib/jets.py`:

1. `add_jec_variables(..., gen_match_dr=None, nomatch_value=0.0)`: the correctionlib
   path calls it with `gen_match_dr = R/2` (0.2 for AK4, 0.4 for AK8) and
   `nomatch_value = −1`. The legacy coffea path keeps the defaults (no dR cut, 0).
   Fixes 2.1 and 2.2 (`CorrT1METJet` included: every jet gets −1 → stochastic).
2. `safe_jersmear`: a non-finite or non-positive smear factor becomes 1, for the nominal
   and the up/down factors. Fixes 2.3.
3. `eval_dict["JetPt"]` is set once, right after the nominal JES, and is no longer
   overwritten after the JER; the JES uncertainties are evaluated at the JES pt. Fixes 2.4.
4. The JERSmear node definition is exposed as `JERSMEAR_SCHEMA` so it can be tested
   without a JME JSON file.

Expected effect on the nominal result: the smearing of low-pt forward jets (and of the
`CorrT1METJet` in the type-1 MET) changes from `×SF` to a stochastic factor; jets whose
NanoAOD gen match lies at `0.2 < dR < 0.4` move from the scaling to the stochastic method.
The JES variations move by the (small) difference between δ(pt_JES) and δ(pt_smeared).
The Run2 and Run3 reference outputs used by `test_shape_variations.py` are regenerated
with `tests/test_full_configs/test_shape_variations/regenerate_references.py`, which
checks that the data jets are unchanged and that only a minority of the MC jets change.

## 5. Validation on 2024 data and ttbar MC

Configuration: `tests/test_full_configs/test_jer_fix_2024` (2024 EGamma era C data and
TTto2L2Nu Summer24 NanoAODv15, one file each; JES_Total and JER variations; jets with
pt > 20 GeV and |η| < 5, or pt > 30 GeV and |η| < 2.4 in `config_central.py`). The same
jet is matched between the two runs on (event, position in the NanoAOD collection) with
`compare_outputs.py`.

**Data**: no change (no JER on data), as required.

**ttbar MC, all jets (pt > 20 GeV, |η| < 5), 256 540 matched jets, nominal:**

| |η| region | jets | pt changed | ⟨pt_after / pt_before⟩ |
|---|---|---|---|
| 0 – 1.3 | 129 433 | 2.0 % | 0.997 |
| 1.3 – 2.5 | 65 050 | 5.9 % | 0.995 |
| 2.5 – 3.0 | 42 952 | 75.9 % | 0.953 |
| 3.0 – 5.0 | 18 749 | 47.7 % | 0.969 |

* 18.5 % of the jets have no gen match before the fix, 19.6 % after: the dR < R/2 cut
  (finding 2.1) demotes 1.1 % of the jets; this is most of the 2 % change in the barrel.
* Of the 47 448 jets unmatched in both runs, 95.7 % change pt: nearly all unmatched jets
  are forward pile-up jets with σ > 1/3, i.e. all of them had factor = SF before
  (finding 2.2). The mean ratio in 3 < |η| < 5 — 0.969 (nominal), 0.909 (JER up),
  1.08 (JER down) — is 1/SF for the three cases: before the fix the forward jets were
  shifted coherently by SF ≈ 1.03, SF·(1+unc) ≈ 1.10 and SF·(1−unc) ≈ 0.92, with no
  smearing. Before the fix the JER variation acted as a JES-like coherent shift on the
  forward jets; the effect is largest for JER up, where `nJetGood` (jets up to |η| = 5)
  moved by ≈ 10 % per bin between the two runs.
* Negative ratios (down to −10 in the nominal, −2000 in JER up, at 2.5 < |η| < 3.0 where
  the JER SF is large) are jets with a negative pt before the fix (finding 2.3); after the
  fix their factor is 1.
* JES_Total up/down: 99.5 % of the jets change, with the same spread as the nominal — the
  effect of evaluating δ at the JES pt (finding 2.4) is at the numerical level.

**ttbar MC, central jets (pt > 30 GeV, |η| < 2.4)**: no visible effect on the jet and
`nJetGood` distributions, for the nominal and for the JES/JER variations. The fix is
relevant for selections that use forward (|η| > 2.5) or low-pt jets and for the type-1 MET
(through `CorrT1METJet`).
