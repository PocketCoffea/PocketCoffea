#!/usr/bin/env python
"""Regenerate comparison_arrays/output_run2.coffea and output_run3.coffea.

The two legacy-comparison tests in test_shape_variations.py compare the nominal Jet/MET
columns and the JES up/down histograms with these reference files. After a change of the
jet calibration chain that alters the nominal values (e.g. the JER gen-matching fix, see
docs/jerc_chain_comparison.md), the references must be produced again with the new code.

This script re-runs the same configs with the same run options as the tests, VERIFIES that
the new output is coherent with the reference (same events and jet multiplicities, data
jets unchanged, MC jets changed only for a minority of jets and all with a positive pt) and
then replaces the nominal columns and the histograms in the reference file. Run it inside
the CI container with a grid proxy:

    python regenerate_references.py            # both
    python regenerate_references.py run3       # one of them
    python regenerate_references.py --dry-run  # checks and prints only, does not write
"""
import os
import sys
import numpy as np
import awkward as ak

from pocket_coffea.utils.utils import load_config
from pocket_coffea.parameters import defaults
from pocket_coffea.executors import executors_base as executors_lib
from coffea import processor
from coffea.processor import Runner
from coffea.util import load, save

HERE = os.path.dirname(os.path.abspath(__file__))
REFS = {
    "run2": ("config_allvars_Run2.py", "comparison_arrays/output_run2.coffea"),
    "run3": ("config_allvars_Run3.py", "comparison_arrays/output_run3.coffea"),
}
MAX_CHANGED_FRACTION = 0.4   # of the MC jets: the JER fix touches ~20% (unmatched forward jets)
# Data jets get no JER, so they must not change -- except for the JEC version drift of the
# JME JSONs on cvmfs, which the Run3 test already tolerates with atol=1.5 GeV.
DATA_MAX_ABS_DIFF = {"run2": 1e-3, "run3": 1.5}


def run_config(config_file, outputdir):
    os.chdir(HERE)
    os.makedirs(outputdir, exist_ok=True)
    cache = os.path.join(HERE, "jets_calibrator_JES_JER_Syst.pkl.gz")
    if os.path.exists(cache):
        os.remove(cache)
    config = load_config(config_file, save_config=True, outputdir=outputdir)
    run_options = defaults.get_default_run_options()["general"]
    run_options["limit-files"] = 1
    run_options["limit-chunks"] = 1
    run_options["chunksize"] = 300
    config.filter_dataset(run_options["limit-files"])
    executor_factory = executors_lib.get_executor_factory(
        "iterative", run_options=run_options, outputdir=outputdir)
    run = Runner(executor=executor_factory.get(), chunksize=run_options["chunksize"],
                 maxchunks=run_options["limit-chunks"], schema=processor.NanoAODSchema, format="root")
    for attempt in range(5):
        try:
            return run(config.filesets, treename="Events", processor_instance=config.processor_instance)
        except OSError as e:  # transient xrootd read timeouts
            print(f"[attempt {attempt}] IO error: {e}; retrying")
    raise RuntimeError("could not run the config (xrootd)")


def check_coherence(ref, output, key):
    for sample in ref["columns"]:
        for dataset in ref["columns"][sample]:
            is_mc = output["datasets_metadata"]["by_dataset"][dataset]["isMC"] in (True, "True")
            for cat, old in ref["columns"][sample][dataset].items():
                new = output["columns"][sample][dataset][cat]["nominal"]
                old_N, new_N = old["Jet_N"].value, new["Jet_N"].value
                if not np.array_equal(old_N, new_N):
                    # the tests compare the baseline only; the other categories can select
                    # different events after unrelated changes (e.g. b-tag working points)
                    assert cat != "baseline", f"{dataset}/{cat}: Jet_N differs -> different events, refusing"
                    print(f"  {sample}/{dataset}/{cat}: selection changed ({len(old_N)} -> {len(new_N)} events), replaced")
                    continue
                old_pt, new_pt = old["Jet_pt"].value, new["Jet_pt"].value
                if len(old_pt) == 0:
                    print(f"  {sample}/{dataset}/{cat}: {len(old_N)} events, 0 jets")
                    continue
                assert np.all(new_pt > 0), f"{dataset}/{cat}: non-positive jet pt in the new output"
                changed = ~np.isclose(old_pt, new_pt, rtol=1e-5)
                frac = changed.mean() if len(changed) else 0.0
                maxdiff = np.abs(old_pt - new_pt).max() if len(old_pt) else 0.0
                # A reference produced before the nominal re-sort by corrected pt has the same
                # values at different positions: compare the per-event sorted values too.
                old_j, new_j = ak.unflatten(old_pt, old_N), ak.unflatten(new_pt, new_N)
                old_s = ak.to_numpy(ak.flatten(ak.sort(old_j, axis=1)))
                new_s = ak.to_numpy(ak.flatten(ak.sort(new_j, axis=1)))
                maxdiff_sorted = np.abs(old_s - new_s).max() if len(old_s) else 0.0
                print(f"  {sample}/{dataset}/{cat}: {len(old_N)} events, {len(old_pt)} jets, "
                      f"nominal Jet_pt changed for {frac*100:.2f}%, max |diff| = {maxdiff:.3f} GeV "
                      f"(as sorted values: {maxdiff_sorted:.3f} GeV)")
                if not is_mc:
                    assert maxdiff_sorted <= DATA_MAX_ABS_DIFF[key], \
                        f"{dataset}/{cat}: data jet values changed by more than the JEC-version tolerance, refusing"
                else:
                    assert frac <= MAX_CHANGED_FRACTION, f"{dataset}/{cat}: too many MC jets changed, refusing"


def update(ref, output):
    # nominal columns: the reference keeps the old flat layout (no variation level)
    for sample in ref["columns"]:
        for dataset in ref["columns"][sample]:
            for cat, old in ref["columns"][sample][dataset].items():
                new = output["columns"][sample][dataset][cat]["nominal"]
                for col in old:
                    if col in new:
                        old[col] = new[col]
    # histograms: replace the ones the new run produces, keep the others
    for name, h in output["variables"].items():
        ref["variables"][name] = h


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    dry_run = "--dry-run" in sys.argv
    for key in args or list(REFS):
        config_file, ref_rel = REFS[key]
        ref_path = os.path.join(HERE, ref_rel)
        outputdir = os.environ.get("REGEN_OUTPUTDIR", f"/tmp/regen_{key}_ref")
        print(f"== {key}: running {config_file}")
        output = run_config(config_file, outputdir)
        ref = load(ref_path)
        check_coherence(ref, output, key)
        if dry_run:
            print(f"  dry run: {ref_path} not written")
            continue
        update(ref, output)
        save(ref, ref_path)
        print(f"  reference regenerated: {ref_path}")


if __name__ == "__main__":
    main()
