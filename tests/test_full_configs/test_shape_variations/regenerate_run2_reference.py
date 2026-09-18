#!/usr/bin/env python
"""Regenerate the nominal Jet columns of comparison_arrays/output_run2.coffea.

The default calibrator sequence now re-sorts the nominal jets by the corrected pt (for
consistency with the systematic variations). The committed reference was produced when the
nominal jets were kept in NanoAOD order, so the order-sensitive Jet_pt comparison in
test_shape_variation_default_sequence_comparison_with_legacy_run2 no longer matches.

This script re-runs the same config the test runs, VERIFIES that the new nominal jet order
is coherent with the change (i.e. it is exactly the corrected-pt re-sort of the same jet
values, so only the order changed, not the values), and then updates the reordered nominal
Jet columns in the reference file. Run it inside the CI container with a grid proxy.
"""
import os
import numpy as np
import awkward as ak

from pocket_coffea.utils.utils import load_config
from pocket_coffea.parameters import defaults
from pocket_coffea.executors import executors_base as executors_lib
from coffea import processor
from coffea.processor import Runner
from coffea.util import load, save

HERE = os.path.dirname(os.path.abspath(__file__))
REF = os.path.join(HERE, "comparison_arrays", "output_run2.coffea")
SAMPLE, DATASET, CAT = "TTTo2L2Nu", "TTTo2L2Nu_2018", "baseline"


def run_config(outputdir):
    os.chdir(HERE)
    os.makedirs(outputdir, exist_ok=True)
    cache = os.path.join(HERE, "jets_calibrator_JES_JER_Syst.pkl.gz")
    if os.path.exists(cache):
        os.remove(cache)
    config = load_config("config_allvars_Run2.py", save_config=True, outputdir=outputdir)
    run_options = defaults.get_default_run_options()["general"]
    run_options["limit-files"] = 1
    run_options["limit-chunks"] = 1
    run_options["chunksize"] = 300
    config.filter_dataset(run_options["limit-files"])
    executor_factory = executors_lib.get_executor_factory(
        "iterative", run_options=run_options, outputdir=outputdir)
    run = Runner(executor=executor_factory.get(), chunksize=run_options["chunksize"],
                 maxchunks=run_options["limit-chunks"], schema=processor.NanoAODSchema, format="root")
    return run(config.filesets, treename="Events", processor_instance=config.processor_instance)


def main():
    outputdir = os.environ.get("REGEN_OUTPUTDIR", "/tmp/regen_run2_ref")
    output = None
    for attempt in range(5):
        try:
            output = run_config(outputdir)
            break
        except OSError as e:  # transient xrootd read timeouts
            print(f"[attempt {attempt}] IO error: {e}; retrying")
    assert output is not None, "could not run the config (xrootd)"

    new = output["columns"][SAMPLE][DATASET][CAT]["nominal"]
    ref = load(REF)
    old = ref["columns"][SAMPLE][DATASET][CAT]

    new_N, old_N = new["Jet_N"].value, old["Jet_N"].value
    new_j = ak.unflatten(new["Jet_pt"].value, new_N)
    old_j = ak.unflatten(old["Jet_pt"].value, old_N)

    # (1) same events / jet multiplicities
    assert np.array_equal(new_N, old_N), "Jet_N differs -> different events, refusing to regenerate"
    # (2) the new nominal jets are sorted by descending pt
    assert ak.all(new_j[:, :-1] >= new_j[:, 1:]), "new nominal jets are not pt-descending"
    # (3) per-event, new == old re-sorted by descending pt: same values, only reordered
    old_sorted = old_j[ak.argsort(old_j, axis=1, ascending=False)]
    assert np.allclose(ak.to_numpy(ak.flatten(new_j)), ak.to_numpy(ak.flatten(old_sorted)), rtol=1e-5), \
        "new nominal Jet_pt is NOT the corrected-pt re-sort of the reference values"

    changed = ~ak.to_numpy(ak.all(old_j == new_j, axis=1))
    n_changed = int(changed.sum())
    print(f"coherence OK: {len(new_N)} events, {n_changed} with a reordered nominal Jet collection")
    if n_changed:
        i = int(np.argmax(changed))
        print(f"  example event {i}\n    old (nano order): {ak.to_list(old_j[i])}\n    new (sorted)    : {ak.to_list(new_j[i])}")

    # Update the reordered nominal Jet columns (kept aligned by taking both from the new run)
    for col in ("Jet_pt", "Jet_pt_original"):
        if col in new:
            old[col] = new[col]
    save(ref, REF)
    print("reference regenerated:", REF)


if __name__ == "__main__":
    main()
