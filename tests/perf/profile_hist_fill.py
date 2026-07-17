#!/usr/bin/env python
"""Profile HistManager.fill_histograms and/or compare two outputs bit-for-bit.

Written for the histogram-fill caching work (perf/hist-fill-cache) and kept for future
performance studies of the fill hot path.

Profile a config (prints a fill-path timing table; with --save-output it writes the
accumulated output so it can be diffed against another run):

    python tests/profiling/profile_hist_fill.py \
        --cfg tests/test_full_configs/test_new_weights/config.py \
        --outdir /tmp/prof --label after --save-output

A/B two implementations by running the SAME config twice and comparing the outputs. For a
git change, use `git stash` to toggle it (the outputs must be bit-identical for a pure
caching/reordering change):

    # with the change applied
    python tests/profiling/profile_hist_fill.py --cfg <cfg> --outdir /tmp/prof --label after  --save-output
    git stash push <changed files>
    python tests/profiling/profile_hist_fill.py --cfg <cfg> --outdir /tmp/prof --label before --save-output
    git stash pop
    python tests/profiling/profile_hist_fill.py --compare /tmp/prof/output_before.coffea /tmp/prof/output_after.coffea

The comparison is exact (numpy.array_equal) over every histogram (values and variances,
flow included) plus sumw / sumw2 / cutflow.

Run inside the CI container with a grid proxy (the configs read NanoAOD over xrootd).
"""
import argparse
import cProfile
import os
import pstats
import sys

import numpy as np

# Functions worth watching in the fill hot path. `inner` is the weights_cache wrapper
# (called for every broadcast request); `mask_and_broadcast_weight` is the wrapped body,
# so its call count is the number of actual broadcasts (i.e. cache misses).
_WATCH = ("fill_histograms", "inner", "mask_and_broadcast_weight",
          "get_masks", "ones_like", "flatten", "broadcast_arrays")


def run_and_profile(cfg_path, outdir, label, chunksize, limit_files, limit_chunks, save_output):
    from coffea.processor import Runner
    from coffea.nanoevents import NanoAODSchema
    from coffea.util import save
    from pocket_coffea.utils.utils import load_config
    from pocket_coffea.parameters import defaults
    from pocket_coffea.executors import executors_base as executors_lib

    cfg_path = os.path.abspath(cfg_path)
    os.makedirs(outdir, exist_ok=True)
    # load_config resolves relative paths (datasets/params) from the config's own directory
    os.chdir(os.path.dirname(cfg_path))
    config = load_config(os.path.basename(cfg_path), save_config=True, outputdir=outdir)

    run_options = defaults.get_default_run_options()["general"]
    run_options["limit-files"] = limit_files
    run_options["limit-chunks"] = limit_chunks
    run_options["chunksize"] = chunksize
    config.filter_dataset(limit_files)
    executor = executors_lib.get_executor_factory(
        "iterative", run_options=run_options, outputdir=outdir).get()
    run = Runner(executor=executor, chunksize=chunksize, maxchunks=limit_chunks,
                 schema=NanoAODSchema, format="root")

    profiler = cProfile.Profile()
    profiler.enable()
    output = run(config.filesets, treename="Events",
                 processor_instance=config.processor_instance)
    profiler.disable()

    if save_output:
        save(output, os.path.join(outdir, f"output_{label}.coffea"))

    _print_timing(profiler, label, cfg_path)
    return output


def _print_timing(profiler, label, cfg_path):
    stats = pstats.Stats(profiler)
    agg = {}
    for (_fp, _ln, fname), (_cc, ncalls, tottime, cumtime, _callers) in stats.stats.items():
        entry = agg.setdefault(fname, [0, 0.0, 0.0])
        entry[0] += ncalls
        entry[1] += tottime
        entry[2] = max(entry[2], cumtime)
    print(f"\n===== hist-fill profile [{label}] cfg={os.path.basename(cfg_path)} =====")
    print(f"  {'function':30s} {'ncalls':>9s} {'tottime':>10s} {'cumtime':>10s}")
    for fn in _WATCH:
        if fn in agg:
            ncalls, tottime, cumtime = agg[fn]
            print(f"  {fn:30s} {ncalls:9d} {tottime:9.3f}s {cumtime:9.3f}s")
    print("  (inner = broadcast requests; mask_and_broadcast_weight = actual broadcasts)")
    print("=" * 62 + "\n")


def compare(file_a, file_b):
    from coffea.util import load

    a, b = load(file_a), load(file_b)
    diffs = []
    nchecked = 0

    va, vb = a.get("variables", {}), b.get("variables", {})
    if set(va) != set(vb):
        diffs.append(f"variable sets differ: {set(va) ^ set(vb)}")
    for var in set(va) & set(vb):
        for sample in va[var]:
            for dataset in va[var][sample]:
                ha, hb = va[var][sample][dataset], vb[var][sample][dataset]
                pairs = (("values", ha.values(flow=True), hb.values(flow=True)),
                         ("variances", ha.variances(flow=True), hb.variances(flow=True)))
                for what, fa, fb in pairs:
                    nchecked += 1
                    if fa is None and fb is None:
                        continue
                    if not np.array_equal(fa, fb):
                        diffs.append(f"variables/{var}/{sample}/{dataset} {what}")

    def walk(pa, pb, path):
        nonlocal nchecked
        if isinstance(pa, dict):
            if set(pa) != set(pb):
                diffs.append(f"keys differ at {path}")
                return
            for k in pa:
                walk(pa[k], pb[k], f"{path}/{k}")
        else:
            nchecked += 1
            if not np.array_equal(np.asarray(pa), np.asarray(pb)):
                diffs.append(path)

    for key in ("sumw", "sumw2", "cutflow"):
        if key in a and key in b:
            walk(a[key], b[key], key)

    print(f"checked {nchecked} arrays/scalars")
    if diffs:
        print(f"NOT BIT-IDENTICAL: {len(diffs)} differences, e.g.:")
        for d in diffs[:20]:
            print("   ", d)
        return 1
    print("BIT-IDENTICAL: the two outputs match for every histogram, sumw, sumw2 and cutflow")
    return 0


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--cfg", help="config .py to profile")
    parser.add_argument("--outdir", default="/tmp/pocket_coffea_prof")
    parser.add_argument("--label", default="run", help="tag for the printout / output file")
    parser.add_argument("--chunksize", type=int, default=200000)
    parser.add_argument("--limit-files", type=int, default=1)
    parser.add_argument("--limit-chunks", type=int, default=1)
    parser.add_argument("--save-output", action="store_true",
                        help="save the accumulated output as output_<label>.coffea for --compare")
    parser.add_argument("--compare", nargs=2, metavar=("BEFORE.coffea", "AFTER.coffea"),
                        help="compare two saved outputs bit-for-bit and exit")
    args = parser.parse_args()

    if args.compare:
        sys.exit(compare(*args.compare))
    if not args.cfg:
        parser.error("either --cfg or --compare is required")
    run_and_profile(args.cfg, args.outdir, args.label, args.chunksize,
                    args.limit_files, args.limit_chunks, args.save_output)


if __name__ == "__main__":
    main()
