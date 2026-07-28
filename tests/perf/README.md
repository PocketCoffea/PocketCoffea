# Profiling harnesses

Small scripts to study PocketCoffea performance-sensitive paths. They are not run by the
pytest suite; run them by hand inside the CI container with a grid proxy (the configs read
NanoAOD over xrootd).

## `profile_hist_fill.py`

Profiles `HistManager.fill_histograms` and can compare two runs bit-for-bit. Used to
develop and validate the histogram-fill caching (`perf/hist-fill-cache`).

```bash
# time the fill of a config and print a per-function table
python tests/profiling/profile_hist_fill.py \
    --cfg tests/test_full_configs/test_new_weights/config.py \
    --outdir /tmp/prof --label after --save-output

# A/B a git change (must be bit-identical for a pure caching/reordering change):
git stash push pocket_coffea/lib/hist_manager.py            # revert to baseline
python tests/profiling/profile_hist_fill.py --cfg <cfg> --outdir /tmp/prof --label before --save-output
git stash pop                                               # restore the change
python tests/profiling/profile_hist_fill.py --cfg <cfg> --outdir /tmp/prof --label after  --save-output
python tests/profiling/profile_hist_fill.py --compare /tmp/prof/output_before.coffea /tmp/prof/output_after.coffea
```

In the profile table, `inner` counts every weight-broadcast request while
`mask_and_broadcast_weight` counts the actual broadcasts (cache misses), so the ratio
shows the broadcast cache hit rate.
