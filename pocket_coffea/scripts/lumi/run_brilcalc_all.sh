#!/bin/bash
# Recorded luminosity per era for all Run 3 periods, in 2 steps:
#   ./run_brilcalc_all.sh brilcalc [outdir]   # lxplus host (needs apptainer/singularity, not the PocketCoffea container)
#   ./run_brilcalc_all.sh parse [outdir]      # PocketCoffea container: sums per era, writes lumi_summary_<year>.json
# Normtag per period: PHYSICS covers runs up to 386951 (end of 2024), BRIL also covers 2025-2026
# (DATACERT covers only 271036-297308).
# Golden JSONs and run ranges: keep in sync with lumi.goldenJSON / lumi.run_ranges in parameters/lumi.yaml.
set -euo pipefail

step=${1:?usage: $0 brilcalc|parse [outdir]}
out=${2:-lumi_brilcalc}
NT=/cvmfs/cms-bril.cern.ch/cms-lumi-pog/Normtags
DC=/cvmfs/cms-griddata.cern.ch/cat/metadata/DC
IMAGE=/cvmfs/unpacked.cern.ch/gitlab-registry.cern.ch/cms-cloud/brilws-docker:latest

PERIODS="
2022_preEE    Collisions22/latest/Cert_Collisions2022_355100_362760_Golden.json 355862 357900 normtag_PHYSICS.json
2022_postEE   Collisions22/latest/Cert_Collisions2022_355100_362760_Golden.json 359022 362760 normtag_PHYSICS.json
2023_preBPix  Collisions23/latest/Cert_Collisions2023_366442_370790_Golden.json 367095 368823 normtag_PHYSICS.json
2023_postBPix Collisions23/latest/Cert_Collisions2023_366442_370790_Golden.json 369803 370790 normtag_PHYSICS.json
2024          Collisions24/latest/Cert_Collisions2024_378981_386951_Golden.json 379415 386974 normtag_PHYSICS.json
2025          Collisions25/latest/Cert_Collisions2025_391658_398903_Golden.json 391548 398903 normtag_BRIL.json
2026          Collisions26/latest/Cert_Collisions2026_401624_403937_golden.json 401606 404010 normtag_BRIL.json
"

mkdir -p "$out"
case $step in
  brilcalc)
    # Same as the brilcalc alias of /cvmfs/cms-bril.cern.ch/cms-lumi-pog/brilws-docker/brilws-env
    exe=$(command -v apptainer || command -v singularity)
    while read -r y g b e n; do
      [ -n "$y" ] || continue
      echo "== $y"
      "$exe" -s exec -B /cvmfs --env PYTHONPATH=/home/bril/.local/lib/python3.10/site-packages "$IMAGE" \
        brilcalc lumi -u /pb --normtag "$NT/$n" -i "$DC/$g" --begin "$b" --end "$e" -o "$out/brilcalc_$y.csv" \
        || { echo "FAILED $y"; rm -f "$out/brilcalc_$y.csv"; }
    done <<< "$PERIODS"
    ;;
  parse)
    while read -r y _; do
      [ -n "$y" ] || continue
      echo "== $y"
      [ -f "$out/brilcalc_$y.csv" ] || { echo "no $out/brilcalc_$y.csv, skip"; continue; }
      python -m pocket_coffea.scripts.lumi.run_brilcalc --year "$y" --csv "$out/brilcalc_$y.csv" -o "$out"
    done <<< "$PERIODS"
    ;;
  *) echo "unknown step '$step': use brilcalc or parse" >&2; exit 1 ;;
esac
