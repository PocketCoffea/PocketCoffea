"""Compute the recorded luminosity per era with brilcalc.

The golden JSON (``lumi.goldenJSON``) and the era run ranges (``lumi.run_ranges``)
come from the PocketCoffea default parameters (``parameters/lumi.yaml``).

    python -m pocket_coffea.scripts.lumi.run_brilcalc --year 2025

brilcalc comes from PATH or, if not there, from the brilws container image on CVMFS
(run with apptainer/singularity, as the lxplus ``brilws-env`` alias does).
If neither is available, the script prints the brilcalc command: run it where
brilcalc works, then rerun this script with ``--csv``.
"""
import csv
import json
import os
import shutil
import subprocess

import click

from pocket_coffea.parameters.defaults import get_default_parameters

# normtag_BRIL covers all Run 3 runs; normtag_PHYSICS stops at 386951 (end of 2024)
NORMTAG = "/cvmfs/cms-bril.cern.ch/cms-lumi-pog/Normtags/normtag_BRIL.json"
# Same as the `brilcalc` alias that brilws-env defines on lxplus
BRILWS_IMAGE = "/cvmfs/unpacked.cern.ch/gitlab-registry.cern.ch/cms-cloud/brilws-docker:latest"


def brilcalc_command():
    """brilcalc from PATH, else from the brilws container. None if neither works."""
    if shutil.which("brilcalc"):
        return ["brilcalc"]
    exe = shutil.which("apptainer") or shutil.which("singularity")
    if exe and os.path.exists(BRILWS_IMAGE):
        return [exe, "-s", "exec", "-B", "/cvmfs", "--env", "PYTHONPATH=/home/bril/.local/lib/python3.10/site-packages",
                BRILWS_IMAGE, "brilcalc"]
    return None


def read_brilcalc_csv(path):
    """Return {run: recorded lumi} from a ``brilcalc lumi -o`` csv file."""
    with open(path) as f:
        return {
            int(row[0].split(":")[0]): float(row[-1])
            for row in csv.reader(f)
            if row and not row[0].startswith("#")
        }


def lumi_by_era(lumi_per_run, run_ranges):
    """Sum the per-run lumi into eras. Runs outside every era go to 'outside'."""
    out = {era: 0.0 for era in run_ranges}
    outside = 0.0
    for run, lumi in lumi_per_run.items():
        era = next((e for e, (first, last) in run_ranges.items() if first <= run <= last), None)
        if era is None:
            outside += lumi
        else:
            out[era] += lumi
    out["tot"] = sum(out.values())
    return out, outside


@click.command()
@click.option("-y", "--year", required=True, help="Data-taking period key of lumi.yaml, e.g. 2025 or 2022_preEE")
@click.option("--normtag", default=NORMTAG, show_default=True)
@click.option("--csv", "csv_file", type=click.Path(exists=True), help="Use this brilcalc csv output, do not run brilcalc")
@click.option("-o", "--output", default="lumi_brilcalc", show_default=True, help="Output folder")
def run_brilcalc(year, normtag, csv_file, output):
    params = get_default_parameters().lumi
    run_ranges = {str(era): tuple(r) for era, r in params.run_ranges[year].items()}
    os.makedirs(output, exist_ok=True)

    if csv_file is None:
        csv_file = os.path.join(output, f"brilcalc_{year}.csv")
        cmd = [
            "lumi", "-u", "/pb", "--normtag", normtag,
            "-i", params.goldenJSON[year],
            "--begin", str(min(r[0] for r in run_ranges.values())),
            "--end", str(max(r[1] for r in run_ranges.values())),
            "-o", csv_file,
        ]
        brilcalc = brilcalc_command()
        print(" ".join((brilcalc or ["brilcalc"]) + cmd))
        if brilcalc is None:
            raise click.ClickException(
                "brilcalc not found (no brilcalc in PATH, no apptainer/singularity for the brilws image): "
                "run the command above, then rerun with --csv <file>"
            )
        subprocess.run(brilcalc + cmd, check=True)

    result, outside = lumi_by_era(read_brilcalc_csv(csv_file), run_ranges)
    current = params.picobarns.get(year, {})
    print(f"{'era':>5} {'brilcalc /pb':>14} {'lumi.yaml /pb':>14}")
    for era, lumi in result.items():
        print(f"{era:>5} {lumi:14.1f} {current.get(era, float('nan')):14.1f}")
    if outside:
        print(f"WARNING: {outside:.1f} /pb of golden runs are outside the eras in lumi.run_ranges[{year}]")

    summary_file = os.path.join(output, f"lumi_summary_{year}.json")
    with open(summary_file, "w") as f:
        json.dump({year: result}, f, indent=2)
    print(f"Summary written to {summary_file}")


if __name__ == "__main__":
    run_brilcalc()
