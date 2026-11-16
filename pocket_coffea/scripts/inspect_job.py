"""Print a human-readable summary of one job of the manual-jobs executors
(`condor@lxplus`, `condor@rubin`, ...).

Each job is `jobs_dir/fileset_job_{i}.json` (its slice of the fileset) plus
the `configurator_jobs.pkl` shared by all jobs. This tool loads both and prints
what the job will actually run on: workflow class,
workflow options, save_skimmed_files setting, and the per-dataset table
(sample, year, isMC, n_files, n_events). Pass --files to also list every
file URL.

Typical use:
    pocket-coffea inspect-job /path/to/jobs_dir/job/fileset_job_42.json
    pocket-coffea inspect-job .../fileset_job_42.json --files
"""
import json
import os

import click
import cloudpickle
from rich import print as rprint
from rich.table import Table
from rich.console import Console

from pocket_coffea.executors.executors_manual_jobs import SHARED_CONFIG_NAME


@click.command(name="inspect-job")
@click.argument("job_path", type=click.Path(exists=True, dir_okay=False))
@click.option("-f", "--files", "show_files", is_flag=True,
              help="List every file URL per dataset.")
@click.option("-m", "--metadata", "show_metadata", is_flag=True,
              help="Print the full per-dataset metadata dict (verbose).")
@click.option("--workflow-options/--no-workflow-options", default=True,
              help="Print the workflow_options dict (default: on).")
def inspect_job(job_path, show_files, show_metadata, workflow_options):
    """Print the content of a job (jobs_dir/fileset_job_*.json)."""
    console = Console()

    # The job fileset + the configurator shared by all jobs, next to it.
    with open(os.path.join(os.path.dirname(job_path), SHARED_CONFIG_NAME), "rb") as f:
        cfg = cloudpickle.load(f)
    with open(job_path) as f:
        cfg.set_filesets_manually(json.load(f))

    # --- header
    rprint(f"[bold]Job fileset:[/] {os.path.abspath(job_path)}")
    workflow = getattr(cfg, "workflow", None)
    wf_name = getattr(workflow, "__name__", str(workflow))
    wf_mod = getattr(workflow, "__module__", "")
    rprint(f"[bold]Workflow:[/] {wf_mod}.{wf_name}" if wf_mod else f"[bold]Workflow:[/] {wf_name}")

    if cfg.save_skimmed_files:
        rprint(f"[bold]save_skimmed_files:[/] {cfg.save_skimmed_files_folder}")
    else:
        rprint("[bold]save_skimmed_files:[/] off")

    if workflow_options and cfg.workflow_options:
        rprint("[bold]workflow_options:[/]")
        for k, v in cfg.workflow_options.items():
            rprint(f"  {k}: {v}")

    # --- datasets table
    filesets = getattr(cfg, "filesets", {}) or {}
    if not filesets:
        rprint("[yellow]No filesets in this job.[/]")
        return

    table = Table(title=f"Datasets in this job ({len(filesets)})")
    table.add_column("Dataset", style="cyan", no_wrap=True)
    table.add_column("Sample", style="magenta")
    table.add_column("Year", justify="right")
    table.add_column("isMC", justify="center")
    table.add_column("N files", justify="right", style="green")
    table.add_column("N events", justify="right", style="green")

    tot_files = 0
    tot_events = 0
    for ds_name, ds in filesets.items():
        meta = ds.get("metadata", {})
        files_list = ds.get("files", [])
        nev = meta.get("nevents", "?")
        try:
            nev_int = int(nev)
            tot_events += nev_int
            nev_str = f"{nev_int:_}"
        except (TypeError, ValueError):
            nev_str = str(nev)
        tot_files += len(files_list)

        table.add_row(
            ds_name,
            str(meta.get("sample", "?")),
            str(meta.get("year", "?")),
            str(meta.get("isMC", "?")),
            str(len(files_list)),
            nev_str,
        )
    table.add_section()
    table.add_row("[b]Total[/]", "", "", "", str(tot_files), f"{tot_events:_}")
    console.print(table)

    # --- per-dataset detail
    if show_metadata or show_files:
        for ds_name, ds in filesets.items():
            rprint(f"\n[bold cyan]── {ds_name}[/]")
            if show_metadata:
                rprint("  [bold]metadata:[/]")
                for k, v in (ds.get("metadata") or {}).items():
                    rprint(f"    {k}: {v}")
            if show_files:
                files_list = ds.get("files", [])
                rprint(f"  [bold]files ({len(files_list)}):[/]")
                for url in files_list:
                    rprint(f"    {url}")


if __name__ == "__main__":
    inspect_job()
