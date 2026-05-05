import os
import sys
import re
import shlex

from coffea.util import save as coffea_save
from rich import print
from rich.progress import Progress


def _build_make_plots_command_template(p):
    '''Build a `make-plots` command line for a single-histogram condor job.

    Input files are intentionally excluded here: each job receives a
    pre-cached per-histogram file passed as a positional shell argument.
    `--overwrite` is always added because the output directory is
    pre-created by the submission step.
    '''
    cmd = ["make-plots"]
    if p.get("input_dir"):
        cmd += ["-inp", p["input_dir"]]
    if p.get("cfg"):
        cmd += ["--cfg", p["cfg"]]
    if p.get("outputdir"):
        cmd += ["-o", p["outputdir"]]
    if p.get("workers") is not None:
        cmd += ["-j", str(p["workers"])]
    if p.get("verbose") is not None:
        cmd += ["-v", str(p["verbose"])]
    if p.get("format"):
        cmd += ["--format", p["format"]]
    if p.get("index_file"):
        cmd += ["--index-file", p["index_file"]]
    for op in (p.get("overwrite_parameters") or ()):
        cmd += ["-op", op]
    # inputfiles deliberately omitted — each job uses its own cached file
    for c in (p.get("only_cat") or ()):
        cmd += ["-oc", c]
    for y in (p.get("only_year") or ()):
        cmd += ["-oy", y]
    for s in (p.get("only_syst") or ()):
        cmd += ["-os", s]
    for e in (p.get("exclude_hist") or ()):
        cmd += ["-e", e]
    if p.get("split_systematics"):
        cmd.append("--split-systematics")
    if p.get("partial_unc_band"):
        cmd.append("--partial-unc-band")
    if p.get("no_syst"):
        cmd.append("-ns")
    if p.get("log_x"):
        cmd.append("--log-x")
    if p.get("log_y"):
        cmd.append("--log-y")
    if p.get("density"):
        cmd.append("--density")
    if p.get("systematics_shifts"):
        cmd.append("--systematics-shifts")
    if p.get("no_ratio"):
        cmd.append("--no-ratio")
    if p.get("no_systematics_ratio"):
        cmd.append("--no-systematics-ratio")
    if p.get("compare"):
        cmd.append("--compare")
    if p.get("no_cache"):
        cmd.append("--no-cache")
    cmd.append("--overwrite")
    return " ".join(shlex.quote(a) for a in cmd)


def _submit_condor_jobs(variables, accumulator, outputdir, jobs_dir,
                        mem_per_worker, cores_per_worker, condor_queue,
                        dry_run, cli_params, local_virtualenv=False,
                        env_setup=()):
    '''Generate and submit one HTCondor job per histogram in `variables`.

    Each job loads a small per-histogram coffea file pre-cached on AFS
    rather than the full (potentially multi-GB) original input on EOS.
    '''
    if not variables:
        sys.exit("[red]No histograms left to plot after filtering — nothing to submit.[/]")

    abs_outputdir = os.path.abspath(outputdir)
    if jobs_dir is None:
        jobs_dir = os.path.join(abs_outputdir, "condor_jobs")
    abs_jobs_dir = os.path.abspath(jobs_dir)

    if abs_jobs_dir.startswith("/eos/"):
        sys.exit(
            "[red]HTCondor does not allow job submission from EOS paths.\n"
            "Pass --jobs-dir pointing to an AFS or local directory, e.g.:\n"
            "  --jobs-dir /afs/cern.ch/work/.../condor_jobs\n"
            "The output plots can remain on EOS — only the submit/log files "
            "must be outside EOS.[/]"
        )

    log_dir = os.path.join(abs_jobs_dir, "logs")
    hist_cache_dir = os.path.join(abs_jobs_dir, "hist_cache")
    os.makedirs(abs_outputdir, exist_ok=True)
    os.makedirs(abs_jobs_dir, exist_ok=True)
    os.makedirs(log_dir, exist_ok=True)
    os.makedirs(hist_cache_dir, exist_ok=True)

    # Save one small coffea file per histogram so each job avoids loading
    # the full (multi-GB) input file.
    datasets_metadata = accumulator["datasets_metadata"]
    hist_files = {}
    print(f"[blue]Caching {len(variables)} per-histogram files in {hist_cache_dir} ...[/]")
    with Progress() as progress:
        task = progress.add_task("[cyan]Saving histogram cache...", total=len(variables))
        for v in variables:
            safe = re.sub(r"[^A-Za-z0-9_\-]", "_", v)
            hist_file = os.path.join(hist_cache_dir, f"{safe}.coffea")
            coffea_save(
                {"variables": {v: accumulator["variables"][v]},
                 "datasets_metadata": datasets_metadata},
                hist_file,
            )
            hist_files[v] = hist_file
            progress.update(task, advance=1)

    env_lines = []
    if local_virtualenv:
        env_lines.append(f"export PATH={sys.prefix}/bin:$PATH")
        pythonpath = os.getenv("PYTHONPATH", "")
        if pythonpath:
            env_lines.append(f"export PYTHONPATH={pythonpath}")
    env_lines.extend(env_setup)
    env_block = ("\n".join(env_lines) + "\n") if env_lines else ""

    base_cmd = _build_make_plots_command_template(cli_params)

    job_script_path = os.path.join(abs_jobs_dir, "make_plots_job.sh")
    with open(job_script_path, "w") as f:
        f.write(
            "#!/bin/bash\n"
            "set -e\n"
            f"{env_block}"
            'VARIABLE="$1"\n'
            'HISTFILE="$2"\n'
            'echo "Plotting histogram: $VARIABLE"\n'
            f'{base_cmd} -i "$HISTFILE" -oh "^${{VARIABLE}}$"\n'
        )
    os.chmod(job_script_path, 0o755)

    sub = {
        'Executable': job_script_path,
        'Error': f"{log_dir}/job_$(ClusterId).$(ProcId).err",
        'Output': f"{log_dir}/job_$(ClusterId).$(ProcId).out",
        'Log': f"{log_dir}/job_$(ClusterId).log",
        'MY.SendCredential': True,
        '+JobFlavour': f'"{condor_queue}"',
        'RequestCpus': cores_per_worker,
        'RequestMemory': mem_per_worker,
        'arguments': "$(variable) $(histfile)",
        'should_transfer_files': 'NO',
        'on_exit_remove': '(ExitBySignal == False) && (ExitCode == 0)',
    }
    sub_path = os.path.join(abs_jobs_dir, "make_plots_jobs.sub")
    with open(sub_path, "w") as f:
        for k, v in sub.items():
            f.write(f"{k} = {v}\n")
        f.write("queue variable, histfile from (\n")
        for v in variables:
            f.write(f"{v} {hist_files[v]}\n")
        f.write(")\n")

    print(f"[blue]Prepared {len(variables)} condor jobs[/]")
    print(f"  Jobs dir   : {abs_jobs_dir}")
    print(f"  Hist cache : {hist_cache_dir}")
    print(f"  Submit     : {sub_path}")
    print(f"  Logs       : {log_dir}")
    print(f"  Memory     : {mem_per_worker}")
    print(f"  Cores      : {cores_per_worker}")
    print(f"  Queue      : {condor_queue}")

    if dry_run:
        print("[yellow]Dry run — not submitting. To submit manually:[/]")
        print(f"  cd {abs_jobs_dir} && condor_submit make_plots_jobs.sub")
        return

    rc = os.system(f"cd {shlex.quote(abs_jobs_dir)} && condor_submit make_plots_jobs.sub")
    if rc != 0:
        sys.exit(f"[red]condor_submit failed with exit code {rc}[/]")
