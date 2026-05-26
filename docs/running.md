# Running the analysis
## CLI interface
Once installed in your software environemnt, either by using an apptainer image or a custom python environment,
PocketCoffea exposes different scripts and utilities with a command-line-interface (CLI)

```bash
$> pocket-coffea 

    ____             __        __  ______      ________
   / __ \____  _____/ /_____  / /_/ ____/___  / __/ __/__  ____ _
  / /_/ / __ \/ ___/ //_/ _ \/ __/ /   / __ \/ /_/ /_/ _ \/ __ `/
 / ____/ /_/ / /__/ ,< /  __/ /_/ /___/ /_/ / __/ __/  __/ /_/ /
/_/    \____/\___/_/|_|\___/\__/\____/\____/_/ /_/  \___/\__,_/


Running PocketCoffea version 0.9.11
- Documentation page:  https://pocketcoffea.readthedocs.io/
- Repository:          https://github.com/PocketCoffea/PocketCoffea

Run with --help option for the list of available commands

```

The commands and their options can be explored directly with the CLI: 
```bash
$> pocket-coffea  --help
Usage: pocket-coffea [OPTIONS] COMMAND [ARGS]...

Options:
  -v, --version BOOLEAN  Print PocketCoffea package version
  --help                 Show this message and exit.

Commands:
  build-datasets      Build dataset fileset in json format
  hadd-skimmed-files  Regroup skimmed datasets by joining different files...
  make-plots          Plot histograms produced by PocketCoffea processors
  run                 Run an analysis on NanoAOD files using PocketCoffea...
```

The `run` command is the one used to execute the analysis workflow on the datasets. This is replacing the previous
`pocket-coffea run` script, but it has the same user interface.


# Executors

The PocketCoffea analysis can be runned in different ways, locally or sending out jobs to a cluster throught the Dask
scheduling system. 

- local iterative processing
    The processor works on one chunk at a time. Useful for debugging and local testing.

- local multiprocessing
    The processor works on the chunks with multiple processes in parallel.
    Fast way to analyze small datasets locally. 

- Dask scale-up
    Scale the processor to hundreds of workers in HTCondor through the Dask scheduler.
    Automatic handling of jobs submissions and results aggregation.
  
Assuming that PocketCoffea is installed (for example inside the singularity machine), to run the analysis just use the
`pocket-coffea run` command:

```bash
$> pocket-coffea run --help

    ____             __        __  ______      ________
   / __ \____  _____/ /_____  / /_/ ____/___  / __/ __/__  ____ _
  / /_/ / __ \/ ___/ //_/ _ \/ __/ /   / __ \/ /_/ /_/ _ \/ __ `/
 / ____/ /_/ / /__/ ,< /  __/ /_/ /___/ /_/ / __/ __/  __/ /_/ /
/_/    \____/\___/_/|_|\___/\__/\____/\____/_/ /_/  \___/\__,_/


Usage: pocket-coffea run [OPTIONS]

  Run an analysis on NanoAOD files using PocketCoffea processors

Options:
  --cfg TEXT                      Config file with parameters specific to the
                                  current run  [required]
  -ro, --custom-run-options TEXT  User provided run options .yaml file
  -o, --outputdir TEXT            Output folder  [required]
  -t, --test                      Run with limit 1 interactively
  -lf, --limit-files INTEGER      Limit number of files
  -lc, --limit-chunks INTEGER     Limit number of chunks
  -e, --executor TEXT             Overwrite executor from config (to be used
                                  only with the --test options)
  -s, --scaleout INTEGER          Overwrite scaleout config
  -c, --chunksize INTEGER         Overwrite chunksize config
  -q, --queue TEXT                Overwrite queue config
  -ll, --loglevel TEXT            Console logging level
  -ps, --process-separately       Process each dataset separately
  --filter-years TEXT             Filter the data taking period of the
                                  datasets to be processed (comma separated
                                  list)
  --filter-samples TEXT           Filter the samples to be processed (comma
                                  separated list)
  --filter-datasets TEXT          Filter the datasets to be processed (comma
                                  separated list)
  --resubmit-failed               Resubmit only failed jobs from previous run
                                  (requires failed_jobs.json)
  --executor-custom-setup TEXT    Python module to be loaded as custom
                                  executor setup
  --help                          Show this message and exit.

```

To run with a predefined executor just pass the `--executor` option string when calling the the `run` command

```bash
$> pocket-coffea run --cfg analysis_config.py -o output --executor dask@lxplus
```
Have a look below for more details about the available executor setups.

### Executors availability

The `iterative` and `futures` executors are available everywhere as they run locally (single thread and multi-processing
respectively).


| Site | Supported executor | Executor string|
|------|--------------------|----------------|
|lxplus| dask, condor       | dask@lxplus, condor@lxplus |
|swan| dask                 | dask@swan      |
|T3_CH_PSI| dask            | dask@T3_CH_PSI |
|DESY NAF | parsl           | parsl@DESY     |
|RWTH Aachen LX-Cluster | parsl, dask    | parsl@RWTH, dask@RWTH |
|RWTH CLAIX | parsl, dask          | parsl@CLAIX, dask@CLAIX |
|[Purdue Analysis Facility](https://analysis-facility.physics.purdue.edu)| dask | dask@purdue-af |
|[INFN Analysis Facility](https://infn-cms-analysisfacility.readthedocs.io/)| dask | dask@infn-af |
|Brown brux20 cluster | dask | dask@brux |
|Brown CCV Oscar | dask | dask@oscar |
|Maryland rubin cluster | dask, condor | dask@rubin condor@rubin |

---------------------------------------

## Executors setup
The analysis processors is handled by **executors**. The setup of the executor can vary between sites. A set of
predefined executors has been prepared and configured with default options for tested analysis facilties (lxplus,
T3_CH_PSI). More sites are being included, please send us a PR when you have successfully run PocketCoffea at your
facility!

The preconfigured executors are located in the
[`pocket_coffea/executors`](https://github.com/PocketCoffea/PocketCoffea/tree/main/pocket_coffea/executors) module. 
The default options for the running options and different type of executors are stores in
[`pocket_coffea/parameters/executor_options_defaults.yaml`](https://github.com/PocketCoffea/PocketCoffea/tree/main/pocket_coffea/parameters/executor_options_defaults.yaml). 

For example:
```yaml
general:
  scaleout: 1
  chunksize: 150000
  limit-files: null
  limit-chunks: null
  retries: 20
  tree-reduction: 20
  skip-bad-files: false
  voms-proxy: null
  ignore-grid-certificate: false
  group-samples: null

dask@lxplus:
  scaleout: 10
  cores-per-worker: 1
  mem-per-worker: "2GB"
  disk-per-worker: "2GB"
  worker-image: /cvmfs/unpacked.cern.ch/gitlab-registry.cern.ch/cms-analysis/general/pocketcoffea:lxplus-el9-stable
  death-timeout: "3600"
  queue: "microcentury"
  adaptive: false
  performance-report: true
  custom-setup-commands: null
  conda-env: false
  local-virtualenv: false

```


### Executor options
The dataset splitting (chunksize), the number of workers, memory and most other options
can be re-configured by the user via custom `my_run_options.yaml` file, that is passed to
`pocket-coffea run` command. These options are overwrite the default parameters of the
requested executor.

For example: `$> cat my_run_options.yaml`

```yaml
scaleout: 400
chunksize: 50000
queue: "espresso"
mem-per-worker: 6GB
```
then use `--custom-run-options my_run_options.yaml` to `run`:
```bash
$> pocket-coffea run  --cfg analysis_config.py -o output --executor dask@lxplus  --custom-run-options my_run_options.yaml
```

The user can also modify on the fly some run options using arguments to the `pocket-coffea
run` script. For example, limiting the number of files or number of chunks to analyse (for
testing purposes):

```bash
$> pocket-coffea run  --cfg analysis_config.py -o output --executor dask@lxplus \
              --limit-files 10  --limit-chunks 10 \
              --chunksize 150000 --queue espresso
```


### Filter datasets, samples, and years

The fileset to process can be narrowed down on the command line without editing the configuration file, using the three `--filter-*` options. Each option accepts a comma-separated list of names:

| Option | Filters on |
|--------|-----------|
| `--filter-years` | Data-taking period (e.g. `2016,2017`) |
| `--filter-samples` | Sample name as defined in the configuration (e.g. `TTToSemiLeptonic,DATA_SingleMuon`) |
| `--filter-datasets` | Individual dataset key in the fileset (e.g. `TTToSemiLeptonic_2018`) |

The filters can be combined freely:

```bash
# Run only 2018 data and MC
pocket-coffea run --cfg config.py -o output/ --filter-years 2018

# Run only two specific samples across all years
pocket-coffea run --cfg config.py -o output/ --filter-samples TTToSemiLeptonic,DATA_SingleMuon

# Run a single dataset
pocket-coffea run --cfg config.py -o output/ --filter-datasets TTToSemiLeptonic_2018
```

### Process datasets separately and group samples
By default, the `pocket-coffea run` command will run all the datasets together in one shot and a single output `output_all.coffea` is saved.
In case one wants to save intermediate outputs, it is possible to run with the `--process-separately` option, where each dataset
is processed separately and an independent output `output_{dataset}.coffea` is saved for each dataset.

In case several datasets need to be processed with the `--process-separately` option, there is the additional possibility to group datasets
belonging to the same sample, process them together and save an output `output_{group}.coffea` for each group.
To group samples during processing it is sufficient to add an extra entry to the custom `run_options.yaml` file passed to `pocket-coffea run`,
defining the dictionary `group-samples`. Each key in this dictionary corresponds to the group name, and the values of the dictionary are lists
of samples.

As an example, by adding the following snippet to the `run_options.yaml` file:

```yaml
group-samples:
  signal:
    - "ttHTobb"
    - "ttHTobb_ttToSemiLep"
  TTToSemiLeptonic:
    - "TTToSemiLeptonic"
  TTbbSemiLeptonic:
    - "TTbbSemiLeptonic"
  "TTTo2L2Nu_SingleTop":
    - "TTTo2L2Nu"
    - "SingleTop"
  VJets:
    - "WJetsToLNu_HT"
    - "DYJetsToLL"
  VV_TTV:
    - "VV"
    - "TTV"
  DATA:
    - "DATA_SingleEle"
    - "DATA_SingleMuon"
```
and running the analysis with the command `pocket-coffea run --cfg config.py -ro run_options.yaml --process-separately`, will result in running
the analysis processor sequentially for 7 times, saving 7 independent outputs: `output_signal.coffea`, `output_TTToSemiLeptonic.coffea`, `output_TTbbSemiLeptonic.coffea`,
`output_TTTo2L2Nu_SingleTop.coffea`, `output_VJets.coffea`, `output_VV_TTV.coffea` and `output_DATA.coffea`.
For example, the output file `output_signal.coffea` file will contain the output obtained by processing the datasets of the samples `ttHTobb` and `ttHTobb_ttToSemiLep`,
for all the data-taking years specified in the `datasets["filter"]["year"]` dictionary in the constructor of the Configurator.

### Resubmit failed jobs

When running with `--process-separately`, any dataset or group that fails during processing is automatically tracked. After the run completes, the names of all failed datasets/groups are saved to `failed_jobs.json` inside the output directory:

```json
["dataset_A", "group_XYZ"]
```

A warning is printed at the end of the run to indicate how many jobs failed:

```
WARNING: 2 job(s) failed. Failed jobs saved to output/failed_jobs.json
```

To resubmit only the failed jobs without re-running the successful ones, use the `--resubmit-failed` flag together with `--process-separately`:

```bash
# Initial run — failed jobs are automatically saved to output/failed_jobs.json
pocket-coffea run --cfg config.py -o output/ --process-separately

# Resubmit only the failed jobs
pocket-coffea run --cfg config.py -o output/ --process-separately --resubmit-failed
# INFO: Resubmitting 2 failed jobs: ['dataset_A', 'group_XYZ']
```

The `--resubmit-failed` flag reads `failed_jobs.json` from the output directory and restricts the fileset to only the listed datasets/groups, leaving the outputs of previously successful jobs untouched.

:::{note}
`--resubmit-failed` requires `--process-separately` to be set as well.
:::

### Manual-job (HTCondor) executors

In addition to the Dask executors — which keep a Python scheduler alive on the submit
node and stream results back to it — PocketCoffea also ships **manual-job executors**
that submit one self-contained HTCondor job per chunk-group and let HTCondor do the
scheduling. Currently available:

| Executor string | Site             |
|-----------------|------------------|
| `condor@lxplus` | CERN HTCondor    |
| `condor@rubin`  | Maryland HTCondor |

This mode is best when:

- the run is long and you don't want to keep a Dask scheduler running for hours,
- you want each input chunk-group to live in a clearly identified, individually
  resubmittable job,
- you want to ship the analysis to a separate (singularity) container per job.

#### How it works

When `--executor condor@lxplus` (or `condor@rubin`) is used, PocketCoffea:

1. **Splits the fileset** into `N` job groups based on either `--scaleout N` (total
   number of jobs) or the run option `max-events-per-job: M` (target events per job).
   See `prepare_splitting()` in
   [`pocket_coffea/executors/executors_manual_jobs.py`](https://github.com/PocketCoffea/PocketCoffea/blob/main/pocket_coffea/executors/executors_manual_jobs.py).
2. **Pickles one `Configurator` per job** to `jobs_dir/config_job_{i}.pkl`. Each pickle
   carries the *original* `Configurator` with the fileset filtered down to that job's
   slice. A summary `jobs_dir/jobs_config.yaml` records every job's fileset, config
   path, and output path.
3. **Writes one HTCondor submit file per job** (`job_{i}.sub`) plus a batch
   `jobs_all.sub`, a wrapper `job.sh`, and an empty `job_{i}.idle` flag file. The
   wrapper script copies the per-job output back to `--outputdir` once the job
   succeeds (using `xrdcp` if the output is on EOS).
4. **Submits with `condor_submit jobs_all.sub`** unless `dry-run: true` is set.

Each running job updates a flag file under `jobs_dir/`:

| Flag           | Meaning                                          |
|----------------|--------------------------------------------------|
| `job_{i}.idle`    | Waiting in the queue (also: never started)     |
| `job_{i}.running` | Picked up by HTCondor and currently executing  |
| `job_{i}.done`    | Finished successfully, output copied back      |
| `job_{i}.failed`  | Wrapper exited non-zero (e.g. xrdcp or pocket-coffea failure) |

Logs land in `jobs_dir/logs/job_*.{out,err,log}`.

#### Submitting jobs

```bash
# Split into 50 condor jobs over the whole fileset
pocket-coffea run --cfg config.py -o output/ --executor condor@lxplus --scaleout 50

# Or target a fixed number of events per job, derived from the dataset metadata
pocket-coffea run --cfg config.py -o output/ --executor condor@lxplus \
    --custom-run-options my_run_options.yaml
```

where `my_run_options.yaml` may set:

```yaml
max-events-per-job: 2_000_000       # alternative to scaleout
job-name: "myanalysis_v3"           # subfolder under jobs-dir
jobs-dir: "/eos/user/u/user/jobs"   # default: --outputdir
queue: "workday"                    # HTCondor JobFlavour (condor@lxplus only)
cores-per-worker: 1                 # >1 uses the futures executor inside the job
mem-per-worker: "4GB"
worker-image: "/cvmfs/unpacked.cern.ch/gitlab-registry.cern.ch/cms-analysis/general/pocketcoffea:lxplus-el9-stable"
split-by-category: false            # if true, runs split-output per job
eos-prefix: "root://eosuser.cern.ch/"
custom-setup-commands:              # extra `source ...` lines added to the job env
  - "source /cvmfs/.../setup.sh"
dry-run: false                      # true → prepare jobs_dir but skip condor_submit
```

Defaults for `condor@lxplus` and `condor@rubin` live in
[`pocket_coffea/parameters/executor_options_defaults.yaml`](https://github.com/PocketCoffea/PocketCoffea/blob/main/pocket_coffea/parameters/executor_options_defaults.yaml).
You **must** provide either `--scaleout` or `max-events-per-job` — otherwise the
splitting will produce a single job. The HTCondor `+JobFlavour` queues from shortest to
longest are `espresso`, `microcentury`, `longlunch`, `workday`, `tomorrow`, `testmatch`,
`nextweek`.

##### Per-sample `max-events-per-job`

`max-events-per-job` also accepts a **dict** in the run-options YAML, mapping the
sample name (as it appears in the dataset metadata) to a per-job event budget.
This is the right setting when sample sizes are very uneven and you want short
samples in fast queues without making the long ones spawn thousands of jobs:

```yaml
max-events-per-job:
  default: 2_000_000               # fallback for samples not listed
  TTToSemiLeptonic: 500_000
  ttHTobb: 800_000
  DATA_SingleMuon: 5_000_000
```

In dict mode each dataset is split **independently**, so every condor job contains
files from exactly one sample (and one dataset). Samples that match no explicit key
fall back to `default`; if `default` is also missing the run aborts with a clear
error pointing at the offending sample. Keys that don't match any sample in the
current fileset are reported as a warning, so typos surface quickly.

##### Per-sample `chunksize`

`chunksize` (the in-job Coffea chunk size) accepts the **same dict form** as
`max-events-per-job`:

```yaml
chunksize:
  default: 150_000              # fallback for samples not listed
  TTToSemiLeptonic: 50_000      # heavier events, smaller in-job chunks
  ttHTobb: 80_000
  DATA_SingleMuon: 300_000      # cheap branches, larger chunks fine
```

Each condor job's `.sub` file is then written with **its own** chunksize value
spliced into the `arguments` line, so the inner `pocket-coffea run --chunksize ...`
call inside the job picks up the per-sample budget. The CLI flag
`--chunksize <int>` still works as a global override; it stays an int-only
parameter (dict form is YAML-only).

A few caveats worth knowing about:

- **Pair this with the per-sample `max-events-per-job` dict.** That mode
  already isolates one sample per job, which is exactly what the chunksize
  resolver requires. If you keep uniform splitting and a job happens to
  contain files from more than one sample, the run aborts with a clear
  *"multiple samples"* error pointing at the offending job — the resolver
  refuses to silently pick one chunksize over another.
- **Batch submission is always preserved.** The per-job chunksize is fed
  into HTCondor with the inline `queue chunksize from ( ... )` form — one
  chunksize per line, embedded directly in `jobs_all.sub` — so a single
  `condor_submit jobs_all.sub` call submits everything regardless of whether
  the resolved values are uniform or vary across jobs. The per-job `.sub`
  files are still written and individually carry the right chunksize, so
  `--recreate-jobs` of an individual job works without any special handling.
- **Unknown dict keys produce a warning** listing the samples actually
  present in the current fileset, so typos surface immediately.

This is currently implemented for the manual-job executors (`condor@lxplus`,
`condor@rubin`) only.

#### Tips

- Use `dry-run: true` to inspect the generated `jobs_dir/` tree and the submit files
  before sending them to HTCondor.
- After `condor_submit`, you can monitor the run with `condor_q` and the per-job flag
  files. To collect outputs, use `pocket-coffea merge-outputs output/output_job_*.coffea`
  (the wrapper script names outputs `output_job_{i}.coffea` per job).
- Each job is fully self-contained: it ships its pickled `Configurator`, its X509
  proxy, and the wrapper `job.sh` to the worker. The PocketCoffea code is taken from
  the container image (`worker-image`) — you don't need to ship your local checkout
  unless `local-virtualenv: true` is set.
- For resubmitting only a subset of jobs (e.g. after failures), see the next section.

### Recreate jobs on manual-job executors

The HTCondor-based manual-job executors (`condor@lxplus`, `condor@rubin`, ...) submit one
HTCondor job per chunk-group and pickle the per-job `Configurator` to
`jobs_dir/config_job_{i}.pkl`. To resubmit a subset of these jobs without re-running the
splitting, use `--recreate-jobs`:

```bash
# Resubmit specific jobs
pocket-coffea run --cfg config.py -o output/ --executor condor \
    --recreate-jobs 0,1,3

# Or auto-detect failed/idle/running jobs from the .failed/.idle/.running flag files
pocket-coffea run --cfg config.py -o output/ --executor condor --recreate-jobs auto
```

By default the fileset stored in the pickle is reused as-is, except for jobs whose logs
contain an `XRootD error` — those have each input file rewritten to an alternate site
(via a Rucio `list_replicas` lookup).

Use `--blocklist-sites` together with `--recreate-jobs` to proactively migrate every
file currently served by one of the listed sites:

```bash
pocket-coffea run --cfg config.py -o output/ --executor condor \
    --recreate-jobs auto --blocklist-sites T1_DE_KIT,T2_US_FNAL
```

For each input file whose redirector matches a blocklisted site, Rucio is queried for an
alternative replica at a non-blocklisted site. If none is found, the file is rewritten to
use the global xrootd redirector (`root://xrootd-cms.infn.it//`) instead of keeping the
blocklisted URL. Files at non-blocklisted sites are left untouched, and file order is
preserved. The original `jobs_config.yaml` is not modified — only the per-job pickle.

:::{note}
`--recreate-jobs` is locked to the fileset that was pickled at submission time; it does
not re-read the dataset JSON. If you want to incorporate new files or new sites, rebuild
the dataset with `build-datasets` (optionally with `--blocklist-sites`) and resubmit from
scratch.
:::

#### Change the HTCondor queue on resubmit

Use `--recreate-queue <queue>` together with `--recreate-jobs` to rewrite the
`+JobFlavour` of every resubmitted `.sub` file before sending it to HTCondor. This is
the right knob when the original queue was too short (jobs are getting killed by
`SYSTEM_PERIODIC_REMOVE`) or too long (you want to push a few stragglers into
`espresso` to grab a worker faster):

```bash
# Move the failed jobs from their original queue to "workday"
pocket-coffea run --cfg config.py -o output/ --executor condor \
    --recreate-jobs auto --recreate-queue workday
```

Known values are `espresso`, `microcentury`, `longlunch`, `workday`, `tomorrow`,
`testmatch`, `nextweek`. Unknown values are written verbatim and produce a warning —
useful for site-specific queues but easy to typo. The explicit `--recreate-queue`
overrides the implicit one-step bump that `--recreate-jobs auto` would otherwise apply
to jobs found in the `.running` state.

#### Route every file through the global xrootd redirector

When many sites are flaky and you don't want to spend time on per-file Rucio lookups,
use `--use-redirector` to rewrite **every** file in the resubmitted jobs to use the
global xrootd redirector (`root://xrootd-cms.infn.it//`), letting xrootd figure out
routing on the fly:

```bash
pocket-coffea run --cfg config.py -o output/ --executor condor \
    --recreate-jobs auto --use-redirector
```

This is the fastest recovery path: no Rucio client opened, no DAS query, no
per-sample dependency. It also takes precedence over `--blocklist-sites` — if both
are passed, `--use-redirector` wins and a warning is printed (since the blocklist
becomes meaningless once everything is going through the global redirector). Files
whose URLs don't carry a recognisable `/store/...` LFN are left untouched.

#### Forwarding Coffea-Runner options to the inner job

By default the inner `pocket-coffea run` that runs **inside each condor job**
re-derives its `run_options` from the YAML defaults only — your outer
`--custom-run-options` YAML is consumed by the submitter but is *not* shipped
to the worker. To bridge that gap, the manual-job executor now writes
`jobs_dir/inner_run_options.yaml` at submit (and `--recreate-jobs`) time, ships
it via `transfer_input_files`, and the wrapper passes
`--custom-run-options inner_run_options.yaml` to the inner call.

The YAML is whitelist-filtered to a small set of *Coffea-Runner-side* keys —
the ones the inner `Runner` actually consumes:

- `skip-bad-files`
- `tree-reduction`

Outer-only keys (`cores-per-worker`, `mem-per-worker`, `worker-image`, `queue`,
`scaleout`, `max-events-per-job`, ...) are intentionally **not** propagated:
they describe how the outer HTCondor jobs are sized and scheduled, and have no
meaning inside the worker.

The two most useful CLI flags that hit this channel today:

```bash
# Make every chunk-level read-error survivable, both on the submitter and inside the job
pocket-coffea run --cfg config.py -o output/ --executor condor@lxplus --skip-bad-files

# Same thing applied retroactively to an existing jobs_dir
pocket-coffea run --cfg config.py -o output/ --executor condor@lxplus \
    --recreate-jobs auto --skip-bad-files
```

In the recreate-jobs flow, `inner_run_options.yaml` is **rewritten** from the
outer `run_options`, and `job.sh` plus each resubmitted `.sub` are
idempotently patched to reference it. An existing jobs_dir produced before
this feature shipped therefore picks it up on the first `--recreate-jobs`
call without a fresh submission.

### Monitor and auto-resubmit jobs with `check-jobs`

`pocket-coffea check-jobs` is a live monitoring tool for the manual-job executors.
It polls a `jobs_dir/` every few seconds, prints a rich summary of how many jobs are
idle / running / done / failed (using the `.idle / .running / .done / .failed` flag
files written by the wrapper script), and can optionally drive resubmission of failed
jobs in place — without having to call `pocket-coffea run --recreate-jobs` yourself.

```bash
# Just watch the jobs (read-only)
pocket-coffea check-jobs -j /path/to/output/job

# Watch + auto-resubmit failed jobs as they appear
pocket-coffea check-jobs -j /path/to/output/job --resubmit
```

If you point `-j` at the parent output directory and it contains a single subfolder
called `job`, the tool descends into it automatically.

#### Options

| Flag | Default | Purpose |
|------|---------|---------|
| `-j, --jobs-folder` | *(required)* | Folder containing the `job_*.sub` and `job_*.{idle,running,done,failed}` files. |
| `-d, --details` | off | Show a per-job status table in addition to the summary. |
| `-r, --resubmit` | off | Actively resubmit failed jobs with the recovery logic described below. |
| `-m, --max-resubmit` | `4` | Give up on a job after this many resubmissions. |
| `-b, --blacklist-threshold` | `3` | After this many XRootD failures coming from the same site, that site is added to a local blacklist for the rest of the session. |
| `-q, --queue-shift` | `1` | When HTCondor aborts a job with `SYSTEM_PERIODIC_REMOVE` (max time exceeded), bump its `+JobFlavour` by this many steps along `espresso → microcentury → longlunch → workday → tomorrow → testmatch → nextweek` before resubmitting. |
| `--by sample\|dataset\|none` | `sample` | Show a per-group progress table below the summary, with a stacked coloured bar (green=done, magenta=running, blue=idle, red=failed) and a `% Done` column sorted from slowest to fastest sample. Requires `jobs_config.yaml` in the jobs folder (written by the manual-job executors); pass `none` to disable. If the YAML is missing the tool silently falls back to the legacy single-table layout. |

#### What `--resubmit` actually does

For every job whose flag file is `.failed`, `check-jobs` inspects
`jobs_dir/logs/job_*.{id}.out` and reacts to the failure mode it finds:

- **XRootD read failure** (`OSError: XRootD error` or `FileNotFoundError: file not found`):
  the offending file URL is recorded in `jobs_dir/xrootdfaillist.txt`, the failed file
  is rewritten via a Rucio replica lookup to a non-failed, non-blacklisted replica
  (`find_other_file` inside the script), and the per-job pickle
  `config_job_{i}.pkl` is updated in place. The original log is moved under
  `jobs_dir/logs/processedlogs/` so the same failure is not counted twice.
- **Recurring failures from the same site**: once a site has produced more than
  `--blacklist-threshold` failed reads, it is added to an in-memory blacklist and
  *every* file in the failed job's config that lives at a blacklisted site is
  proactively migrated, not just the one that failed.
- **HTCondor max-time abort** (`SYSTEM_PERIODIC_REMOVE` in the `.log` file): the job's
  `.sub` file is rewritten with the next `+JobFlavour` queue (per `--queue-shift`),
  the job is marked failed, and the next poll picks it up for resubmission. The
  cluster/proc-id is remembered in `jobs_dir/maxtime.txt` so the same abort is not
  bumped again.

After patching, the script issues `condor_submit job_{i}.sub`, removes the `.failed`
flag, and touches `.idle`.

The tool exits automatically when `done + failed == total`, and prints the suggested
next command:

```
All jobs are completed
Now merge outputs with merge-outputs -jc <jobs-folder>
```

Use `Ctrl-C` to detach at any time — the script does not own the jobs, so leaving it
just stops monitoring.

:::{note}
`check-jobs --resubmit` and `pocket-coffea run --recreate-jobs` overlap in
functionality but are aimed at different workflows: `check-jobs` is a long-running
"babysitter" that reacts to failures as they happen, while `--recreate-jobs` is a
one-shot, manual operation you trigger after looking at the state of `jobs_dir/`.
Pick one or the other for a given run — running both at the same time will produce
duplicate `condor_submit` calls.
:::

### Merging skim outputs with a skipped input file

When you merge the per-job outputs of a **skimming** workflow with
`merge-outputs`, PocketCoffea writes a new `skimmed_dataset_definition.json` and
checks, for every dataset, that the number of initial events in the dataset
metadata matches the `cutflow["initial"]` count. A mismatch normally aborts the
merge:

```
ERROR: The number of initial events in the metadata is different from the number of initial events in the cutflow for dataset TTW_2022_postEE
Exception: Inconsistent number of initial events in the output of the skimming processing
```

This is the expected behaviour: it catches input chunks that were silently lost.
But if you **intentionally** skipped a corrupted input file during processing,
the mismatch is legitimate for that one dataset. Use
`--skip-initial-events-check <dataset>` to downgrade the error to a warning for
the named dataset(s) only — every other dataset still aborts on a mismatch:

```bash
# Tolerate the mismatch for one dataset (repeat the flag for more)
merge-outputs -jc /path/to/output/job \
    --skip-initial-events-check TTW_2022_postEE
```

The flag is repeatable (`--skip-initial-events-check A --skip-initial-events-check B`).

:::{note}
The skimmed dataset's `nevents` and `skim_efficiency` for the affected sample
will be computed from the events that were actually processed, so its
cross-section normalisation will be slightly off by the contribution of the
dropped file. Only use this when that residual mismatch is acceptable.
:::

### Customize the executor software environment
The software environment where the executor runs the analysis is defined by the python environment where the analysis is
launched but also by the executor options. 

In particular if the user is using a `virtual environment` or `conda` to develop the core PocketCoffea code inside a singularity
image, there is an option to make the remote executors pickup the correct python env.

Just specify  `local-virtualenv: true` in the custom run options for virtualenv inside the singularity or `conda-env:
true` for using the conda (or mamba/micromamba) env activated where the `pocket-coffea run` script is run.

:::{admonition} Local environment support
:class: warning
The local environment propagation to the remote executors has been implemented at the moment for lxplus and some other
sites.  It is dependent of the presence of a shared filesystem to propagate the environment to the workers and activate
it before executing the dask worker jobs. 
:::

Moreover the user can add a list of completely custom setup commands that are run inside a worker job before executing
the analysis processor. Just specify them in the run options user file `my_run_options.yaml`:

```yaml
custom-setup-commands:
  - echo $HOME
  - source /etc/profile.d/conda.sh
  - export CUSTOM_VARIABLE=1
```

## Dask scheduler on lxplus
The dask scheduler started by the `pocket-coffea run` script needs to stay alive in the user interactive session. 
This means that if you start a runner process directly in the lxplus machine (in a singularity session) you cannot
logout from the session. 

The solution is using the `tmux` program to keep your analysis session in the background. `tmux` allows you to create a
session, detach from it, exit lxplus, and at the next login reattch to the running session. 

This service needs to be activate, only once, for your user with `systemctl --user enable --now tmux.service`. The full
documentation about this (new) feature is available on the [Service
Portal](https://cern.service-now.com/service-portal?id=kb_article\&n=KB0008111).

Once setup you can start a tmux session as:
```bash
tmux new -s your-session-name
# start an apptainer image and launch your analysis

# press `Ctrl+b d` to detach from the session
```
your running session are visible with `tmux ls`. To reconnect do `tmux a -t your-session-name`. Look
[here](https://tmuxcheatsheet.com/) for more info about tmux. 


## Easy debugging

The easiest way to debug a new processor is to run locally on a single process. The `run` command has
the `--test` options which enables the `iterative` processor independently from the running configuration specified in
the configuration file. The processor is run on a file of each input dataset. If you set the `--process-separately` flag, the datasets are processed separately. Otherwise all datasets are processed at once.

```bash
$ pocket-coffea run --cfg config.py --test
```

If you want to run locally with multiple processes for a fixed number of chunks just use the options:

```bash
$ pocket-coffea run --cfg config.py --test -e futures -s 4 --limit-files 10 --limit-chunks 10 
```


## Adding support for a new executor/site

If you want to run PocketCoffea in a analysis environment that is still not centrally implemented you can implement a
custom `ExecutorFactory` and pass it to the `pocket-coffea run` script on the fly. In practice, this means that the user is free
to define from scratch the configuration of its cluster for running with Dask for example. 

Have a look at
[`pocket_coffea/executors/executors_lxplus.py`](https://github.com/PocketCoffea/PocketCoffea/tree/main/pocket_coffea/executors/executors_lxplus.py)
for a full-fledged example. 

The user factory must implement a class deriving from
[ExecutorFactoryABC](https://github.com/PocketCoffea/PocketCoffea/tree/main/pocket_coffea/executors/executors_base.py). 
The factory returns an instance of a Executor that is passed to the coffea Runner. 

```python
# custom executor defined in my_custom_executor.py by the user

from pocket_coffea.executors.executors_base import ExecutorFactoryABC
from .executors_base import IterativeExecutorFactory, FuturesExecutorFactory

from coffea import processor as coffea_processor


class ExecutorFactoryCustom(ExecutorFactoryABC):
    def get(self):
        return coffea_processor.dask_executor(**self.customized_args())

    def setup(self):
        """This function is called by the base class constructor"""
        # do all the needed setup
        self.start_custom_dask_cluster()

    def start_custom_dask_cluster(self):
        self.dask_cluster = None  # custom configuration

    def customized_args(self):
        """This function customized the args that coffea uses to instantiate
        the executor class passed by the get() method"""
        args = super().customized_args()
        args["custom-arg"] = "..."
        return args

    def close(self):
        # cleanup
        self.dask_cluster.close()


def get_executor_factory(executor_name, **kwargs):
    if executor_name == "iterative":
        return IterativeExecutorFactory(**kwargs)
    elif executor_name == "futures":
        return FuturesExecutorFactory(**kwargs)
    elif executor_name == "dask":
        return ExecutorFactoryCustom(**kwargs)
```

The user's module must implement a `get_executor_factory(string, run_options)` method which returns the instantiated Executor. 

The module is then used like this:

```bash
$> pocket-coffea run --cfg analysis_config.py -o output --executor dask  --executor-custom-setup my_custom_executor.py
--run-options my_run_options.yaml
```


:::{tip}
When the setup is working fine we would highly appreciate a PR to add the executor to the list of centrally supported
sites with default options!
:::


