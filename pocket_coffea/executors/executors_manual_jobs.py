import os
from abc import ABC, abstractmethod
from coffea import processor as coffea_processor
from pocket_coffea.utils.network import get_proxy_path
from rich import print
from rich.table import Table
from math import ceil

class ExecutorFactoryManualABC(ABC):

    def __init__(self, run_options, outputdir, **kwargs):
        self.run_options = run_options
        self.job_name = run_options.get("job-name", "job")
        self.jobs_dir = os.path.join(run_options.get("jobs-dir", outputdir), self.job_name)
        recreate_jobs = run_options.get("recreate-jobs", None)
        if not recreate_jobs:
            if  os.path.exists(self.jobs_dir):
                print(f"Jobs directory {self.jobs_dir} already exists. Please clean it up before running the jobs.")
                exit(1)
            else:
                os.makedirs(self.jobs_dir)
        self.setup()
        # If handles_submission == True, the executor is responsible for submitting the job
        self.handles_submission = True

    @abstractmethod
    def get(self):
        pass

    def setup(self):
        self.setup_proxyfile()
        self.set_env()

    def setup_proxyfile(self):
        if self.run_options['ignore-grid-certificate']: return
        if vomsproxy:=self.run_options.get('voms-proxy', None) is not None:
             self.x509_path = vomsproxy
        else:
             _x509_localpath = get_proxy_path()
             # Copy the proxy to the home from the /tmp to be used by workers
             self.x509_path = os.environ['HOME'] + f'/{_x509_localpath.split("/")[-1]}'
             print("Copying proxy file to $HOME.")
             os.system(f'scp {_x509_localpath} {self.x509_path}')       # scp makes sure older file is overwritten without prompting
             
    def set_env(self):
        # define some environmental variable
        # that are general enought to be always useful
        vars= {
            "XRD_RUNFORKHANDLER": "1",
            "MALLOC_TRIM_THRESHOLD_" : "0",
        }
        if not self.run_options['ignore-grid-certificate']:
            vars["X509_USER_PROXY"] = self.x509_path
        for k,v in vars.items():
            os.environ[k] = v

    def customized_args(self):
        return {}

    def close(self):
        pass


    def submit(self, config, filesets, outputdir):
        # storing the job config
        self.config = config
        self.outputdir = outputdir
        self.filesets = filesets
        if jobs_to_recreate:=self.run_options.get("recreate-jobs", None):
            # Don't run the splitting but read the jobs config, recreate the configurator
            # and submit the jobs
            self.recreate_jobs(jobs_to_recreate)
        else:
            splits = self.prepare_splitting(filesets)
            job_configs = self.prepare_jobs(splits)
            self.submit_jobs(job_configs)

    def prepare_splitting(self, filesets):
        '''Looking at the run options the fileset can be split in different ways.
        The goal is to have uniform splitting across jobs. We can both split and merge different datasets.

        Three splitting modes are supported, in order of precedence:

        1. `max-events-per-job` as a **dict** mapping sample name -> events-per-job
           (with an optional `default` fallback). In this mode each dataset is split
           **independently** so a single job contains files from exactly one
           sample/dataset. Useful when sample sizes differ by orders of magnitude.
        2. `max-events-per-job` as a **scalar**: same as (1) but with a single limit
           applied to every sample, and jobs may pack multiple datasets together
           (legacy behaviour).
        3. `scaleout`: target total number of jobs; the per-job event budget is
           derived as `tot_n_events // scaleout`.
        '''
        tot_n_events = sum([int(fileset["metadata"]["nevents"]) for fileset in filesets.values()])
        max_events_per_job = self.run_options.get("max-events-per-job", None)

        if isinstance(max_events_per_job, dict):
            jobs, nfiles_jobs = self._split_per_sample(filesets, max_events_per_job)
        else:
            if max_events_per_job is not None:
                max_events_per_job = int(max_events_per_job)
                n_jobs = tot_n_events // max_events_per_job
            else:
                n_jobs = self.run_options.get("scaleout", None)
                if n_jobs is None:
                    raise Exception("No splitting strategy provided --> please provide either njobs or max-events-per-job")
                max_events_per_job = tot_n_events // n_jobs
            print(f"Splitting the fileset in {n_jobs} jobs with {max_events_per_job} events each")
            jobs, nfiles_jobs = self._split_uniform(filesets, max_events_per_job)

        self._print_jobs_table(jobs, nfiles_jobs, tot_n_events)
        return jobs

    @staticmethod
    def _split_uniform(filesets, max_events_per_job):
        '''Legacy mixed-dataset splitting: pack files across datasets until each
        accumulating job hits `max_events_per_job` events.'''
        jobs = []
        current_job = {}
        current_job_nevents = 0
        current_fileset_job = []
        nfiles_jobs = []

        for dataset_name, fileset in filesets.items():
            nevents_per_file = ceil(int(fileset["metadata"]["nevents"]) / len(fileset["files"]))
            for file in fileset["files"]:
                current_fileset_job.append(file)
                current_job_nevents += nevents_per_file
                if current_job_nevents >= max_events_per_job:
                    current_job[dataset_name] = {
                        "files": current_fileset_job,
                        "metadata": fileset["metadata"],
                    }
                    jobs.append(current_job)
                    nfiles_jobs.append(current_job_nevents)
                    current_job = {}
                    current_job_nevents = 0
                    current_fileset_job = []
            # when the fileset is done we add it to the current job
            if len(current_fileset_job) > 0:
                current_job[dataset_name] = {
                    "files": current_fileset_job,
                    "metadata": fileset["metadata"],
                }
                current_fileset_job = []
        # Add the last job
        if current_job_nevents > 0:
            jobs.append(current_job)
            nfiles_jobs.append(current_job_nevents)
        return jobs, nfiles_jobs

    @staticmethod
    def _split_per_sample(filesets, max_events_per_job_by_sample):
        '''Per-sample splitting: each dataset is split independently using its
        sample-specific events-per-job budget. Output jobs contain files from
        exactly one dataset.'''
        default_limit = max_events_per_job_by_sample.get("default", None)
        known_keys = {k for k in max_events_per_job_by_sample if k != "default"}

        # Discover the samples actually present in this fileset to surface typos early.
        samples_present = {fs["metadata"]["sample"] for fs in filesets.values()}
        unknown_keys = known_keys - samples_present
        if unknown_keys:
            print(f"[max-events-per-job] WARNING: dict keys {sorted(unknown_keys)} do not match any sample in the current fileset. "
                  f"Samples present: {sorted(samples_present)}. These keys will be ignored.")

        jobs = []
        nfiles_jobs = []
        for dataset_name, fileset in filesets.items():
            sample = fileset["metadata"]["sample"]
            limit = max_events_per_job_by_sample.get(sample, default_limit)
            if limit is None:
                raise Exception(
                    f"max-events-per-job dict has no entry for sample {sample!r} (dataset {dataset_name!r}) "
                    f"and no 'default' fallback. Provide one or the other."
                )
            limit = int(limit)
            nevents = int(fileset["metadata"]["nevents"])
            print(f"[max-events-per-job] {dataset_name} (sample={sample}): {nevents:_} events, limit {limit:_} ev/job")

            nevents_per_file = ceil(nevents / len(fileset["files"]))
            current_files = []
            current_nevents = 0
            for file in fileset["files"]:
                current_files.append(file)
                current_nevents += nevents_per_file
                if current_nevents >= limit:
                    jobs.append({
                        dataset_name: {"files": current_files, "metadata": fileset["metadata"]}
                    })
                    nfiles_jobs.append(current_nevents)
                    current_files = []
                    current_nevents = 0
            if current_files:
                jobs.append({
                    dataset_name: {"files": current_files, "metadata": fileset["metadata"]}
                })
                nfiles_jobs.append(current_nevents)
        return jobs, nfiles_jobs

    @staticmethod
    def _print_jobs_table(jobs, nfiles_jobs, tot_n_events):
        table = Table(title="Jobs")
        table.add_column("Job", justify="right", style="cyan")
        table.add_column("Dataset", justify="right", style="magenta")
        table.add_column("N files", justify="right", style="green")
        table.add_column("N events", justify="right", style="green")
        for i, job in enumerate(jobs):
            for e, (dataset_name, fileset) in enumerate(job.items()):
                if e == 0:
                    table.add_row(str(i), dataset_name, str(len(fileset["files"])), f"{nfiles_jobs[i]:_}")
                else:
                    table.add_row("", dataset_name, str(len(fileset["files"])))
            table.add_row("", "", "", "")
        table.add_section()
        tot_files = sum(len(ds["files"]) for job in jobs for ds in job.values())
        table.add_row(f"{len(jobs)}", "Total", str(tot_files), f"{tot_n_events:_}")
        print(table)

    @abstractmethod
    def prepare_jobs(self, splits):
        pass

    @abstractmethod
    def submit_jobs(self, jobs):
        pass            

    @abstractmethod
    def recreate_jobs(self, jobs):
        pass
