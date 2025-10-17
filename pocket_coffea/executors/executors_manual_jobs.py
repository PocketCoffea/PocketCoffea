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
        By default we take the tot number of events / number of jobs and then split.
        If the run_option max-events-by-job is provided instead we submit the amount of jobs necessary to get the desired number of events.
        '''
        tot_n_events = sum([ int(fileset["metadata"]["nevents"]) for fileset in filesets.values()])
        max_events_per_job = self.run_options.get("max-events-per-job", None)
        if max_events_per_job is not None:
            max_events_per_job = int(max_events_per_job)
            n_jobs = tot_n_events // max_events_per_job
        else:
            n_jobs = self.run_options.get("scaleout", None)
            max_events_per_job = tot_n_events // n_jobs
            if n_jobs is None:
                raise Exception("No splitting strategy provided --> please provide either njobs or max-events-by-job")

        print(f"Splitting the fileset in {n_jobs} jobs with {max_events_per_job} events each")
        jobs = []
        current_job = {}
        current_job_nevents = 0
        current_fileset_job = []
        nfiles_jobs = []
                    
        for dataset_name, fileset in filesets.items():
            nevents_per_file = ceil(int(fileset["metadata"]["nevents"])/ len(fileset["files"]))
            #print(f"\t- Dataset {dataset_name} has {len(fileset['files'])} files with {nevents_per_file} events each")
            for file in fileset["files"]:
                current_fileset_job.append(file)
                current_job_nevents += nevents_per_file
                if current_job_nevents >= max_events_per_job:
                    # Add the job to the list
                    current_job[dataset_name] = {
                        "files": current_fileset_job,
                        "metadata": fileset["metadata"]
                    }
                    # The job is added only when maxevents is reached
                    jobs.append(current_job)
                    nfiles_jobs.append(current_job_nevents)
                    current_job = {}
                    current_job_nevents = 0
                    current_fileset_job = []
            #when the fileset is done we add it to the current job
            if len(current_fileset_job) > 0:
                current_job[dataset_name] = {
                    "files": current_fileset_job,
                    "metadata": fileset["metadata"]
                }
                current_fileset_job = []
        # Add the last job
        if current_job_nevents > 0:
            jobs.append(current_job)
            nfiles_jobs.append(current_job_nevents)

        # Print the jobs with the dataset names and number of files in a rich table
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
        tot_files = sum( [len(ds["files"]) for job in jobs for ds in job.values()])
        table.add_row(f"{len(jobs)}","Total", str(tot_files), f"{tot_n_events:_}")
        print(table)
        return jobs

    @abstractmethod
    def prepare_jobs(self, splits):
        pass

    @abstractmethod
    def submit_jobs(self, jobs):
        pass            

    @abstractmethod
    def recreate_jobs(self, jobs):
        pass
