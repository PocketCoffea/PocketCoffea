'''Simple script that checks the status of the jobs submitted by runner on condor.

The status of the jobs can be checked by looking at the file in the jobs folder.

- job_x.idle: The job is waiting to be executed
- job_x.running: The job is running
- job_x.done: The job has finished
- job_x.failed: The job has failed
    
    where x is the job id.
'''

import os
import sys
import click
import glob
import subprocess as sp
from pathlib import Path
from rich.console import Console
from rich.table import Table
from rich.progress import Progress
import time
import re

queues = [
    "espresso",
    "microcentury",
    "longlunch",
    "workday",
    "tomorrow",
    "testmatch",
    "nextweek"
]

@click.command()
@click.option("-j", "--jobs-folder", type=str, help="Folder containing the jobs", required=True)
@click.option("-d","--details", is_flag=True, help="Show the details of the jobs")
def check_jobs(jobs_folder, details):
    jobs_folder = Path(jobs_folder)
    # Get the list of files in the folder
    tot_jobs = [ a[:-4] for a in glob.glob(f"{jobs_folder}/job_*.sub")]
    # Redo everything every 5 sec
    console = Console()

    with Progress() as progress:
        task = progress.add_task("[green]Completed jobs...", total=len(tot_jobs))

        tot_done = 0        
        """Check the status of the jobs in the given folder"""
        while not progress.finished:
            # Idle jobs
            idle_jobs = [ a.split("/")[-1][:-5] for a in glob.glob(f"{jobs_folder}/job_*.idle")]
            # Running jobs
            running_jobs = [a.split("/")[-1][:-8] for a in glob.glob(f"{jobs_folder}/job_*.running")]
            # Done jobs
            done_jobs = [ a.split("/")[-1][:-5] for a in glob.glob(f"{jobs_folder}/job_*.done")]
            if len(done_jobs) > tot_done:
                progress.update(task, advance=len(done_jobs)-tot_done)
                tot_done = len(done_jobs)
                
            # Failed jobs
            failed_jobs = [ a.split("/")[-1][:-7] for a in glob.glob(f"{jobs_folder}/job_*.failed")]
            # Summary table
            table = Table(title="Job Summary")
            table.add_column("Total jobs", style="cyan", no_wrap=True)
            table.add_column("Idle jobs", style="blue", no_wrap=True)
            table.add_column("Running jobs", style="magenta", no_wrap=True)
            table.add_column("Done jobs", style="green", no_wrap=True)
            table.add_column("Failed jobs", style="red", no_wrap=True)
            table.add_row(str(len(tot_jobs)),
                          str(len(idle_jobs)),
                          str(len(running_jobs)),
                          str(len(done_jobs)),
                          str(len(failed_jobs)))
            console.print(table)
            
            # Create a table to display the status
            if details:
                table = Table(title="Job Status")
                table.add_column("Job ID", style="cyan", no_wrap=True)
                table.add_column("Submitted", style="blue", no_wrap=True)
                table.add_column("Running", style="magenta", no_wrap=True)
                table.add_column("Done", style="green", no_wrap=True)
                table.add_column("Failed", style="red", no_wrap=True)
                
                for job in tot_jobs:
                    table.add_row(job,
                                  "X" if job in idle_jobs else "",
                                  "X" if job in running_jobs else "",
                                  "X" if job in done_jobs else "",
                                  "X" if job in failed_jobs else "")
                    
                console.print(table)

            
            # checked in the logs for SYSTEM_PERIODIC_REMOVE
            # They are not failed but remain running
            log_file = glob.glob(f"{jobs_folder}/logs/job_*.log")[0]
            with open(log_file) as f:
                c = f.readlines()
                for il, line in enumerate(c):
                    if line.startswith("009"):
                        # Match with a regex the job id from this
                        # line format "005 (5189350.010.000) 11/15 21:29:13 Job aborted
                        pattern = re.compile(r"\((\d+)\.(\d+)\.\d+\)")
                        match = pattern.search(line)
                        if match:
                            job_id = match.group(2)
                            if job_id in running_jobs:
                                running_jobs.remove(f"job_{job_id}")
                                failed_jobs.append(f"job_{job_id}")
                                os.system(f"rm {jobs_folder}/job_{job_id}.running")
                                os.system(f"touch {jobs_folder}/job_{job_id}.failed")
                                # Modify the sub file
                                # Check if next line has SYSTEM_PERIODIC_REMOVE
                                if not "SYSTEM_PERIODIC_REMOVE" in c[il+1]:
                                    print(f"Job {job_id} was aborted by condor. Check the log file for more details")
                                else:
                                    print(f"Job {job_id} was removed by the system by max-time reached. Resubmitting the job with longer queue.")
                                    continue
                                sub_file = f"{jobs_folder}/job_{i}.sub"
                                with open(sub_file) as f:
                                    lines = f.readlines()
                                with open(sub_file, "w") as f:
                                    for line in lines:
                                        if "+JobFlavour" in line:
                                            jf = line.split("=")[1].strip().replace('"', '')
                                            next_jf = queues[(queues.index(jf)+1)%len(queues)]
                                            f.write(f'+JobFlavour="{next_jf}"\n')
                                        else:
                                            f.write(line)

                                os.system(f"cd {jobs_folder} && condor_submit job_{i}.sub")
                                os.system(f"touch {jobs_folder}/job_{i}.idle")
                
                    
            # sleep for 5 sec
            if not progress.finished:
                time.sleep(5)
                console.clear()
               
