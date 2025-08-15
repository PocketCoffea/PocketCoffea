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
from rich import print as rprint
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
import time
import re
import cloudpickle
from copy import deepcopy
from pocket_coffea.utils.rucio import get_xrootd_sites_map
from collections import Counter

queues = [
    "espresso",
    "microcentury",
    "longlunch",
    "workday",
    "tomorrow",
    "testmatch",
    "nextweek"
]
    


def get_tables(tot_jobs, idle_jobs, running_jobs, done_jobs, failed_jobs, details=False):
    # Summary table
    table1 = Table(title="Job Summary")
    table1.add_column("Total jobs", style="cyan", no_wrap=True)
    table1.add_column("Idle jobs", style="blue", no_wrap=True)
    table1.add_column("Running jobs", style="magenta", no_wrap=True)
    table1.add_column("Done jobs", style="green", no_wrap=True)
    table1.add_column("Failed jobs", style="red", no_wrap=True)
    table1.add_row(str(len(tot_jobs)),
                  str(len(idle_jobs)),
                  str(len(running_jobs)),
                  str(len(done_jobs)),
                  str(len(failed_jobs)))
    # Create a table to display the status
    if details:
        table2 = Table(title="Job Status")
        table2.add_column("Job ID", style="cyan", no_wrap=True)
        table2.add_column("Submitted", style="blue", no_wrap=True)
        table2.add_column("Running", style="magenta", no_wrap=True)
        table2.add_column("Done", style="green", no_wrap=True)
        table2.add_column("Failed", style="red", no_wrap=True)
        for job in tot_jobs:
            table2.add_row(job,
                          "X" if job in idle_jobs else "",
                          "X" if job in running_jobs else "",
                          "X" if job in done_jobs else "",
                          "X" if job in failed_jobs else "")
    else:
        table2 = None
    return table1, table2


# Layout setup
def create_layout():
    layout = Layout()

    layout.split_row(
        Layout(name="left"),
        Layout(name="right")
    )
    #layout["left"].size = 60  # You can adjust the width of the left panel
    return layout

def check_jobs_logs(jobs_folder):
     # Idle jobs
    idle_jobs = [ a.split("/")[-1][:-5] for a in glob.glob(f"{jobs_folder}/job_*.idle")]
    # Running jobs
    running_jobs = [a.split("/")[-1][:-8] for a in glob.glob(f"{jobs_folder}/job_*.running")]
    # Done jobs
    done_jobs = [ a.split("/")[-1][:-5] for a in glob.glob(f"{jobs_folder}/job_*.done")]
    # Failed jobs
    failed_jobs = [ a.split("/")[-1][:-7] for a in glob.glob(f"{jobs_folder}/job_*.failed")]
    return idle_jobs, running_jobs, done_jobs, failed_jobs

def find_other_file(filepath,sitemap,xrootdfaillist=[],blacklist_sites=[]):
    if filepath.startswith("root:/"):
        rootpref = filepath.split("/store/")[0]
        file = "/store/"+filepath.split("/store/")[1]
    else:
        rootpref = None
        file = filepath
    
    command = f'/cvmfs/cms.cern.ch/common/dasgoclient -query="site file={file}"' 
    sites = os.popen(command,'r').read().split()
    for site in sites:
        if site not in sitemap:
            continue
        sitepath = sitemap[site]
        if not isinstance(sitepath,str):
            continue
        if sitepath in blacklist_sites:
            continue
        if sitepath+file in xrootdfaillist:
            continue
        if rootpref:
            if rootpref in sitepath or sitepath in rootpref:
                continue
        return sitepath+file
                        
    return filepath  

def update_blacklist(xrootdfaillist,blacklist_threshold):
    sitepathlist = [i.split("/store/")[0] for i in xrootdfaillist]
    failedsitecounter = Counter(sitepathlist)
    blacklist_sites = []
    for site,fails in failedsitecounter.items():
        if fails > blacklist_threshold:
            blacklist_sites.append(site)
    return blacklist_sites

def bump_jobqueue(sub_file):
    with open(sub_file) as f:
        lines = f.readlines()
    with open(sub_file, "w") as f:
        for line in lines:
            if "+JobFlavour" in line:
                jf = line.split("=")[1].strip().replace('"', '')
                next_jf = queues[min(queues.index(jf)+1, len(queues)-1)]
                f.write(f'+JobFlavour="{next_jf}"\n')
            else:
                f.write(line)
    return next_jf

@click.command()
@click.option("-j", "--jobs-folder", type=str, help="Folder containing the jobs", required=True)
@click.option("-d","--details", is_flag=True, help="Show the details of the jobs")
@click.option("-r","--resubmit", is_flag=True, help="Resubmit the failed jobs")
@click.option("-m","--max-resubmit", type=int, help="Maximum number of resubmission", default=3)
@click.option("--blacklist-threshold", type=int, help="Maximum number of allowed failed files at an xrootd site before it's blacklisted", default=10)
def check_jobs(jobs_folder, details, resubmit, max_resubmit, blacklist_threshold):
    # check if the user passed the parent folder
    subdirs = os.listdir(jobs_folder)
    if len(subdirs) == 1 and subdirs[0] == "job":
        jobs_folder = os.path.join(jobs_folder,"job")

    jobs_folder = Path(jobs_folder)
    # Get the list of files in the folder
    tot_jobs = [ a.split("/")[-1][:-4] for a in glob.glob(f"{jobs_folder}/job_*.sub") ]
    # Redo everything every 5 sec
    console = Console()

    failed_jobs_stats = {}
    tot_done = 0

    maxtimefile = f"{jobs_folder}/maxtime.txt"
    if os.path.isfile(maxtimefile):
            with open(maxtimefile,"r") as f:
                maxtimelist = [l.strip() for l in f.readlines()]
    else:
        maxtimelist = []

    if resubmit:
        sitemap = get_xrootd_sites_map()
        xrootdfailfile = f"{jobs_folder}/xrootdfaillist.txt"
        if os.path.isfile(xrootdfailfile):
            with open(xrootdfailfile,"r") as f:
                xrootdfaillist = [l.strip() for l in f.readlines()]
        else:
            xrootdfaillist = []
        os.makedirs(f"{jobs_folder}/logs/processedlogs", exist_ok=True)
        blacklist_sites = update_blacklist(xrootdfaillist,blacklist_threshold)
        if len(blacklist_sites) > 0:
            print("Blacklisted sites:",blacklist_sites)
    
    # Main loop
    layout = create_layout()
    idle_jobs, running_jobs, done_jobs, failed_jobs = check_jobs_logs(jobs_folder)
    tables = get_tables(tot_jobs, idle_jobs, running_jobs, done_jobs, failed_jobs, details=details)
    layout["left"].update(Panel(tables[0], title="Job Status"))
    layout["right"].update(Panel("No logs yet", title="Log"))
    
    log_text = []
    definitive_failed = []
    condor_history_fails = []
    step = 0
    
    with Live(layout, refresh_per_second=1/5, console=console):  # Refresh rate
        try:
            while True:
                step += 1 
                idle_jobs, running_jobs, done_jobs, failed_jobs = check_jobs_logs(jobs_folder)
                tables = get_tables(tot_jobs, idle_jobs, running_jobs, done_jobs, failed_jobs, details=details)
                # Update the left panel with a new table
                layout["left"].update(Panel(tables[0], title="Job Status"))

                # Checking failed jobs
                if len(failed_jobs) > 0:
                    if len(failed_jobs) > len(definitive_failed) and not resubmit:
                        log_text.append("[red]Failed jobs found. Check the details below. Use --resubmit to resubmit the failed jobs[/]")
                    resubmit_count = 0
                    for failed_job in failed_jobs:
                        if failed_job in failed_jobs_stats:
                            if failed_job not in definitive_failed:
                                failed_jobs_stats[failed_job] += 1
                        else:
                            failed_jobs_stats[failed_job] = 1

                        failed_job_num = failed_job.split('_')[1]

                        if not failed_job in definitive_failed:
                            # Check the log file
                            glob_file = glob.glob(f"{jobs_folder}/logs/job_*.{failed_job_num}.err")
                            xrootdfile = None
                            if glob_file:
                                with open(glob_file[-1]) as f:
                                    c = f.readlines()
                                    for iln,ln in enumerate(c):
                                        if "OSError: XRootD error" in ln:
                                            xrootdfile = c[iln+1].strip().split()[-1]
                                            break
                                        if "FileNotFoundError: file not found" in ln:
                                            xrootdfile = c[iln+3].strip().strip("'")
                                            break
                                    if xrootdfile:
                                        thisxrootdsite = xrootdfile.split('/store/')[0]
                                        log_text.append( f"[b]Job {failed_job} failed[/] {failed_jobs_stats[failed_job]} times due to an XRootD error. Site: {thisxrootdsite}")
                                    else:
                                        log_text.append( f"[b]Job {failed_job} failed[/] {failed_jobs_stats[failed_job]} times. Last error:")
                                        log_text.append("\t"+ "".join(c[-3:]))
                            else:
                                log_text.append( f"Error in job {failed_job}: No .err file found")

                            if resubmit and failed_jobs_stats[failed_job] <= max_resubmit:
                                if xrootdfile:
                                    # Include the failed file in the global list so that it's not reused later
                                    if xrootdfile not in xrootdfaillist:
                                        with open(xrootdfailfile,"a") as f:
                                            f.write(xrootdfile+"\n")
                                        xrootdfaillist.append(xrootdfile)
                                        new_blacklist_sites = update_blacklist(xrootdfaillist,blacklist_threshold)
                                        if len(new_blacklist_sites) > len(blacklist_sites):
                                            diff = len(new_blacklist_sites) - len(blacklist_sites)
                                            log_text.append(f"[red][b]New blacklist sites[/]: {new_blacklist_sites[-diff:]}[/]")
                                            blacklist_sites = new_blacklist_sites

                                    # Move the .err file so that this xrootdfile is not marked again as an XRootD failure
                                    if glob_file:
                                        os.system(f"mv {glob_file[-1]} {jobs_folder}/logs/processedlogs")

                                    # Update the filelist in the failed job's config to exclude this failed file
                                    thisconfigfile = f"{jobs_folder}/config_{failed_job}.pkl"
                                    config = cloudpickle.load(open(thisconfigfile, "rb"))
                                    current_fileset = config.filesets
                                    new_fileset = deepcopy(current_fileset)
                                    for sample, dct in new_fileset.items():
                                        fllist = dct['files']
                                        if xrootdfile in fllist:
                                            flidx = fllist.index(xrootdfile)
                                            newfl = find_other_file(xrootdfile,sitemap,xrootdfaillist,blacklist_sites)
                                            if newfl != xrootdfile:
                                                new_fileset[sample]['files'][flidx] = newfl
                                                config.set_filesets_manually(new_fileset)
                                                cloudpickle.dump(config, open(thisconfigfile, "wb"))
                                                log_text.append(f"[b]Job {failed_job}[/]: Updated XRootD path of failed file to a new site.")
                                            else:
                                                log_text.append(f"[b]Job {failed_job}[/]: No alternative site found for {xrootdfile}. Resubmitting with the same file!")
                                        
                                    # Enforce blacklist              
                                    # Take this opportunity to replace all files in the config that are
                                    # at one of the blacklisted sites          
                                    if len(blacklist_sites) > 0:
                                        flcounter = 0
                                        samecounter = 0
                                        sitecounter = []
                                        for sample, dct in new_fileset.items():
                                            fllist = dct['files']
                                            newfllist = []
                                            for flname in fllist:
                                                thissite = flname.split("/store/")[0]
                                                if thissite in blacklist_sites:
                                                    newfl = find_other_file(flname,sitemap,xrootdfaillist,blacklist_sites)
                                                    newfllist.append(newfl)
                                                    if newfl != flname:
                                                        flcounter += 1
                                                        if thissite not in sitecounter:
                                                            sitecounter.append(thissite)
                                                    else:
                                                        samecounter += 1
                                                else:
                                                    newfllist.append(flname)
                                            
                                            new_fileset[sample]['files'] = newfllist

                                        config.set_filesets_manually(new_fileset)
                                        cloudpickle.dump(config, open(thisconfigfile, "wb"))

                                        if flcounter > 0:
                                            log_text.append(f"[b]Job {failed_job}[/]: Replaced {flcounter} files in config because they were in {sitecounter} blacklisted sites.")
                                        if samecounter > 0:
                                            log_text.append(f"[red][b]Job {failed_job}[/]: Could not replace {samecounter} files in config though they were in blacklisted sites, because no alternative site was found![/]")

                                os.system(f"rm {jobs_folder}/{failed_job}.failed")
                                os.system(f"touch {jobs_folder}/{failed_job}.idle")
                                resubmit_log = os.popen(f"cd {jobs_folder} && condor_submit {failed_job}.sub",'r').read().split('\n')[-2]
                                log_text.append(resubmit_log)
                                resubmit_count += 1
                                if resubmit_count % 10 == 0:
                                    rprint(f"[green]Resubmitted {resubmit_count} jobs so far in step {step}[/]")   # Terminal output so that the user knows something's going on
                            else:
                                # Add it to the list of jobs that are definitely failed
                                definitive_failed.append(failed_job)
                    if resubmit_count > 0:
                        log_text.append(f"[red]Resubmitted {resubmit_count} failed jobs to condor[/]")

                # check in the logs for SYSTEM_PERIODIC_REMOVE
                # they are not failed but remain running/idle
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
                            cluster_id = match.group(1)
                            job_id = match.group(2)
                            job_name = f"{cluster_id}_{job_id}"

                            # If this job was already resubmitted, skip
                            if job_name in maxtimelist:
                                continue

                            thisjob = f"job_{job_id}"
                            if thisjob in running_jobs or thisjob in idle_jobs:
                                if thisjob in running_jobs:
                                    running_jobs.remove(thisjob)
                                    os.system(f"rm {jobs_folder}/{thisjob}.running")
                                
                                # Sometimes jobs which never run also get aborted; they have the idle tag
                                # but exist in the log file as and aborted job
                                if thisjob in idle_jobs:
                                    idle_jobs.remove(thisjob)
                                    os.system(f"rm {jobs_folder}/{thisjob}.idle")

                                failed_jobs.append(thisjob)                                
                                os.system(f"touch {jobs_folder}/{thisjob}.failed")

                                maxtimelist.append(job_name)
                                with open(maxtimefile,'a') as f:
                                    f.write(job_name+"\n")

                                # Modify the sub file
                                # Check if next line has SYSTEM_PERIODIC_REMOVE
                                if not "SYSTEM_PERIODIC_REMOVE" in c[il+1]:
                                    log_text.append(f"{thisjob} was aborted by condor. Check the log file for more details")
                                else:     
                                    sub_file = f"{jobs_folder}/{thisjob}.sub"
                                    next_jf = bump_jobqueue(sub_file)                                    

                                    log_text.append(f"{thisjob} was removed by the system due to max-time reached. Marked as failed and bumped to longer condor queue: {next_jf}.")

                                    # No need to resubmit, just let the next pass handle it in its first section
                                    # os.system(f"cd {jobs_folder} && condor_submit {thisjob}.sub")
                                    # os.system(f"rm {jobs_folder}/{thisjob}.failed")
                                    # os.system(f"touch {jobs_folder}/{thisjob}.idle")
                
                # Now check jobs which were resubmitted by this script but then failed again
                # Look for "job was aborted"
                failedlogs = os.popen(f'grep -il {jobs_folder}/logs/job_*.log -e "Job was aborted"').read().split("\n")[:-1]
                for failedlog in failedlogs:
                    # Skip the OG log
                    if log_file.split("/")[-1] in failedlog:
                        continue
                    failedlogcluster = failedlog.split("/")[-1].replace("job_","").replace(".log","")
                    # Try to get job history
                    cmd = ['stdbuf', '-oL', 'condor_history', failedlogcluster]
                    proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, text=True)
                    try:
                        stdout, stderr = proc.communicate(timeout=1)
                    except sp.TimeoutExpired:
                        proc.kill()
                        stdout, stderr = proc.communicate()
                    jobid = None
                    if '\n' in stdout:
                        stdoutlines = stdout.split('\n')
                        if len(stdoutlines) > 2:
                            lastline = stdoutlines[-2]
                            jobid = lastline.split()[-4]

                    if not jobid:
                        if failedlog not in condor_history_fails:
                            # Report once, but don't keep reporting the same thing
                            log_text.append(f"[red]Detected a failed job log {failedlog} but could not retrieve condor_history.[/r]")
                            condor_history_fails.append(failedlog)
                    else:
                        job_name = f"{failedlogcluster}_{jobid}"
                        if job_name in maxtimelist:
                            continue

                        thisjob = f"job_{jobid}"
                        if thisjob in running_jobs or thisjob in idle_jobs:
                            if thisjob in running_jobs:
                                running_jobs.remove(thisjob)
                                os.system(f"rm {jobs_folder}/{thisjob}.running")
                            
                            # Sometimes jobs which never run also get aborted; they have the idle tag
                            # but exist in the log file as and aborted job
                            if thisjob in idle_jobs:
                                idle_jobs.remove(thisjob)
                                os.system(f"rm {jobs_folder}/{thisjob}.idle")

                            failed_jobs.append(thisjob)                                
                            os.system(f"touch {jobs_folder}/{thisjob}.failed")

                            maxtimelist.append(job_name)
                            with open(maxtimefile,'a') as f:
                                f.write(job_name+"\n")

                            # Modify the sub file
                            # Check if log file has SYSTEM_PERIODIC_REMOVE
                            with open(failedlog,"r") as f:
                                lines = f.readlines()
                            
                            dobump = False
                            for line in lines:
                                if "SYSTEM_PERIODIC_REMOVE" in line:
                                    dobump = True
                                    break

                            if not dobump:
                                log_text.append(f"{thisjob} was aborted by condor. Check the log file for more details")
                            else:     
                                sub_file = f"{jobs_folder}/{thisjob}.sub"
                                next_jf = bump_jobqueue(sub_file)                                    

                                log_text.append(f"{thisjob} was removed by the system due to max-time reached. Marked as failed and bumped to longer condor queue: {next_jf}.")
                            
                            os.system(f"mv {failedlog} {jobs_folder}/logs/processedlogs")
                   
                if len(log_text):
                    if len(log_text) > 20:
                        log_text = log_text[-20:]
                    layout["right"].update(Panel("\n".join(log_text), title="Log"))

                if len(tot_jobs) == len(done_jobs) + len(failed_jobs):
                    rprint("[green]All jobs are completed[/]")
                    break
                time.sleep(5)
        except KeyboardInterrupt:
            pass

if __name__ == "__main__":
    check_jobs()
