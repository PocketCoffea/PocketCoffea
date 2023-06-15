# Running the analysis

The PocketCoffea analysis can be runned in three main modalities

- local iterative processing
    The processor works on one chunk at a time. Useful for debugging and local testing.

- local multiprocessing
    The processor works on the chunks with multiple processes in parallel.
    Fast way to analyze small datasets locally. 

- Dask scale-up
    Scale the processor to hundreds of workers in HTCondor through the Dask scheduler.
    Automatic handling of jobs submissions and results aggregation.
  
Assuming that PocketCoffea is installed, to run the analysis just use the `runner.py` script:

```bash
$ runner.py --help


         ____             __        __  ______      ________          
        / __ \____  _____/ /_____  / /_/ ____/___  / __/ __/__  ____ _
       / /_/ / __ \/ ___/ //_/ _ \/ __/ /   / __ \/ /_/ /_/ _ \/ __ `/
      / ____/ /_/ / /__/ ,< /  __/ /_/ /___/ /_/ / __/ __/  __/ /_/ / 
     /_/    \____/\___/_/|_|\___/\__/\____/\____/_/ /_/  \___/\__,_/  


     usage: runner.py [-h] --cfg CFG -o OUTPUTDIR [-t] [-lf LIMIT_FILES] [-lc LIMIT_CHUNKS] [-e EXECUTOR] [-s SCALEOUT] [-ll LOGLEVEL] [-f]

     Run analysis on NanoAOD files using PocketCoffea processors

     optional arguments:
       -h, --help            show this help message and exit
       --cfg CFG             Config file with parameters specific to the current run
       -o OUTPUTDIR, --outputdir OUTPUTDIR
                             Output folder
       -t, --test            Run with limit 1 interactively
       -lf LIMIT_FILES, --limit-files LIMIT_FILES
                             Limit number of files
       -lc LIMIT_CHUNKS, --limit-chunks LIMIT_CHUNKS
                             Limit number of chunks
       -e EXECUTOR, --executor EXECUTOR
                             Overwrite executor from config (to be used only with the --test options)
       -s SCALEOUT, --scaleout SCALEOUT
                             Overwrite scalout config
       -ll LOGLEVEL, --loglevel LOGLEVEL
                             Logging level
       -f, --full            Process all datasets at the same time
```

## Easy debugging

The easiest way to debug a new processor is to run locally on a single process. The `runner()` script has
the `--test` options which enables the `iterative` processor independently from the running configuration specified in
the configuration file. The processor is run on a file of each input dataset.

```bash
$ runner.py --cfg config.py --test
```

If you want to run locally with multiple processes for a fixed number of chunks just use the options:

```bash
$ runner.py --cfg config.py --test -e futures -s 4 --limit-files 10 --limit-chunks 10
```
