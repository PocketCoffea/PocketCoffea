from contextlib import contextmanager
import importlib.util
import os
import sys
from typing import List, Optional
import awkward
import pathlib
import shutil
from .configurator import Configurator
import hashlib
from numba import njit
import awkward as ak
import json
import logging

# Constant for the failed jobs filename
FAILED_JOBS_FILENAME = "failed_jobs.json"

@contextmanager
def add_to_path(p):
    old_path = sys.path
    sys.path = sys.path[:]
    sys.path.insert(0, p)
    try:
        yield
    finally:
        sys.path = old_path

def get_random_seed(metadata, salt=""):
    '''Generate a random seed based on the current file and entry range being processed.
    This ensures that different files and different entry ranges will produce different seeds,
    while the same file and entry range will always produce the same seed.
    An optional salt can be provided to further differentiate the seed generation for different function.'''
    key = f"{metadata['fileuuid']}-{metadata['entrystart']}-{metadata['entrystop']}-{salt}".encode("utf-8")
    seed = int(hashlib.sha256(key).hexdigest()[:8], 16)
    return seed

# import a module from a path.
# Solution from https://stackoverflow.com/questions/41861427/python-3-5-how-to-dynamically-import-a-module-given-the-full-file-path-in-the
def path_import(absolute_path):
    if not os.path.exists(absolute_path):
        raise Exception(f"Module path {absolute_path} not found!")
    with add_to_path(os.path.dirname(absolute_path)):
        spec = importlib.util.spec_from_file_location(absolute_path, absolute_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    
def load_config(cfg, do_load=True, save_config=True, outputdir=None):
    ''' Helper function to load a Configurator instance from a user defined python module'''
    config_module =  path_import(cfg)
    try:
        config = config_module.cfg
        # Load the configuration
        if do_load:
            config.load()
        if save_config and outputdir is not None:
            config.save_config(outputdir)
    except AttributeError as e:
        print("Error: ", e)
        raise Exception("The provided configuration module does not contain a `cfg` attribute of type Configurator. Please check your configuration!")

    if not isinstance(config, Configurator):
        raise Exception("The configuration module attribute `cfg` is not of type Configurator. Please check yuor configuration!")
    return config

def adapt_chunksize(nevents, run_options):
    '''Helper function to adjust the chunksize so that each worker has at least a chunk to process.
    If the number of available workers exceeds the maximum number of workers for a given dataset,
    the chunksize is reduced so that all the available workers are used to process the given dataset.'''
    n_workers_max = nevents / run_options["chunksize"]
    if (run_options["scaleout"] > n_workers_max):
        adapted_chunksize = int(nevents / run_options["scaleout"])
    else:
        adapted_chunksize = run_options["chunksize"]
    return adapted_chunksize

# Function taken from HiggsDNA
# https://gitlab.cern.ch/HiggsDNA-project/HiggsDNA/-/blob/master/higgs_dna/utils/dumping_utils.py
def dump_ak_array(
    akarr: awkward.Array,
    fname: str,
    location: str,
    subdirs: Optional[List[str]] = None,
) -> None:
    """
    Dump an awkward array to disk at location/'/'.join(subdirs)/fname.
    """
    subdirs = subdirs or []
    xrd_prefix = "root://"
    pfx_len = len(xrd_prefix)
    xrootd = False
    if xrd_prefix in location:
        try:
            import XRootD  # type: ignore
            import XRootD.client  # type: ignore

            xrootd = True
        except ImportError as err:
            raise ImportError(
                "Install XRootD python bindings with: conda install -c conda-forge xroot"
            ) from err
    local_file = (
        os.path.abspath(os.path.join(".", fname))
        if xrootd
        else os.path.join(".", fname)
    )
    merged_subdirs = "/".join(subdirs) if xrootd else os.path.sep.join(subdirs)
    destination = (
        location + merged_subdirs + f"/{fname}"
        if xrootd
        else os.path.join(location, os.path.join(merged_subdirs, fname))
    )
    awkward.to_parquet(akarr, local_file)
    if xrootd: # fix XRootD mkdir offen meet problem like: Exception: [ERROR] Server responded with an error: [3018] Unable to mkdir ...（your dir）..; File exists
        copyproc = XRootD.client.CopyProcess()
        #old: copyproc.add_job(local_file, destination, force=True)
        copyproc.add_job(local_file, destination, mkdir=True, force=True) 
        copyproc.prepare()
        status, response = copyproc.run()
        if status.status != 0:
            raise Exception(status.message)
        del copyproc
    else:
        dirname = os.path.dirname(destination)
        pathlib.Path(dirname).mkdir(parents=True, exist_ok=True)
        shutil.copy(local_file, destination)
        assert os.path.isfile(destination)
    pathlib.Path(local_file).unlink()


def get_nano_version(events, params, year):
    '''Helper function to get the nano version from the events metadata or from the default parameters.'''
    if "nano_version" in events.metadata:
        nano_version = events.metadata["nano_version"]
    else:
        if events.metadata.get("isMC", False):
            # Try to extract from the sample name
            if "NanoAODv12" in events.metadata["filename"]:
                nano_version = 12
            elif "NanoAODv15" in events.metadata["filename"]:
                nano_version = 15
            else:
                # For MC if it's not defined we take the default nano version
                nano_version = params["default_nano_version"][year]
        else:
            # For data if it's not defined we take the default nano version
            nano_version = params["default_nano_version"][year]

    return nano_version


@njit
def replace_at_indices(a, indices, a_corrected, array_builder):
    """
    Replace elements of array `a` at positions specified by `indices` 
    with values from `a_corrected`.
    `indices` is a jagged array where each sub-array contains the indices
    to be replaced for the corresponding sub-array in `a`.
    `a_corrected` is a jagged array with the same shape as `indices`,
    containing the new values to insert.
    The shape of `a` is different from that of `indices` and `a_corrected`,
    but they share the same outer dimension.
    
    Parameters:
    -----------
    a : array
        Original array to be modified
    indices : array
        Indices where replacements should occur
    a_corrected : array
        Corrected values to insert (same shape as indices)
    array_builder : ak.ArrayBuilder, optional
        ArrayBuilder to use. If None, a new one is created.
    
    Returns:
    --------
    array : Modified copy of `a`
    """
    
    # Create a new ArrayBuilder for each call to avoid accumulation
    if array_builder is None:
        raise ValueError("array_builder must be provided and cannot be None.")
    
    # Replace values at specified indices
    for ievt, values in enumerate(a):
        array_values = []
        for i in range(len(values)):
            if i in indices[ievt]:
                # Find the position of i in indices[ievt]
                idx_pos = -1
                for j in range(len(indices[ievt])):
                    if indices[ievt][j] == i:
                        idx_pos = j
                        break
                array_values.append(a_corrected[ievt][idx_pos])
            else:
                array_values.append(values[i])
        array_builder.begin_list()
        for val in array_values:
            array_builder.append(val)
        array_builder.end_list()

    return array_builder

def save_failed_jobs(failed_jobs_list, outputdir):
    """
    Save the list of failed job names to a JSON file.
    
    Parameters
    ----------
    failed_jobs_list : list of str
        List of dataset or group names that failed processing
    outputdir : str
        Output directory where the failed_jobs.json file will be saved
    """
    failed_jobs_file = os.path.join(outputdir, FAILED_JOBS_FILENAME)
    try:
        with open(failed_jobs_file, 'w') as f:
            json.dump(failed_jobs_list, f, indent=2)
        logging.info(f"Failed jobs saved to {failed_jobs_file}")
    except (IOError, OSError) as e:
        logging.error(f"Failed to save failed jobs to {failed_jobs_file}: {e}")
        raise

def load_failed_jobs(outputdir):
    """
    Load the list of failed job names from a JSON file.
    
    Parameters
    ----------
    outputdir : str
        Output directory where the failed_jobs.json file is located
        
    Returns
    -------
    list of str or None
        List of dataset or group names that failed processing, or None if file doesn't exist
    """
    failed_jobs_file = os.path.join(outputdir, FAILED_JOBS_FILENAME)
    if not os.path.exists(failed_jobs_file):
        return None
    
    try:
        with open(failed_jobs_file, 'r') as f:
            failed_jobs_list = json.load(f)
        logging.info(f"Loaded {len(failed_jobs_list)} failed jobs from {failed_jobs_file}")
        return failed_jobs_list
    except (IOError, OSError) as e:
        logging.error(f"Failed to read failed jobs file {failed_jobs_file}: {e}")
        raise
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse failed jobs file {failed_jobs_file}: {e}")
        raise
