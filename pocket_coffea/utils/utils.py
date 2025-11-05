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
    if xrootd:
        copyproc = XRootD.client.CopyProcess()
        copyproc.add_job(local_file, destination, force=True)
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
        if events.metadata["isMC"]:
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