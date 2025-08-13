from omegaconf import OmegaConf
import os
from typing import List, Optional
import glob
from pathlib import Path

_current_tag = "2025-08-12"

def register_configuration_dir(key: str, directory: str):
    '''
    This function registers a new resolved in the OmegaConf library
    to prepend the correct path in a configuration.

    E.g.
    ```
    register_configuration_dir("config_dir","/test/analysis/config")
    # now this is possible in the configuration yaml
    ${config_dir:}/params/btagsf_calibration/test.yaml
    '''
    OmegaConf.register_new_resolver(key, lambda: directory, replace=True)


def _get_local_data_path(tag: Optional[str]) -> Optional[str]:
    """
    Get path to local data directory for a specific tag.
    
    Args:
        tag: Version tag
    
    Returns:
        Path to local data directory or None if not found.
    """
    basedir = os.path.dirname(__file__)
    
    # Check if we have local cvmfs dumps directory structure
    cvmfs_dumps_dir = os.path.join(basedir, "cvmfs_dumps")
    
    if not os.path.exists(cvmfs_dumps_dir):
        return None
    
    if tag:
        # Look for specific tag directory structure as created by download script
        # Format: cvmfs_dumps/<tag>/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration
        tagged_dir = os.path.join(cvmfs_dumps_dir, tag)
        if os.path.exists(tagged_dir):
            return tagged_dir
        else:
            raise ValueError(f"Request tag for cvmfs dumped file ({tag}) does not exist!")
    else:
        return None  # no tag, no output directory, different from requesting an non existing tag
    

def _list_available_local_tags() -> List[str]:
    """List all available local data tags."""
    basedir = os.path.dirname(__file__)
    cvmfs_dumps_dir = os.path.join(basedir, "cvmfs_dumps")
    
    if not os.path.exists(cvmfs_dumps_dir):
        return []
    tags = []
    try:
        for item in os.listdir(cvmfs_dumps_dir):
            tag_path = os.path.join(cvmfs_dumps_dir, item)
            if os.path.isdir(tag_path):
                # Check if this looks like a valid tag directory
                jsonpog_path = os.path.join(tag_path, "metadata.json")
                if os.path.exists(jsonpog_path):
                    tags.append(item)
    except OSError:
        pass
    
    return sorted(tags)


def setup_cvmfs_resolver(use_cvmfs_dump: bool, tag: Optional[str] = None):
    """
    Setup the CVMFS path resolver to use either local files or CVMFS.
    
    Args:
        use_cvmfs_dump: If True, use local files; if False, use CVMFS; if None, auto-detect
        tag: Version tag for local files (only used when use_cvmfs_dump=True)
    """
    global _current_tag
    
    _current_local_base_path = None
    # If a tag is requested replace the global default
    if tag:
        _current_tag = tag
    
    
    # Check for local data availability
    local_path = _get_local_data_path(_current_tag)
    
    # Determine final source
    if use_cvmfs_dump is True:
        if local_path:
            _current_local_base_path = local_path
            print(f"Using local CVMFS files from: {local_path}")
        else:
            print("Warning: Local files requested but not found, falling back to CVMFS")   
    
    
    # Register the resolver
    def cvmfs_path_resolver(relative_path: str) -> str:
        """
        Resolve CVMFS paths to either local or remote location.
        
        Args:
            relative_path: Path relative to jsonpog-integration directory
            
        Returns:
            Full path to the file
        """
        if _current_local_base_path:
            full_path = _current_local_base_path + relative_path
            # Verify file exists locally, fallback to CVMFS if not
            if os.path.exists(full_path):
                return full_path
            else:
                raise ValueError(f"Local cvmfs file not found: {full_path}")
        else:
            return "/cvmfs" + relative_path
    
    # Register the resolver with OmegaConf
    OmegaConf.register_new_resolver("cvmfs", cvmfs_path_resolver, replace=True)
    return _current_local_base_path


# Resolver for PocketCoffea default parameters file location
# By using the resolved ${default_params_dir:}/file.yaml the file
# is loaded from the default params folder of the PocketCoffea package
register_configuration_dir("default_params_dir", os.path.dirname(os.path.abspath(__file__)))
OmegaConf.register_new_resolver("pico_to_femto", lambda x: float(x)/1000.)

##############################################
def get_default_parameters(use_cvmfs_dump: bool = True, tag: Optional[str] = None):
    '''
    This function loads the default parameters from the PocketCoffea package for
    - pileup files
    - event flags
    - lumi
    - jet_scale_factors: btagSF, jetPuID
    - btagging
    - lepton_scale_factors
    - MET_xy corrections.

    Args:
        use_cvmfs_dump: If True, use local downloaded files; if False, use CVMFS (default True); 
        tag: Version tag for local files (only used if use_cvmfs_dump is not False)
        if tag is None the current default tag specified in defaults is used.

    The user can use this function to get a basic set of parameters to customize
    in each analysis.
    '''
    # Setup the CVMFS resolver based on parameters
    current_local_cvmfs_base_path = setup_cvmfs_resolver(use_cvmfs_dump, tag)

    # The default configs are part of the package
    basedir = os.path.dirname(__file__)
    pileup = OmegaConf.load(os.path.join(basedir, 'pileup.yaml'))
    event_flags = OmegaConf.load(os.path.join(basedir, 'event_flags.yaml'))
    lumi = OmegaConf.load(os.path.join(basedir, 'lumi.yaml'))
    #xsec = OmegaConf.load(os.path.join(basedir, 'xsec.yaml'))
    jet_calibration = OmegaConf.load(os.path.join(basedir, "jets_calibration.yaml"))
    jet_scale_factors = OmegaConf.load(os.path.join(basedir, 'jet_scale_factors.yaml'))
    btagging = OmegaConf.load(os.path.join(basedir, "btagging.yaml"))
    lepton_scale_factors = OmegaConf.load(
        os.path.join(basedir, 'lepton_scale_factors.yaml')
    )
    met_xy = OmegaConf.load(os.path.join(basedir, "met_xy.yaml"))
    syst_variations = OmegaConf.load(os.path.join(basedir, 'variations.yaml'))
    plotting_style = OmegaConf.load(os.path.join(basedir, 'plotting_style.yaml'))

    all = OmegaConf.merge(
        pileup,
        event_flags,
        lumi,
        jet_calibration,
        jet_scale_factors,
        btagging,
        lepton_scale_factors,
        met_xy,
        syst_variations,
        plotting_style
    )
    
    # Add source information
    source_info = {}
    source_info["source"] = "local" if current_local_cvmfs_base_path else "cvmfs"
    source_info["base_path"] = current_local_cvmfs_base_path or "/cvmfs"

    if _current_tag:
        source_info["tag"] = _current_tag
    
    # Add metadata from download session if available
    if current_local_cvmfs_base_path:
        metadata_file = os.path.join(current_local_cvmfs_base_path, "metadata.json")
        if os.path.exists(metadata_file):
            try:
                import json
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)
                
                # Include relevant metadata
                source_info["download_metadata"] = {
                    "download_date": metadata.get("download_date"),
                    "total_files": len(metadata.get("files", [])),
                    "summary": metadata.get("summary", {}),
                    "version_info": metadata.get("version_info", {})
                }
                
            except Exception as e:
                print(f"Warning: Could not read metadata file {metadata_file}: {e}")
    
    all.cvmfs_source_info = source_info

    # resolve the config to catch problems
    OmegaConf.resolve(all)
    return all

def get_default_run_options():
    basedir = os.path.dirname(__file__)
    run_options = OmegaConf.load(os.path.join(basedir, "executor_options_defaults.yaml"))
    return run_options


def list_available_local_tags() -> List[str]:
    """
    List all available local data tags.
    
    Returns:
        List of available version tags
    """
    return _list_available_local_tags()



def get_defaults_and_compose(*files: str):
    default_params = get_default_parameters()
    return merge_parameters_from_files(default_params, *files)


def merge_parameters(main_config: OmegaConf, *configs: OmegaConf, update=True):
    '''
    Utility function to update a config with a list of other configs.
    `update=True` means that if the key is already present
    elements are added to dictionaries and keys, and they are not just replaced.

    This is needed because OmegaConf.merge does not allow just updating the content, but
    it always replaces the keys
    '''
    # create a deepcopy of the initial conf instead of modifying it
    main = OmegaConf.masked_copy(main_config, main_config.keys())
    for c in configs:
        for key in c.keys():
            OmegaConf.update(main, key, c[key], merge=update)
    return main


def merge_parameters_from_files(conf: OmegaConf, *kargs: str, update=True):
    '''
    Helper function to merge a list yaml files with parameters to an
    existing OmegaConf object.

    The parameters files are loaded and merged in order.
    If `update=True` the configurations are added on top of each other merging
    existing dictionaries and lists. If `update=False` pre-existing keys are
    re-assigned to the new dictionary or list present in the merged configurations.

    - If you want to overwrite values without redefining completely a dictionary use update=True,
    - If you want to completely redefine a subkey use update=False
    '''
    return merge_parameters(conf, *[OmegaConf.load(f) for f in kargs], update=update)


def merge_parameters_from_string(conf: OmegaConf, *kargs, update=True):
    '''
    Helper function to merge a list of parameters to an
    existing OmegaConf object.
    Each element in the list is converted to a OmegaConf object before the merging.
    In this way one can use a list of dot-string to define a config.

    N.B: if you want to merge yaml files use `merge_parameters_from_files`.

    The parameters are loaded and merged in order.
     If `update=True` the configurations are added on top of each other merging
    existing dictionaries and lists. If `update=False` pre-existing keys are
    re-assigned to the new dictionary or list present in the merged configurations.

    - If you want to overwrite values without redefining completely a dictionary use update=True,
    - If you want to completely redefine a subkey use update=False
    '''
    return merge_parameters(conf, *[OmegaConf.create(f) for f in kargs], update=update)


def compose_parameters_from_files(*files: str, update=True):
    '''
    Helper functions which loads separately the parameters
    in all the files and then merge then in order.
    '''
    # create OmegaConfs in order and merge them
    confs = [OmegaConf.load(f) for f in files]
    return merge_parameters(confs[0], *confs[1:], update=update)


def dump_parameters(conf: OmegaConf, outfile: str, overwrite=False):
    if os.path.exists(outfile) and not overwrite:
        raise Exception(f"Trying to overwrite the file {outfile} with overwrite=False")

    with open(outfile, "w") as f:
        f.write(OmegaConf.to_yaml(conf))
