from omegaconf import OmegaConf
import os
from pathlib import Path
from typing import List

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


# Resolver for PocketCoffea default parameters file location
# By using the resolved ${default_params_dir:}/file.yaml the file
# is loaded from the default params folder of the PocketCoffea package
register_configuration_dir("default_params_dir", os.path.dirname(os.path.abspath(__file__)))
OmegaConf.register_new_resolver("pico_to_femto", lambda x: float(x)/1000.)

def setup_cvmfs_resolver(groups_tags: dict = None):
    """
    Setup the CVMFS path resolver to point to the correct version of the POGs files
    If groups_tags is None the latest version is used. Otherwise a dictionary with the group names
    and the corresponding tags for each period must be provided.
    """
    basepath = Path("/cvmfs/cms-griddata.cern.ch/cat/metadata/")
    valid_groups = [ n.name for n in basepath.iterdir() if n.is_dir()]
    pogpath = Path("/cvmfs/cms-griddata.cern.ch/cat/metadata/EGM")
    # All the groups must share the same valid periods
    valid_periods = [ n.name for n in pogpath.iterdir() if n.is_dir()]
    
    # Register the resolver
    def cvmfs_path_resolver(period: str, group: str, file: str, tag=None) -> str:
        '''
        Resolver to get the cvmfs path for a given group, period and file.
        If a tag is provided, it is used to get the specific version of the file.
        Otherwise the group_tags dictionary is used to get the tag for the group.
        If the group is not in the group_tags dictionary, the latest version is used.
        The group_tags dictionary must have the following structure:
        {
            "EGM": {"period1": "tag1",
                    "period2": "tag2" },
            "BTV": {"period1": "tag3",
                    "period2": "tag4"},
            ...
        }
        '''
        if group not in valid_groups:
            raise ValueError(f"Invalid group '{group}' for period '{period}' file '{file}'. Valid groups are: {valid_groups}")
        if period not in valid_periods:
            raise ValueError(f"Invalid period '{period}' for group '{group}' file '{file}'. Valid periods are: {valid_periods}")
        
        if tag is not None:
            tag = tag
        elif groups_tags is not None and group in groups_tags and period in groups_tags[group]:
            tag = groups_tags[group][period]
        else:
            tag = "latest"
       
        filepath = f"/cvmfs/cms-griddata.cern.ch/cat/metadata/{group}/{period}/{tag}/{file}"
        # Check if the file exists
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File '{filepath}' not found on CVMFS.")
        return filepath
    
    # Register the resolver with OmegaConf
    OmegaConf.register_new_resolver("cvmfs", cvmfs_path_resolver, replace=True)


##############################################
def get_default_parameters(group_tags: dict = None) -> OmegaConf:
    '''
    This function loads the default parameters from the PocketCoffea package for
    - pileup files
    - event flags
    - lumi
    - jet_scale_factors: btagSF, jetPuID
    - btagging
    - lepton_scale_factors
    - MET_xy corrections.

    The use can use this function to get a basic set of parameters to customize
    in each analysis.
    '''
    # The default configs are part of the package
    basedir = os.path.dirname(__file__)

    # Loading the cvmfs resolver
    setup_cvmfs_resolver(group_tags)
    nano_version = OmegaConf.load(os.path.join(basedir, "nano_version.yaml"))
    pileup = OmegaConf.load(os.path.join(basedir, 'pileup.yaml'))
    event_flags = OmegaConf.load(os.path.join(basedir, 'event_flags.yaml'))
    lumi = OmegaConf.load(os.path.join(basedir, 'lumi.yaml'))
    #xsec = OmegaConf.load(os.path.join(basedir, 'xsec.yaml'))
    jets_calibration = OmegaConf.load(os.path.join(basedir, "jets_calibration.yaml"))
    jet_scale_factors = OmegaConf.load(os.path.join(basedir, 'jet_scale_factors.yaml'))
    btagging = OmegaConf.load(os.path.join(basedir, "btagging.yaml"))
    lepton_scale_factors = OmegaConf.load(
        os.path.join(basedir, 'lepton_scale_factors.yaml')
    )
    photon_sf = OmegaConf.load(os.path.join(basedir, 'photon_scale_factors.yaml'))
    met_xy = OmegaConf.load(os.path.join(basedir, "met_xy.yaml"))
    syst_variations = OmegaConf.load(os.path.join(basedir, 'variations.yaml'))
    plotting_style = OmegaConf.load(os.path.join(basedir, 'plotting_style.yaml'))

    all = OmegaConf.merge(
        nano_version,
        pileup,
        event_flags,
        lumi,
        jets_calibration,
        jet_scale_factors,
        btagging,
        lepton_scale_factors,
        photon_sf,
        met_xy,
        syst_variations,
        plotting_style
    )
    # resolve the config to catch problems
    OmegaConf.resolve(all)
    return all

def get_default_run_options():
    basedir = os.path.dirname(__file__)
    run_options = OmegaConf.load(os.path.join(basedir, "executor_options_defaults.yaml"))
    return run_options

def get_defaults_and_compose(*files: List[str]):
    default_params = get_default_parameters()
    return merge_parameters_from_files(default_params, *files)


def merge_parameters(main_config: OmegaConf, *configs: List[OmegaConf], update=True):
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


def merge_parameters_from_files(conf: OmegaConf, *kargs: List[str], update=True):
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


def merge_parameters_from_string(conf: OmegaConf, *kargs: List, update=True):
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


def compose_parameters_from_files(*files: List[str], update=True):
    '''
    Helper functions which loads separately the parameters
    in all the files and then merge then in order.
    '''
    # create OmegaConfs in order and merge them
    confs = [OmegaConf.load(f) for f in files]
    return merge_parameters(confs[0], *confs[1:], merge=update)


def dump_parameters(conf: OmegaConf, outfile: str, overwrite=False):
    if os.path.exists(outfile) and not overwrite:
        raise Exception(f"Trying to overwrite the file {outfile} with overwrite=False")

    with open(outfile, "w") as f:
        f.write(OmegaConf.to_yaml(conf))
