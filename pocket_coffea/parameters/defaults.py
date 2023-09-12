from omegaconf import OmegaConf
import os
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

##############################################
def get_default_parameters():
    '''
    This function loads the default parameters from the PocketCoffea package for
    - pileup files
    - event flags
    - lumi
    - jet_scale_factors: btagSF, jetPuID
    - btagging
    - lepton_scale_factors.

    The use can use this function to get a basic set of parameters to customize
    in each analysis.
    '''
    # The default configs are part of the package
    basedir = os.path.dirname(__file__)
    pileup = OmegaConf.load(os.path.join(basedir, 'pileup.yaml'))
    event_flags = OmegaConf.load(os.path.join(basedir, 'event_flags.yaml'))
    lumi = OmegaConf.load(os.path.join(basedir, 'lumi.yaml'))
    jet_calibration = OmegaConf.load(os.path.join(basedir, "jets_calibration.yaml"))
    jet_scale_factors = OmegaConf.load(os.path.join(basedir, 'jet_scale_factors.yaml'))
    btagging = OmegaConf.load(os.path.join(basedir, "btagging.yaml"))
    lepton_scale_factors = OmegaConf.load(
        os.path.join(basedir, 'lepton_scale_factors.yaml')
    )
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
        syst_variations,
        plotting_style
    )
    return all


def get_defaults_and_compose(*files: List[str]):
    default_params = get_default_parameters()
    return merge_parameters_from_files(default_params, files)


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
