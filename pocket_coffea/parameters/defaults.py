from omegaconf import OmegaConf 
import os

#Resolver for PocketCoffea default parameters file location
# By using the resolved ${default_params_dir:file.yaml} the file
# is loaded from the default params folder of the PocketCoffea package
def get_default_params_dir(file=""):
    return os.path.join(os.path.dirname(__file__),
                     file)
OmegaConf.register_new_resolver("default_params_dir", get_default_params_dir, replace=True)



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
    
    pileup = OmegaConf.load(os.path.join(basedir,'pileup.yaml'))
    event_flags = OmegaConf.load(os.path.join(basedir,'event_flags.yaml'))
    lumi = OmegaConf.load(os.path.join(basedir,'lumi.yaml'))
    jet_scale_factors = OmegaConf.load(os.path.join(basedir,'jet_scale_factors.yaml'))
    btagging = OmegaConf.load(os.path.join(basedir, "btagging.yaml"))
    lepton_scale_factors = OmegaConf.load(os.path.join(basedir,'lepton_scale_factors.yaml'))
    
    all = OmegaConf.merge(pileup, event_flags, lumi,
                          jet_scale_factors, btagging,
                          lepton_scale_factors,
                          )
    return all 


def compose_parameters(*files: list[str]):
    '''
    Helper functions which loads separately the parameters
    in all the files and then merge then in order.
    '''
    # create OmegaConfs in order and merge them
    return OmegaConf.merge(*[OmegaConf.load(f) for f in files])

def merge_parameters_from_files(conf: OmegaConf, *kargs: list[str]):
    '''
    Helper function to merge a list yaml files with parameters to an
    existing OmegaConf object.

    The parameters files are loaded and merged in order.
    '''
    return conf.merge(*[OmegaConf.load(f) for f in files])


def merge_parameters(conf: OmegaConf, *kargs: list[str]):
    '''
    Helper function to merge a list of parameters to an
    existing OmegaConf object.
    Each string in the list is converted to a OmegaConf object before the merging.
    N.B: if you want to merge yaml files use `merge_parameters_from_files`.

    The parameters are loaded and merged in order.
    '''
    return conf.merge(*[OmegaConf.create(f) for f in files])


def dump_parameters(conf: OmegaConf, outfile: str, overwrite=False):
    if os.path.exists(outfile) and not overwrite:
        raise Exception(f"Trying to overwrite the file {outfile} with overwrite=False")

    with open(outfile, "w") as f:
        f.write(OmegaConf.to_yaml(conf))
        
