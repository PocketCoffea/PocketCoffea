'''
Nice code to build the JEC/JER and JES uncertainties taken from
https://github.com/andrzejnovak/boostedhiggs/blob/master/boostedhiggs/build_jec.py
'''

import importlib.resources
import contextlib
from coffea.lookup_tools import extractor
from coffea.jetmet_tools import JECStack, CorrectedJetsFactory, CorrectedMETFactory
import gzip
import os
import cloudpickle


def _jet_factory_factory(files,jec_name_map):
    ext = extractor()
    ext.add_weight_sets([f"* * {file}" for file in files])
    ext.finalize()
    jec_stack = JECStack(ext.make_evaluator())
    return CorrectedJetsFactory(jec_name_map, jec_stack)


def build(params):
    '''
    Build the factory objects from the list of JEC files for each era
    for ak4 and ak8 jets and same them on disk in cloudpikle format
    '''
    factories = {}
    for jet_type, eras in params.jet_types.items():
        factories[jet_type] = {}
        for era, files in eras.items():
            print(f"Creating {jet_type} jet calibrator for {era}")
            factories[jet_type][era] = _jet_factory_factory(files, params.jec_name_map)

    os.makedirs(os.path.dirname(os.path.abspath(params.factory_file)), exist_ok=True)
    with gzip.open(params.factory_file, "wb") as fout:
        cloudpickle.dump(factories, fout)



if __name__ == "__main__":
    from omegaconf import OmegaConf
    from pocket_coffea.parameters import defaults  # to define the macros for the config
    import sys
    if len(sys.argv) < 2:
        raise Exception(
            "The script requires at least one argument for the params file."
        )

    params = OmegaConf.load(sys.argv[1])
    if 'jets_calibration' not in params or 'default_jets_calibration' not in params:
        raise Expection("The provided configuration file does not contain the jey 'jets_calibration' or 'default_jets_calibration' defaults")

    #building all the configurations
    jet_types = list(params.default_jets_calibration.factory_configuration.keys())
    print("Available jet types ",jet_types )

    # We assume to have the same set of labels for all the type of jets
    # this assumption is used only to create a default set of factory files.
    # The user can mix the calibration labels/types for each jet type in the specific config
    for label in params.default_jets_calibration.factory_configuration[jet_types[0]]:
        print(f"Preparing configurator label: {label}")
        conf = {
            "factory_file": f"{params.default_jets_calibration.factory_files_dir}/jets_calibrator_{label}.pkl.gz",
            "jet_types": {
                k: params.default_jets_calibration.factory_configuration[k][label] for k in jet_types
            },
            "jec_name_map": params.default_jets_calibration.jec_name_map
            }
        build(OmegaConf.create(conf))
    
