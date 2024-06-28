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


def build(params, filter_years=None):
    '''
    Build the factory objects from the list of JEC files for each era
    for ak4 and ak8 jets and same them on disk in cloudpikle format
    '''
    factories = {"MC": {}, "Data": {}}
    for jet_type, years in params.jet_types["MC"].items():
        factories["MC"][jet_type] = {}
        for year, files in years.items():
            if filter_years and year not in filter_years:
                continue
            print(f"Creating {jet_type} jet calibrator for {year} (MC)")
            factories["MC"][jet_type][year] = _jet_factory_factory(files, params.jec_name_map_MC)
    # For data the corrections are by ERA
    for jet_type, years in params.jet_types["Data"].items():
        factories["Data"][jet_type] = {}
        for year, eras in years.items():
            if filter_years and year not in filter_years:
                continue
            factories["Data"][jet_type][year] = {}
            for era, files in eras.items(): 
                print(f"Creating {jet_type} jet calibrator for {year} in era: {era} (DATA)")
                factories["Data"][jet_type][year][era] = _jet_factory_factory(files, params.jec_name_map_Data)

            
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
        raise Expection("The provided configuration file does not contain the key 'jets_calibration' or 'default_jets_calibration' defaults")

    #building all the configurations
    jet_types_MC = list(params.default_jets_calibration.factory_configuration_MC.keys())
    jet_types_Data = list(params.default_jets_calibration.factory_configuration_Data.keys())
    print("Available jet types MC ",jet_types_MC )
    print("Available jet types Data ",jet_types_Data )

    # We assume to have the same set of labels for all the type of jets
    # this assumption is used only to create a default set of factory files.
    # The user can mix the calibration labels/types for each jet type in the specific config
    for label in params.default_jets_calibration.factory_configuration_MC[jet_types[0]]:
        print(f"Preparing configurator MC: {label}")
        conf = {
            "factory_file": f"{params.default_jets_calibration.factory_files_dir}/jets_calibrator_{label}.pkl.gz",
            "jet_types": {
                k: params.default_jets_calibration.factory_configuration_MC[k][label] for k in jet_types
            },
            "jec_name_map": params.default_jets_calibration.jec_name_map
            }
        build(OmegaConf.create(conf))

    for label in params.default_jets_calibration.factory_configuration_Data[jet_types[0]]:
        print(f"Preparing configurator Data: {label}")
        conf = {
            "factory_file": f"{params.default_jets_calibration.factory_files_dir}/jets_calibrator_{label}.pkl.gz",
            "jet_types": {
                k: params.default_jets_calibration.factory_configuration_Data[k][label] for k in jet_types
            },
            "jec_name_map": params.default_jets_calibration.jec_name_map
            }
        build(OmegaConf.create(conf))
    
