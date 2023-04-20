'''
Nice code to build the JEC/JER and JES uncertainties taken from
https://github.com/andrzejnovak/boostedhiggs/blob/master/boostedhiggs/build_jec.py
'''

import importlib.resources
import contextlib
from coffea.lookup_tools import extractor
from coffea.jetmet_tools import JECStack, CorrectedJetsFactory, CorrectedMETFactory
import gzip
# jme stuff not pickleable in coffea
import cloudpickle

def _jet_factory_factory(files,jec_name_map):
    ext = extractor()
    ext.add_weight_sets([f"* * {file}" for file in files])
    ext.finalize()
    jec_stack = JECStack(ext.make_evaluator())
    return CorrectedJetsFactory(jec_name_map, jec_stack)


def build(params):
    factories = {"jet_ak4":{}, "jet_ak8": {}}
    for era, files in params.jet_ak4.items():
        print(f"Creating AK4 jet calibrator: {era}")
        factories["jet_ak4"][era] = _jet_factory_factory(files, params.jec_name_map)

    for era, files in params.jet_ak8.items():
        print(f"Creating AK8 jet calibrator: {era}")
        factories["jet_ak8"][era] = _jet_factory_factory(files, params.jec_name_map)

    with gzip.open(params.file, "wb") as fout:
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
    for label in params.default_jets_calibration.factory_configuration.jet_ak4:
        print(f"Preparing configurator label: {label}")
        conf = {
            "file": f"{params.default_jets_calibration.factory_files_dir}/jets_calibrator_{label}.pkl.gz",
            "jet_ak4": params.default_jets_calibration.factory_configuration.jet_ak4[label],
            "jet_ak8": params.default_jets_calibration.factory_configuration.jet_ak8[label],
            "jec_name_map": params.default_jets_calibration.jec_name_map
            }
        build(OmegaConf.create(conf))
    
