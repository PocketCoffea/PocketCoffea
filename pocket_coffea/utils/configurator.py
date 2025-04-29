import os
import sys
import json
from copy import deepcopy
from pprint import pprint, pformat
import cloudpickle
from collections import defaultdict
import inspect
import logging
from omegaconf import OmegaConf
from warnings import warn

from ..lib.cut_definition import Cut
from ..lib.categorization import StandardSelection, CartesianSelection
from ..parameters.cuts import passthrough
from ..lib.hist_manager import Axis, HistConf
from ..utils import build_jets_calibrator

from pprint import PrettyPrinter

def format(data, indent=0, width=80, depth=None, compact=True, sort_dicts=True):
    pp = PrettyPrinter(indent=indent, width=width, depth=depth, compact=compact, sort_dicts=sort_dicts)
    return pp.pformat(data)


class Configurator:
    '''
    Main class driving the configuration of a PocketCoffea analysis.
    The Configurator groups the several aspects that define an analysis run:
    - skims, preselections, categorization
    - output: variables and columns
    - datasets
    - weights and variations and the objects proving them
    - workflow
    - analysis parameters

    The running environment configuration is not part of the Configurator class.

    The available Weights are taken from the list of weights classes passed to the Configurator.
    '''

    def __init__(
        self,
        workflow,
        parameters,
        datasets,
        skim,
        preselections,
        categories,
        weights,
        variations,
        variables,
        weights_classes=None,    
        columns=None,
        workflow_options=None,
        save_skimmed_files=None,
        do_postprocessing=True,
    ):
        '''
        Constructur of the Configurator class.
        It saves the configuration of the analysis and the workflow to be used to load
        the internal objects in the load() method.

        Args:
        - workflow: the workflow class to be used in the analysis
        - parameters: the OmegaConf object with the parameters of the analysis
        - datasets: the dictionary describing the datasets to be used in the analysis
        - skim: the list of cuts to be applied in the skimming step
        - preselections: the list of preselections to apply
        - categories: the dictionary of categories to be used in the analysis
        - weights: the dictionary of weights to be used in the analysis
        - variations: the dictionary of variations to be used in the analysis
        - variables: the dictionary of variables to be used in the analysis
        - weights_classes: the list of WeightWrapper classes to be used in the analysis
        - columns: the dictionary of columns to be used in the analysis
        - workflow_options: the dictionary of options to be passed to the workflow
        - save_skimmed_files:  if !=None and str, it is used to save the skimmed files in the specified folder
        - do_postprocessing: if False the postprocessing step is skipped      
        '''

        # Save the workflow object and its options
        self.workflow = workflow
        self.workflow_options = workflow_options if workflow_options != None else {}
        self.parameters = parameters
        # Resolving the OmegaConf
        try:
            OmegaConf.resolve(self.parameters)
        except Exception as e:
            print("Error during resolution of OmegaConf parameters magic, please check your parameters files.")
            raise(e)
        
        self.save_skimmed_files = save_skimmed_files != None
        self.save_skimmed_files_folder = save_skimmed_files
        self.do_postprocessing = do_postprocessing
        # Save
        # Load dataset
        self.datasets_cfg = datasets
        # The following attributes are loaded by load_datasets
        self.filesets = {}
        self.datasets = []
        self.samples = []
        self.samples_metadata = {}

        self.subsamples = {}
        self.subsamples_list = []  # List of subsamples (for internal checks)
        self.subsamples_reversed_map = {}  # Map of subsample: sample
        self.total_samples_list = []  # List of subsamples and inclusive samples names
        self.has_subsamples = {}

        self.years = []
        self.eras = []

        # Load histogram settings
        # No manipulation is needed, maybe some check can be added
        self.variables = variables

        self.skim_cfg = skim
        self.preselections_cfg = preselections
        self.categories_cfg = categories

        self.skim = []
        self.preselections = []

        ## Weights configuration
        self.weights_cfg = weights
        self.weights_classes = weights_classes
     
        self.variations_cfg = variations
       
        # Column accumulator config
        self.columns = {}
        self.columns_cfg = columns

        self.filesets_loaded = False
        self.loaded = False

    def load(self):
        '''This function loads the configuration for samples/weights/variations and creates
        the necessary objects for the processor to use. It also loads the workflow'''
        if not self.filesets_loaded:
            # Avoid reloading the datasets if the configurator already loaded them manually.
            # This happens when the configurator is manipulated to restrict the fileset before pickling (condor submission)
            self.load_datasets()

        # Now loading and storing the metadata of the filtered filesets
        if len(self.filesets) == 0:
            print("File set is empty: please check you dataset definition...")
            raise Exception("Wrong filesets configuration")
        else:
            for name, d in self.filesets.items():
                m = d["metadata"]
                if name not in self.datasets:
                    self.datasets.append(name)
                if m["sample"] not in self.samples:
                    self.samples.append(m["sample"])
                if m["year"] not in self.years:
                    self.years.append(m["year"])
                if 'era' in m.keys():
                    if (m["era"]) not in self.eras:
                        self.eras.append(m["era"])
                self.samples_metadata[m["sample"]] = {
                    "isMC": m["isMC"] =="True",
                }
            
        self.load_subsamples()

        # Categories: object handling categorization
        # - StandardSelection
        # - CartesianSelection
        ## Call the function which transforms the dictionary in the cfg
        # in the objects needed in the processors
        self.load_cuts_and_categories(self.skim_cfg, self.preselections_cfg, self.categories_cfg)

        self.weights_config = {
            s: {
                "inclusive": [],
                "bycategory": {c: [] for c in self.categories.keys()},
                "is_split_bycat": False,
            }
            for s in self.samples
        }
        ## Defining the available weight strings
        ## If the users hasn't passed a list of WeightWrapper classes, the configurator
        # loads the common_weight one and emits a Warning
        if self.weights_classes == None:
            print("WARNING: No weights classes passed to the configurator, using the default ones")
            from pocket_coffea.lib.weights.common import common
            self.weights_classes = common.common_weights
            # Emitting a warning
            warn("No weights classes passed to the configurator with `weight_classes=[]`, using the ones defined in `lib.weights.common.common`. ",
                 DeprecationWarning, stacklevel=2)
        
        # The list of strings is taken from the names of the list of weights classes
        # passed to the configurator.
        self.available_weights = {}
        self.requested_weights = []
        for w in self.weights_classes:
            self.available_weights[w.name] = w
         
        self.load_weights_config(self.weights_cfg)
        # keeping a unique list of requested weight to load
        self.requested_weights = list(set(self.requested_weights))
        self.weights_classes = list(filter(lambda x: x.name in self.requested_weights, self.weights_classes))
        
        ## Variations configuration
        # The structure is very similar to the weights one,
        # but the common and inclusive collections are fully flattened on a
        # sample:category structure
        self.variations_config = {
            s: {
                "weights": {c: [] for c in self.categories.keys()},
                "shape": {c: [] for c in self.categories.keys()},
            }
            for s in self.samples
            if self.samples_metadata[s]["isMC"]
        }
        
        if "shape" not in self.variations_cfg:
            self.variations_cfg["shape"] = {"common": {"inclusive": []}}

        self.load_variations_config(self.variations_cfg["weights"], variation_type="weights")
        self.load_variations_config(self.variations_cfg["shape"], variation_type="shape")
            
        # Collecting overall list of available weights and shape variations per sample
        self.available_weights_variations = {s: ["nominal"] for s in self.samples}
        self.available_shape_variations = {s: [] for s in self.samples}
        for sample in self.samples:
            # skipping variations for data
            if not self.samples_metadata[sample]["isMC"]:
                continue
            # Weights variations
            for cat, vars in self.variations_config[sample]["weights"].items():
                self.available_weights_variations[sample] += vars
            # Shape variations
            for cat, vars in self.variations_config[sample]["shape"].items():
                self.available_shape_variations[sample] += vars
            # make them unique
            self.available_weights_variations[sample] = list(
                set(self.available_weights_variations[sample])
            )
            self.available_shape_variations[sample] = list(
                set(self.available_shape_variations[sample])
            ) 
            
        # Columns configuration
        self.load_columns_config(self.columns_cfg)

        self.perform_checks()

        # Alway run the jet calibration builder
        if not os.path.exists(self.parameters.jets_calibration.factory_file):
            build_jets_calibrator.build(self.parameters.jets_calibration)

        # Load the workflow as the last thing
        self.load_workflow()

        # Mark the configurator as loaded
        self.loaded = True
        

    def load_datasets(self):
        for json_dataset in self.datasets_cfg["jsons"]:
            ds_dict = json.load(open(json_dataset))
            ds_filter = self.datasets_cfg.get("filter", None)
            if ds_filter != None:
                for key, ds in ds_dict.items():
                    pass_filter = True
                    if "samples" in ds_filter:
                        if ds["metadata"]["sample"] not in ds_filter["samples"]:
                            pass_filter = False
                    if "samples_exclude" in ds_filter:
                        if ds["metadata"]["sample"] in ds_filter["samples_exclude"]:
                            pass_filter = False
                    if "year" in ds_filter:
                        if ds["metadata"]["year"] not in ds_filter["year"]:
                            pass_filter = False
                    if pass_filter:
                        self.filesets[key] = ds
            else:
                self.filesets.update(ds_dict)

    def set_filesets_manually(self, filesets):
        '''This function sets the filesets directly, usually before the configuration is loaded.
        This is useful to pickle an unloaded version of the configuration restricting the filesets a priori.
        It is used in the condor submission script.
        The `filesets_loaded` attribute is set to True to avoid reloading the datasets.'''
        self.filesets = filesets
        self.filesets_loaded = True

    def load_subsamples(self):
        # subsamples configuration
        subsamples_dict = self.datasets_cfg.get("subsamples", {})
        # Save list of subsamples for each sample
        for sample in self.samples:
            if sample in subsamples_dict.keys():
                # build the subsample name as sample_subsample
                self.subsamples_list += [ f"{sample}__{subsam}" for subsam in subsamples_dict[sample].keys()]
                self.has_subsamples[sample] = True
            else:
                self.has_subsamples[sample] = False

        # Complete list of samples and subsamples
        self.total_samples_list = list(sorted(set(self.samples + self.subsamples_list)))
        self.subsamples_list = list(sorted(set(self.subsamples_list)))
        
        # Now saving the subsamples definition cuts
        for sample in self.samples:
            if sample in subsamples_dict:
                subscfg = subsamples_dict[sample]
                self.subsamples_reversed_map.update({f"{sample}__{subsam}": sample for subsam in subscfg.keys()})
                    
                if isinstance(subscfg, dict):
                    # Convert it to StandardSelection
                    self.subsamples[sample] = StandardSelection(subscfg)
                elif isinstance(subscfg, StandardSelection):
                    self.subsamples[sample] = subscfg
                elif isinstance(subscfg, CartesianSelection):
                    self.subsamples[sample] = subscfg
            else:
                # if there is no configured subsample, the full sample becomes its subsample
                # this is useful in the processor to have always a subsample
                # the name is == the name of the sample
                self.subsamples[sample] = StandardSelection({sample: [passthrough]})
                self.subsamples_reversed_map.update({sample: sample})

    def load_cuts_and_categories(self, skim: list, preselections: list, categories):
        '''This function loads the list of cuts and groups them in categories.
        Each cut is identified by a unique id (see Cut class definition)'''
        # If the skim, preselection and categories list are empty, append a `passthrough` Cut

        if len(skim) == 0:
            skim.append(passthrough)
        if len(preselections) == 0:
            preselections.append(passthrough)

        if categories == {}:
            categories["baseline"] = [passthrough]

        for sk in skim:
            if not isinstance(sk, Cut):
                print("Please define skim, preselections and cuts as Cut objects")
                raise Exception("Wrong categories/cuts configuration")
            self.skim.append(sk)

        for pres in preselections:
            if not isinstance(pres, Cut):
                print("Please define skim, preselections and cuts as Cut objects")
                raise Exception("Wrong categories/cuts configuration")
            self.preselections.append(pres)

        # Now saving the categories
        if isinstance(categories, dict):
            # Convert it to StandardSelection
            self.categories = StandardSelection(categories)
        elif isinstance(categories, StandardSelection):
            self.categories = categories
        elif isinstance(categories, CartesianSelection):
            self.categories = categories

    def load_weights_config(self, wcfg):
        '''This function loads the weights definition and prepares a list of
        weights to be applied for each sample and category'''
        # Read the config and save the list of weights names for each sample (and category if needed)
        if "common" not in wcfg:
            print("Weights configuration error: missing 'common' weights key")
            raise Exception("Wrong weight configuration")
        # common/inclusive weights
        for w in wcfg["common"]["inclusive"]:
            if w not in self.available_weights:
                print(f"Weight {w} not available in the configuration. Did you add it in the weights_classes?")
                raise Exception("Wrong weight configuration")
            self.requested_weights.append(w)
            # do now check if the weights is not string but custom
            for sample, wsample in self.weights_config.items():
                # add the weight to all the categories and samples
                if not self.samples_metadata[sample]["isMC"] and self.available_weights[w].isMC_only:
                    # Do not add in the data weights configuration if the weight is MC only
                    continue
                wsample["inclusive"].append(w)

        if "bycategory" in wcfg["common"]:
            for cat, weights in wcfg["common"]["bycategory"].items():
                for w in weights:
                    if w not in self.available_weights:
                        print(f"Weight {w} not available in the configuration. Did you add it in the weights_classes?")
                        raise Exception("Wrong weight configuration")
                    self.requested_weights.append(w)
                    for  sample, wsample in self.weights_config.items():
                        if not self.samples_metadata[sample]["isMC"] and self.available_weights[w].isMC_only:
                            # Do not add in the data weights configuration if the weight is MC only
                            continue
                        wsample["is_split_bycat"] = True
                        # looping on all the samples for this category
                        if w in wsample["inclusive"]:
                            raise Exception(
                                """Error! Trying to include weight {w}
                            by category, but it is already included inclusively!"""
                            )
                        wsample["bycategory"][cat].append(w)

        # Now look at specific samples configurations
        if "bysample" in wcfg:
            for sample, s_wcfg in wcfg["bysample"].items():
                if sample not in self.samples:
                    print(
                        f"Requested missing sample {sample} in the weights configuration"
                    )
                    raise Exception("Wrong weight configuration")

                if "inclusive" in s_wcfg:
                    for w in s_wcfg["inclusive"]:
                        if w not in self.available_weights:
                            print(f"Weight {w} not available in the configuration. Did you add it in the weights_classes?")
                            raise Exception("Wrong weight configuration")
                        
                        if not self.samples_metadata[sample]["isMC"] and self.available_weights[w].isMC_only:
                            # Do not add in the data weights configuration if the weight is MC only
                            continue
                        self.requested_weights.append(w)
                        # append only to the specific sample
                        self.weights_config[sample]["inclusive"].append(w)

                if "bycategory" in s_wcfg:
                    for cat, weights in s_wcfg["bycategory"].items():
                        for w in weights:
                            if w not in self.available_weights:
                                print(f"Weight {w} not available in the configuration. Did you add it in the weights_classes?")
                                raise Exception("Wrong weight configuration")
                            if w in self.weights_config[sample]["inclusive"]:
                                raise Exception(
                                    f"""Error! Trying to include weight {w}
                                by category, but it is already included inclusively!"""
                                )
                            if not self.samples_metadata[sample]["isMC"] and self.available_weights[w].isMC_only:
                                # Do not add in the data weights configuration if the weight is MC only
                                continue
                            self.requested_weights.append(w)
                            self.weights_config[sample]["bycategory"][cat].append(w)
                            self.weights_config[sample]["is_split_bycat"] = True

                if "inclusive" not in s_wcfg and "bycategory" not in s_wcfg:
                    print(f"None of the `inclusive` or `bycategory` keys found in the weights configuration for sample {sample}.\n")
                    print(
                        """The dictionary structure should be like:\n
                        'bysample': {
                            'sample_name': {
                                'inclusive': ['weight1', 'weight2'],
                                'bycategory': {'cat1': ['weight1', 'weight2']}
                        }\n"""
                    )
                    raise Exception("Wrong weight configuration")

    def load_variations_config(self, wcfg, variation_type):
        '''This function loads the variations definition and prepares a list of
        weights to be applied for each sample and category'''

        # Get the list of statically available variations defined in the workflow
        if variation_type=="weights":
            # The variations strings are the name of the weight
            # the WeigthWrapper class will define the available variations
            available_variations = self.available_weights 
        elif variation_type=="shape":
            available_variations = self.workflow.available_variations()
        # Read the config and save the list of variations names for each sample (and category if needed)

        if "common" not in wcfg:
            print("Variation configuration error: missing 'common' weights key")
            raise Exception("Wrong variation configuration")
        # common/inclusive variations
        for w in wcfg["common"]["inclusive"]:
            if w not in available_variations:
                print(f"Variation {w} not available in the workflow")
                print("Available variations: ", available_variations)
                raise Exception(f"Wrong variation configuration: variation {w} not available in the workflow")
            for sample, wsample in self.variations_config.items():
                if variation_type == "weights" and w not in self.weights_config[sample]["inclusive"]:
                    print(f"Error: variation {w} not available for sample {sample} in inclusive category")
                    raise Exception(f"Wrong variation configuration: variation {w} not available for sample {sample} in inclusive category")
                # add the variation to all the categories and samples
                for wcat in wsample[variation_type].values():
                    wcat.append(w)

        if "bycategory" in wcfg["common"]:
            for cat, variations in wcfg["common"]["bycategory"].items():
                for w in variations:
                    if w not in available_variations:
                        print(f"Variation {w} not available in the workflow")
                        raise Exception(f"Wrong variation configuration: variation {w} not available in the workflow")
                    for sample, wsample in self.variations_config.items():
                        if (variation_type == "weights" and
                            self.weights_config[sample]["is_split_bycat"] and
                            w not in self.weights_config[sample]["bycategory"][cat]):
                            print(f"Error: variation {w} not available for sample {sample} in {cat} category")
                            raise Exception(f"Wrong variation configuration: variation {w} not available for sample {sample} in {cat} category")
                        if w not in wsample[variation_type][cat]:
                            wsample[variation_type][cat].append(w)

        # Now look at specific samples configurations
        if "bysample" in wcfg:
            for sample, s_wcfg in wcfg["bysample"].items():
                if sample not in self.samples:
                    print(
                        f"Requested missing sample {sample} in the variations configuration"
                    )
                    raise Exception(f"Wrong variation configuration: sample {sample} not available in the samples list")
                if "inclusive" in s_wcfg:
                    for w in s_wcfg["inclusive"]:
                        if w not in available_variations:
                            print(f"Variation {w} not available in the workflow")
                            raise Exception(f"Wrong variation configuration: variation {w} not available in the workflow")
                        if variation_type == "weights" and  w not in self.weights_config[sample]["inclusive"]:
                            print(f"Error: variation {w} not available for sample {sample} in inclusive category")
                            raise Exception(f"Wrong variation configuration: variation {w} not available for sample {sample} in inclusive category")
                        # append only to the specific sample
                        for wcat in self.variations_config[sample][
                            variation_type
                        ].values():
                            if w not in wcat:
                                wcat.append(w)

                if "bycategory" in s_wcfg:
                    for cat, variation in s_wcfg["bycategory"].items():
                        for w in variation:
                            if w not in available_variations:
                                print(
                                    f"Variation {w} not available in the workflow"
                                )
                                raise Exception(f"Wrong variation configuration: variation {w} not available in the workflow")
                            if (variation_type=="weights" and
                                self.weights_config[sample]["is_split_bycat"] and
                                w not in self.weights_config[sample]["bycategory"][cat]):
                                print(f"Error: variation {w} not available for sample {sample} in {cat} category")
                                raise Exception(f"Wrong variation configuration: variation {w} not available for sample {sample} in {cat} category")
                            self.variations_config[sample][variation_type][cat].append(
                                w
                            )

                if "inclusive" not in s_wcfg and "bycategory" not in s_wcfg:
                    print(f"None of the `inclusive` or `bycategory` keys found in the weights configuration for sample {sample}.\n")
                    print(
                        """The dictionary structure should be like:\n
                        'bysample': {
                            'sample_name': {
                                'inclusive': ['weight1', 'weight2'],
                                'bycategory': {'cat1': ['weight1', 'weight2']}
                        }\n"""
                    )
                    raise Exception("Wrong weight configuration")

    def load_columns_config(self, wcfg):
        if wcfg == None:
            wcfg = {}
        for sample in self.samples:
            if self.has_subsamples[sample]:
                for sub in self.subsamples[sample]:
                    self.columns[f"{sample}__{sub}"] = {c: [] for c in self.categories.keys()}
            else:
                self.columns[sample] = {c: [] for c in self.categories.keys()}
        # common/inclusive variations
        if "common" in wcfg:
            if "inclusive" in wcfg["common"]:
                for w in wcfg["common"]["inclusive"]:
                    # do now check if the variations is not string but custom
                    for wsample in self.columns.values():
                        # add the variation to all the categories and samples
                        for wcat in wsample.values():
                            wcat.append(w)

            if "bycategory" in wcfg["common"]:
                for cat, columns in wcfg["common"]["bycategory"].items():
                    for w in columns:
                        for wsample in self.columns.values():
                            if w not in wsample[cat]:
                                wsample[cat].append(w)

        # Now look at specific samples configurations
        if "bysample" in wcfg:
            for sample, s_wcfg in wcfg["bysample"].items():
                if sample not in self.total_samples_list:
                    print(
                        f"Requested missing sample {sample} in the columns configuration"
                    )
                    raise Exception("Wrong columns configuration")
                if "inclusive" in s_wcfg:
                    for w in s_wcfg["inclusive"]:
                        # If the the sample 
                        if sample not in self.subsamples_list:
                            # the sample name is not a subsample, we need to check if it has subsamples
                            if not self.has_subsamples[sample]:
                            # add column only to the pure sample
                                for wcat in self.columns[sample].values():
                                    if w not in wcat:
                                        wcat.append(w)
                                       
                            else: # the sample has subsamples
                                # If it was added to the general one: include in all subsamples
                                for subs in self.subsamples[sample]:
                                    # Columns manager uses the subsample_list with the full name
                                    for wcat in self.columns[f"{sample}__{subs}"].values():
                                        if w not in wcat:
                                            wcat.append(w)
                        else:
                            # Add only to the specific subsample
                            for wcat in self.columns[sample].values():
                                if w not in wcat:
                                    wcat.append(w)

                if "bycategory" in s_wcfg:
                    for cat, columns in s_wcfg["bycategory"].items():
                        for w in columns:
                            if sample in self.subsamples_list:
                                self.columns[sample][cat].append(w)
                            elif self.has_subsamples[sample]:
                                for subs in self.subsamples[sample].keys():
                                    self.columns[f"{sample}__{subs}"][cat].append(w)
                            else:
                                self.columns[sample][cat].append(w)
        #prune the empty categories
        
    def perform_checks(self):
        # Check if two weights are defined inclusively
        for sample, wcfg in self.weights_config.items():
            if "genWeight" in wcfg["inclusive"] and "signOf_genWeight" in wcfg["inclusive"]:
                raise Exception("genWeight and signOf_genWeight cannot be used together\nPlease fix your weights configuration")
    
    def filter_dataset(self, nfiles):
        filtered_filesets = {}
        filtered_datasets = []
        for dataset_name, ds in self.filesets.items():
            ds["files"] = ds["files"][0:nfiles]
            filtered_filesets[dataset_name] = ds
            filtered_datasets.append(dataset_name)
        self.filesets = filtered_filesets
        self.datasets = filtered_datasets

    def load_workflow(self):
        self.processor_instance = self.workflow(cfg=self)

    def save_config(self, output):
        if not self.loaded:
            print("The configurator is not loaded yet, please load it before saving the configuration")
            return
        ocfg = {}
        ocfg["datasets"] = {
            "names": self.datasets,
            "samples": self.samples,
            "filesets": self.filesets,
        }

        subsamples_cuts = self.subsamples
        dump_subsamples = {}
        for sample, subsamples in subsamples_cuts.items():
            dump_subsamples[sample] = {}
            dump_subsamples[sample] = subsamples.serialize()
        ocfg["datasets"]["subsamples"] = dump_subsamples

        skim_dump = []
        presel_dump = []
        cats_dump = {}
        for sk in self.skim:
            skim_dump.append(sk.serialize())
        for pre in self.preselections:
            presel_dump.append(pre.serialize())

        ocfg["skim"] = skim_dump
        ocfg["preselections"] = presel_dump

        # categories
        ocfg["categories"] = self.categories.serialize()

        try:
            ocfg["workflow"] = {
                "name": self.workflow.__name__,
                "workflow_options": self.workflow_options,
                "srcfile": inspect.getsourcefile(self.workflow),
            }
        except TypeError:
            ocfg["workflow"] = {
                "name": self.workflow.__name__,
                "workflow_options": self.workflow_options,
            }

        ocfg["weights"] = {}
        for sample, weights in self.weights_config.items():
            out = {"bycategory": {}, "inclusive": []}
            for cat, catw in weights["bycategory"].items():
                out["bycategory"][cat] = []
                for w in catw:
                    out["bycategory"][cat].append(w)
            for w in weights["inclusive"]:
                out["inclusive"].append(w)
            ocfg["weights"][sample] = out

        ocfg["variations"] = self.variations_config
        ocfg["variables"] = {
            key: val.serialize() for key, val in self.variables.items()
        }

        ocfg["columns"] = {
            s: {c: [] for c in self.categories} for s in self.total_samples_list
        }
        for sample, columns in self.columns.items():
            for cat, cols in columns.items():
                for col in cols:
                    ocfg["columns"][sample][cat].append(col.__dict__)

        # add the parameters as yaml in a separate file
        with open(os.path.join(output, "parameters_dump.yaml"), "w") as pf:
            # Materialize the interpolations
            pf.write(OmegaConf.to_yaml(self.parameters))

        # Save the serialized configuration in json
        output_cfg = os.path.join(output, "config.json")
        print("Saving config file to " + output_cfg)
        json.dump(ocfg, open(output_cfg, "w"), indent=2)
        # Pickle the configurator object in order to be able to reproduce completely the configuration
        cloudpickle.dump(self, open(os.path.join(output, "configurator.pkl"), "wb"))
        # dump also the parameters

    def __repr__(self):
        if not self.loaded:
            s = [
            'Configurator instance (not loaded yet):',
            f"  - Workflow: {self.workflow}",
            f"  - Workflow options: {self.workflow_options}"]
            return "\n".join(s)

        s = [
            'Configurator instance:',
            f"  - Workflow: {self.workflow}",
            f"  - Workflow options: {self.workflow_options}",
            f"  - N. datasets: {len(self.datasets)} "]

        for dataset, meta in self.filesets.items():
            metadata = meta["metadata"]
            s.append(f"   -- Dataset: {dataset},  Sample: {metadata['sample']}, N. files: {len(meta['files'])}, N. events: {metadata['nevents']}")

        s.append( f"  - Subsamples:")
        for subsample, cuts in self.subsamples.items():
            s.append(f"   -- Sample {subsample}: {cuts}")

        s += [
            f"  - Skim: {[c.name for c in self.skim]}",
            f"  - Preselection: {[c.name for c in self.preselections]}",
            f"  - Categories: {self.categories}",
            f"  - Variables:  {format(list(self.variables.keys()))}",
            f"  - Columns: {format(self.columns)}",
            f"  - available weights variations: {format(self.available_weights_variations)} ",
            f"  - available shape variations: {format(self.available_shape_variations)}",            
        ]
        return "\n".join(s)

    def __str__(self):
        return repr(self)


    def clone(self):
        '''Create a copy of the configurator in the loaded=False state'''
        return Configurator(
            workflow=self.workflow,
            parameters=self.parameters,
            datasets=self.datasets_cfg,
            skim=self.skim_cfg,
            preselections=self.preselections_cfg,
            categories=self.categories_cfg,
            weights=self.weights_cfg,
            weights_classes=self.weights_classes,
            variations=self.variations_cfg,
            variables=self.variables,
            columns=self.columns_cfg,
            workflow_options=self.workflow_options,
            save_skimmed_files=self.save_skimmed_files_folder,
            do_postprocessing=self.do_postprocessing,
        )
