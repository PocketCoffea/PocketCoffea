import os
import sys
import json
from copy import deepcopy
from pprint import pprint, pformat
import cloudpickle
import importlib.util
from collections import defaultdict
import inspect
import logging

from ..lib.cut_definition import Cut
from ..lib.cartesian_categories import CartesianSelection
from ..parameters.cuts.preselection_cuts import passthrough
from ..lib.weights_manager import WeightCustom
from ..lib.hist_manager import Axis, HistConf


class Configurator:
    def __init__(self, cfg, overwrite_output_dir=None, plot=False, plot_version=None):
        # Load config file and attributes
        self.plot = plot
        self.plot_version = plot_version
        self.load_config(cfg)
        # Load all the keys in the config (dangerous, can be improved)
        self.load_attributes()

        # Load dataset
        self.fileset = {}
        self.samples = []
        self.years = []
        self.eras = []
        self.load_dataset()

        # subsamples configuration
        # Dictionary with subsample_name:[list of Cut ids]
        self.subsamples = {
            key: subscfg
            for key, subscfg in self.dataset.get("subsamples", {}).items()
            if key in self.samples
        }

        # Check if output file exists, and in case add a `_v01` label, make directory
        if overwrite_output_dir:
            self.output = overwrite_output_dir
        else:
            self.overwrite_check()

        self.mkdir_output()

        # Truncate file list if self.limit is not None
        self.truncate_filelist()

        # Define output file path
        self.define_output()

        # Load histogram settings
        # No manipulation is needed, maybe some check can be added

        ## Load cuts and categories
        # Cuts: set of Cut objects
        # Categories: dict with a set of Cut ids for each category
        self.cut_functions = []
        self.categories = {}
        # Saving also a dict of Cut objects to map their ids (name__hash)
        # N.B. The preselections are just cuts that are applied before
        # others. It is just a special category of cuts.
        self.cuts_dict = {}
        ## Call the function which transforms the dictionary in the cfg
        # in the objects needed in the processors
        self.load_cuts_and_categories()
        ## Weights configuration
        self.weights_config = {
            s: {
                "inclusive": [],
                "bycategory": {c: [] for c in self.categories.keys()},
                "is_split_bycat": False,
            }
            for s in self.samples
        }
        self.load_weights_config()
        ## Variations configuration
        # The structure is very similar to the weights one,
        # but the common and inclusive collections are fully flattened on a
        # sample:category structure
        self.variations_config = {
            s: {"weights": {c: [] for c in self.categories.keys()},
                "shape": {c: [] for c in self.categories.keys()}}
            for s in self.samples
        }
        if "shape" not in self.cfg["variations"]:
            self.cfg["variations"]["shape"] = {"common":{"inclusive": []}}
        self.load_variations_config(self.cfg["variations"]["weights"], variation_type="weights")
        self.load_variations_config(self.cfg["variations"]["shape"], variation_type="shape")
        self.available_weights_variations = { s : ["nominal"] for s in self.samples}
        self.available_shape_variations = { s : [] for s in self.samples}

        for sample in self.samples:
            # Weights variations
            for cat, vars in self.variations_config[sample]["weights"].items():
                self.available_weights_variations[sample] += vars
            #Shape variations                     
            for cat, vars in self.variations_config[sample]["shape"].items():
                self.available_shape_variations[sample] += vars

            self.available_weights_variations[sample] = list(set(self.available_weights_variations[sample]))
            self.available_shape_variations[sample] = list(set(self.available_shape_variations[sample]))

        # Column accumulator config
        self.columns = {
            s: {c: [] for c in self.categories.keys()} for s in self.samples
        }
        self.load_columns_config()

        # Load workflow
        self.load_workflow()

        # Save config file in output folder
        self.save_config()

    def load_config(self, path):
        spec = importlib.util.spec_from_file_location("cfg", path)
        cfg = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cfg)
        self.cfg = cfg.cfg

    def load_attributes(self):
        exclude_auto_loading = ["categories", "weights", "variations", "columns"]
        for key, item in self.cfg.items():
            if key in exclude_auto_loading:
                continue
            setattr(self, key, item)
        # Define default values for optional parameters
        for key in ['only']:
            try:
                getattr(self, key)
            except:
                setattr(self, key, '')
        if self.plot:
            # If a specific version is specified, plot that version
            if self.plot_version != None:
                if self.plot_version != '':
                    self.output = self.output + f'_{self.plot_version}'
                if not os.path.exists(self.output):
                    sys.exit(f"The output folder {self.output} does not exist")
            # If no version is specified, plot the latest version of the output
            else:
                parent_dir = os.path.abspath(os.path.join(self.output, os.pardir))
                output_dir = os.path.basename(self.output)
                latest_dir = list(
                    filter(
                        lambda folder: (
                            (output_dir == folder) | (output_dir + '_v' in folder)
                        ),
                        sorted(os.listdir(parent_dir)),
                    )
                )[-1]
                self.output = os.path.join(parent_dir, latest_dir)
            self.plots = os.path.join(os.path.abspath(self.output), "plots")

    def load_dataset(self):
        for json_dataset in self.dataset["jsons"]:
            ds_dict = json.load(open(json_dataset))
            ds_filter = self.dataset.get("filter", None)
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
                        self.fileset[key] = ds
            else:
                self.fileset.update(ds_dict)
        if len(self.fileset) == 0:
            print("File set is empty: please check you dataset definition...")
            raise Exception("Wrong fileset configuration")
        else:
            for name, d in self.fileset.items():
                m = d["metadata"]
                if (m["sample"]) not in self.samples:
                    self.samples.append(m["sample"])
                    self.years.append(m["year"])
                    if 'era' in m.keys():
                        self.eras.append(m["era"])

    def filter_dataset(self, nfiles):
        filtered_dataset = {}
        for sample, ds in self.fileset.items():
            ds["files"] = ds["files"][0:nfiles]
            filtered_dataset[sample] = ds
        self.fileset = filtered_dataset

    def load_cuts_and_categories(self):
        '''This function loads the list of cuts and groups them in categories.
        Each cut is identified by a unique id (see Cut class definition)'''
        # If the skim, preselection and categories list are empty, append a `passthrough` Cut
        for cut_type in ["skim", "preselections"]:
            if len(self.cfg[cut_type]) == 0:
                setattr(self, cut_type, [passthrough])
                self.cfg[cut_type] = [passthrough]
        if self.cfg["categories"] == {}:
            self.cfg["categories"]["baseline"] = [passthrough]
        # The cuts_dict is saved just for record
        for skim in self.cfg["skim"]:
            if not isinstance(skim, Cut):
                print("Please define skim, preselections and cuts as Cut objects")
                raise Exception("Wrong categories/cuts configuration")
            self.cuts_dict[skim.id] = skim
        for presel in self.cfg["preselections"]:
            if not isinstance(presel, Cut):
                print("Please define skim, preselections and cuts as Cut objects")
                raise Exception("Wrong categories/cuts configuration")
            self.cuts_dict[presel.id] = presel
        # Now saving the categories and cuts
        if isinstance(self.cfg["categories"], dict):
            for cat, cuts in self.cfg["categories"].items():
                self.categories[cat] = []
                for cut in cuts:
                    if not isinstance(cut, Cut):
                        print("Please define skim, preselections and cuts as Cut objects")
                        raise Exception("Wrong categories/cuts configuration")
                    self.cut_functions.append(cut)
                    self.cuts_dict[cut.id] = cut
                    self.categories[cat].append(cut.id)
            for cat, cuts in self.categories.items():
                self.categories[cat] = set(cuts)

        elif isinstance(self.cfg["categories"], CartesianSelection):
            self.categories = self.cfg["categories"]
        # Unique set of cuts
        self.cut_functions = set(self.cut_functions)
        logging.info("Cuts:")
        logging.info(pformat(self.cuts_dict.keys(), compact=True, indent=2))
        logging.info("Categories:")
        logging.info(pformat(self.categories, compact=True, indent=2))

    def load_weights_config(self):
        '''This function loads the weights definition and prepares a list of
        weights to be applied for each sample and category'''
        # Get the list of statically available weights defined in the workflow
        available_weights = self.workflow.available_weights()
        # Read the config and save the list of weights names for each sample (and category if needed)
        wcfg = self.cfg["weights"]
        if "common" not in wcfg:
            print("Weights configuration error: missing 'common' weights key")
            raise Exception("Wrong weight configuration")
        # common/inclusive weights
        for w in wcfg["common"]["inclusive"]:
            if isinstance(w, str):
                if w not in available_weights:
                    print(f"Weight {w} not available in the workflow")
                    raise Exception("Wrong weight configuration")
            # do now check if the weights is not string but custom
            for wsample in self.weights_config.values():
                # add the weight to all the categories and samples
                wsample["inclusive"].append(w)

        if "bycategory" in wcfg["common"]:
            for cat, weights in wcfg["common"]["bycategory"].items():
                for w in weights:
                    if isinstance(w, str):
                        if w not in available_weights:
                            print(f"Weight {w} not available in the workflow")
                            raise Exception("Wrong weight configuration")
                    for wsample in self.weights_config.values():
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
                        if isinstance(w, str):
                            if w not in available_weights:
                                print(f"Weight {w} not available in the workflow")
                                raise Exception("Wrong weight configuration")
                        # append only to the specific sample
                        self.weights_config[sample]["inclusive"].append(w)

                if "bycategory" in s_wcfg:
                    for cat, weights in s_wcfg["bycategory"].items():
                        for w in weights:
                            if isinstance(w, str):
                                if w not in available_weights:
                                    print(f"Weight {w} not available in the workflow")
                                    raise Exception("Wrong weight configuration")
                            if w in self.weights_config[sample]["inclusive"]:
                                raise Exception(
                                    f"""Error! Trying to include weight {w}
                                by category, but it is already included inclusively!"""
                                )
                            self.weights_config[sample]["bycategory"][cat].append(w)
                            self.weights_config[sample]["is_split_bycat"] = True

        logging.info("Weights configuration")
        logging.info(self.weights_config)

    def load_variations_config(self, wcfg, variation_type):
        '''This function loads the variations definition and prepares a list of
        weights to be applied for each sample and category'''
        # Get the list of statically available variations defined in the workflow
        available_variations = self.workflow.available_variations()
        # Read the config and save the list of variations names for each sample (and category if needed)
  
        # TODO Add shape variations
        if "common" not in wcfg:
            print("Variation configuration error: missing 'common' weights key")
            raise Exception("Wrong variation configuration")
        # common/inclusive variations
        for w in wcfg["common"]["inclusive"]:
            if isinstance(w, str):
                if w not in available_variations:
                    print(f"Variation {w} not available in the workflow")
                    raise Exception("Wrong variation configuration")
            # do now check if the variations is not string but custom
            for wsample in self.variations_config.values():
                # add the variation to all the categories and samples
                for wcat in wsample[variation_type].values():
                    wcat.append(w)

        if "bycategory" in wcfg["common"]:
            for cat, variations in wcfg["common"]["bycategory"].items():
                for w in variations:
                    if isinstance(w, str):
                        if w not in available_variations:
                            print(f"Variation {w} not available in the workflow")
                            raise Exception("Wrong variation configuration")
                    for wsample in self.variations_config.values():
                        if w not in wsample[variation_type][cat]:
                            wsample[variation_type][cat].append(w)

        # Now look at specific samples configurations
        if "bysample" in wcfg:
            for sample, s_wcfg in wcfg["bysample"].items():
                if sample not in self.samples:
                    print(
                        f"Requested missing sample {sample} in the variations configuration"
                    )
                    raise Exception("Wrong variation configuration")
                if "inclusive" in s_wcfg:
                    for w in s_wcfg["inclusive"]:
                        if isinstance(w, str):
                            if w not in available_variations:
                                print(f"Variation {w} not available in the workflow")
                                raise Exception("Wrong variation configuration")
                        # append only to the specific sample
                        for wcat in self.variations_config[sample][variation_type].values():
                            if w not in wcat:
                                wcat.append(w)

                if "bycategory" in s_wcfg:
                    for cat, variation in s_wcfg["bycategory"].items():
                        for w in variation:
                            if isinstance(w, str):
                                if w not in available_variations:
                                    print(
                                        f"Variation {w} not available in the workflow"
                                    )
                                    raise Exception("Wrong variation configuration")
                            self.variations_config[sample][variation_type][cat].append(w)

    def load_columns_config(self):
        wcfg = self.cfg.get("columns",{})
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
                if sample not in self.samples:
                    print(
                        f"Requested missing sample {sample} in the columns configuration"
                    )
                    raise Exception("Wrong columns configuration")
                if "inclusive" in s_wcfg:
                    for w in s_wcfg["inclusive"]:
                        # append only to the specific sample
                        for wcat in self.columns[sample].values():
                            if w not in wcat:
                                wcat.append(w)

                if "bycategory" in s_wcfg:
                    for cat, columns in s_wcfg["bycategory"].items():
                        for w in columns:
                            self.columns[sample][cat].append(w)

    def overwrite_check(self):
        if self.plot:
            print(f"The output will be saved to {self.plots}")
            return
        else:
            path = self.output
            version = 1
            while os.path.exists(path):
                tag = str(version).rjust(2, '0')
                path = f"{self.output}_v{tag}"
                version += 1
            if path != self.output:
                print(f"The output will be saved to {path}")
            self.output = path
            self.cfg['output'] = self.output

    def mkdir_output(self):
        if not self.plot:
            if not os.path.exists(self.output):
                os.makedirs(self.output)
        else:
            if not os.path.exists(self.plots):
                os.makedirs(self.plots)

    def truncate_filelist(self):
        try:
            self.run_options['limit']
        except:
            self.run_options['limit'] = None
        if self.run_options['limit']:
            for dataset, filelist in self.fileset.items():
                if isinstance(filelist, dict):
                    self.fileset[dataset]["files"] = self.fileset[dataset]["files"][
                        : self.run_options['limit']
                    ]
                elif isinstance(filelist, list):
                    self.fileset[dataset] = self.fileset[dataset][
                        : self.run_options['limit']
                    ]
                else:
                    raise NotImplemented

    def define_output(self):
        self.outfile = os.path.join(self.output, "output.coffea")

    def load_workflow(self):
        self.processor_instance = self.workflow(cfg=self)

    def save_config(self):
        ocfg = {k: v for k, v in self.cfg.items()}

        subsamples_cuts = ocfg["dataset"].get("subsamples", {})
        dump_subsamples = {}
        for sample, subsamples in subsamples_cuts.items():
            dump_subsamples[sample] = {}
            for subs, cuts in subsamples.items():
                dump_subsamples[sample][subs] = [c.serialize() for c in cuts]
        ocfg["dataset"]["subsamples"] = dump_subsamples

        skim_dump = []
        presel_dump = []
        cats_dump = {}
        for sk in ocfg["skim"]:
            skim_dump.append(sk.serialize())
        for pre in ocfg["preselections"]:
            presel_dump.append(pre.serialize())

        ocfg["skim"] = skim_dump
        ocfg["preselections"] = presel_dump
        
        if not isinstance(self.categories, CartesianSelection):
            for cat, cuts in ocfg["categories"].items():
                newcuts = []
                for c in cuts:
                    newcuts.append(c.serialize())
                cats_dump[cat] = newcuts

            ocfg["categories"] = cats_dump
        else:
            ocfg["categories"] = str(self.categories)
            
        ocfg["workflow"] = {
            "name": self.workflow.__name__,
            "srcfile": inspect.getsourcefile(self.workflow),
        }

        ocfg["weights"] = {}
        for sample, weights in self.weights_config.items():
            out = {"bycategory": {}, "inclusive": []}
            for cat, catw in weights["bycategory"].items():
                out["bycategory"][cat] = []
                for w in catw:
                    if isinstance(w, WeightCustom):
                        out["bycategory"][cat].append(w.serialize())
                    else:
                        out["bycategory"][cat].append(w)
            for w in weights["inclusive"]:
                if isinstance(w, WeightCustom):
                    out["inclusive"].append(w.serialize())
                else:
                    out["inclusive"].append(w)
            ocfg["weights"][sample] = out

        ocfg["variations"] = self.variations_config
        ocfg["variables"] = {
            key: val.serialize() for key, val in self.variables.items()
        }

        ocfg["columns"] = {s: {c: [] for c in self.categories} for s in self.samples}
        for sample, columns in self.columns.items():
            for cat, cols in columns.items():
                for col in cols:
                    ocfg["columns"][sample][cat].append(col.__dict__)

        # Save the serialized configuration in json
        output_cfg = os.path.join(self.output, "config.json")
        print("Saving config file to " + output_cfg)
        json.dump(ocfg, open(output_cfg, "w"), indent=2)
        # Pickle the configurator object in order to be able to reproduce completely the configuration
        cloudpickle.dump(
            self, open(os.path.join(self.output, "configurator.pkl"), "wb")
        )
