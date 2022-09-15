import os
import sys
import json
from pprint import pprint
import pickle
import importlib.util
from collections import defaultdict
import inspect

from ..lib.cut_definition import Cut
from ..lib.WeightsManager import WeightCustom
from ..lib.HistManager import Axis, HistConf


class Configurator():
    def __init__(self, cfg, overwrite_output_dir=None, plot=False, plot_version=None):
        # Load config file and attributes
        self.plot = plot
        self.plot_version = plot_version
        self.load_config(cfg)
        # Load all the keys in the config (dangerous, can be improved)
        self.load_attributes()

        # Load dataset
        self.samples = []
        self.years = []
        self.load_dataset()

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
        self.weights_config = { s: {"inclusive":[],
                                    "bycategory": { c: [] for c in self.categories.keys()},
                                    "is_split_bycat": False} for s in self.samples }
        self.load_weights_config()
        ## Variations configuration
        # The structure is very similar to the weights one,
        # but the common and inclusive collections are fully flattened on a
        # sample:category structure
        self.variations_config = {
             s: { "weights": {c: [] for c in self.categories.keys()}}
                for s in self.samples   }
        self.load_variations_config()

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
        exclude_auto_loading = ["categories", "weights"]
        for key, item in self.cfg.items():
            if key in exclude_auto_loading: continue
            setattr(self, key, item)
        # Define default values for optional parameters
        for key in ['only']:
            try: getattr(self, key)
            except: setattr(self, key, '')
        if self.plot:
            # If a specific version is specified, plot that version
            if self.plot_version:
                self.output = self.output + f'_{self.plot_version}'
                if not os.path.exists(self.output):
                    sys.exit(f"The output folder {self.output} does not exist")
            # If no version is specified, plot the latest version of the output
            else:
                parent_dir = os.path.abspath(os.path.join(self.output, os.pardir))
                output_dir = os.path.basename(self.output)
                latest_dir = list(filter(lambda folder : ((output_dir == folder) | (output_dir+'_v' in folder)), sorted(os.listdir(parent_dir))))[-1]
                self.output = os.path.join(parent_dir, latest_dir)
            self.plots = os.path.join( os.path.abspath(self.output), "plots" )

    def load_dataset(self):
        self.fileset = {}
        for json_dataset in self.dataset["jsons"]:
            ds_dict = json.load(open(json_dataset))
            ds_filter = self.dataset.get("filter",None)
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
                if (m:=d["metadata"]) not in self.samples:
                    self.samples.append(m["sample"])
                    self.years.append(m["year"])

    def load_cuts_and_categories(self):
        '''This function loads the list of cuts and groups them in categories.
        Each cut is identified by a unique id (see Cut class definition)'''
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
        for cat, cuts in self.cfg["categories"].items():
            self.categories[cat] = []                
            for cut in cuts:
                if not isinstance(cut, Cut):
                    print("Please define skim, preselections and cuts as Cut objects")
                    raise Exception("Wrong categories/cuts configuration")
                self.cut_functions.append(cut)
                self.cuts_dict[cut.id] = cut
                self.categories[cat].append(cut.id)
        # Unique set of cuts
        self.cut_functions = set(self.cut_functions)
        for cat, cuts in self.categories.items():
            self.categories[cat] = set(cuts)
        print("Cuts:", list(self.cuts_dict.keys()))
        print("Categories:", self.categories)

        
    def load_weights_config(self):
        ''' This function loads the weights definition and prepares a list of
        weights to be applied for each sample and category'''
        # Get the list of statically available weights defined in the workflow
        available_weights = self.workflow.available_weights()
        #Read the config and save the list of weights names for each sample (and category if needed)
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
                            print(f"""Error! Trying to include weight {w}
                            by category, but it is already included inclusively!""")
                            raise Exception("Wrong weight configuration")
                        wsample["bycategory"][cat].append(w)

        # Now look at specific samples configurations
        if "bysample" in wcfg:
            for sample, s_wcfg in wcfg["bysample"].items():
                if sample not in self.samples:
                    print(f"Requested missing sample {sample} in the weights configuration")
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
                                print(f"""Error! Trying to include weight {w}
                                by category, but it is already included inclusively!""")
                                raise Exception("Wrong weight configuration")
                            self.weights_config[sample]["bycategory"][cat].append(w)
                            self.weights_config[sample]["is_split_bycat"] = True
                
        print("Weights configuration")
        pprint(self.weights_config)


    def load_variations_config(self):
        ''' This function loads the variations definition and prepares a list of
        weights to be applied for each sample and category'''
        # Get the list of statically available variations defined in the workflow
        available_variations = self.workflow.available_variations()
        #Read the config and save the list of variations names for each sample (and category if needed)
        wcfg = self.cfg["variations"]["weights"]
        # TODO Add shape variations
        print(available_variations)
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
                for wcat in wsample["weights"].values():
                    wcat.append(w)

        if "bycategory" in wcfg["common"]:
            for cat, variations in wcfg["common"]["bycategory"].items():
                for w in variations:
                    if isinstance(w, str):
                        if w not in available_variations:
                            print(f"Variation {w} not available in the workflow")
                            raise Exception("Wrong variation configuration")
                    for wsample in self.variations_config.values():
                        if w not in wsample["weights"][cat]:
                            wsample["weights"][cat].append(w)

        # Now look at specific samples configurations
        if "bysample" in wcfg:
            for sample, s_wcfg in wcfg["bysample"].items():
                if sample not in self.samples:
                    print(f"Requested missing sample {sample} in the variations configuration")
                    raise Exception("Wrong variation configuration")
                if "inclusive" in s_wcfg:
                    for w in s_wcfg["inclusive"]:
                        if isinstance(w, str):
                            if w not in available_variations:
                                print(f"Variation {w} not available in the workflow")
                                raise Exception("Wrong variation configuration")
                        # append only to the specific sample
                        for wcat in self.variations_config[sample]["weights"].values():
                            if w not in wcat:
                                wcat.append(w)

                if "bycategory" in s_wcfg:
                    for cat, variation  in s_wcfg["bycategory"].items():
                        for w in variation :
                            if isinstance(w, str):
                                if w not in available_variations :
                                    print(f"Variation {w} not available in the workflow")
                                    raise Exception("Wrong variation configuration")
                            self.variations_config[sample]["weights"][cat].append(w)
                
        print("Variation configuration")
        pprint(self.variations_config)
 
        
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
        try: self.run_options['limit']
        except: self.run_options['limit'] = None
        if self.run_options['limit']:
            for dataset, filelist in self.fileset.items():
                if isinstance(filelist, dict):
                    self.fileset[dataset]["files"] = self.fileset[dataset]["files"][:self.run_options['limit']]
                elif isinstance(filelist, list):
                    self.fileset[dataset] = self.fileset[dataset][:self.run_options['limit']]
                else:
                    raise NotImplemented

    def define_output(self):
        self.outfile = os.path.join(self.output, "output.coffea")


    def load_workflow(self):
        self.processor_instance = self.workflow(cfg=self)

    def save_config(self):
        ocfg = {k:v for k,v in self.cfg.items()}
        skim_dump = []
        presel_dump = []
        cats_dump = {}
        for sk in ocfg["skim"]:
            skim_dump.append(sk.serialize())
        for pre in ocfg["preselections"]:
            presel_dump.append(pre.serialize())
        for cat,cuts  in ocfg["categories"].items():
            newcuts = []
            for c in cuts:
                newcuts.append(c.serialize())
            cats_dump[cat] = newcuts
        ocfg["skim"] = skim_dump
        ocfg["preselections"] = presel_dump
        ocfg["categories"] = cats_dump
        ocfg["workflow"] = {
            "name": self.workflow.__name__,
            "srcfile": inspect.getsourcefile(self.workflow)
        }
        ocfg["weights"] = self.weights_config
        ocfg["variations"] = self.variations_config
        ocfg["variables"] = {
            key: val.serialize() for key,val in self.variables.items()
        }
        # Save the serialized configuration in json
        output_cfg = os.path.join(self.output, "config.json")
        print("Saving config file to " + output_cfg)
        json.dump(ocfg, open(output_cfg,"w"), indent=2)
        #Pickle the configurator object in order to be able to reproduce completely the configuration 
        pickle.dump(self, open(os.path.join(self.output,"configurator.pkl"),"wb"))
