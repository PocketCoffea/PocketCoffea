import os
import sys
import json
import pprint
import pickle
import importlib.util

from ..parameters.allhistograms import histogram_settings

class Configurator():
    def __init__(self, cfg, overwrite_output_dir=None, plot=False, plot_version=None):
        # Load config file and attributes
        self.plot = plot
        self.plot_version = plot_version
        self.load_config(cfg)
        # Load all the keys in the config (dangerous, can be improved)
        self.load_attributes()

        # Load dataset
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
        self.load_histogram_settings()

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
        exclude_auto_loading = ["categories"]
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
            exit(1)

    def load_cuts_and_categories(self):
        # The cuts_dict is saved just for record
        for skim in self.cfg["skim"]:
            self.cuts_dict[skim.id ] = skim
        for presel in self.cfg["preselections"]:
            self.cuts_dict[presel.id] = presel
        for cat, cuts in self.cfg["categories"].items():
            self.categories[cat] = []                
            for cut in cuts:
                self.cut_functions.append(cut)
                self.cuts_dict[cut.id] = cut
                self.categories[cat].append(cut.id)

        # Unique set of cuts
        self.cut_functions = set(self.cut_functions)
        for cat, cuts in self.categories.items():
            self.categories[cat] = set(cuts)
        print("Cuts:", list(self.cuts_dict.keys()))
        print("Categories:", self.categories)
    
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

    def load_histogram_settings(self):
        if isinstance(self.cfg['variables'], list):
            self.cfg['variables'] = {var_name : None for var_name in self.cfg['variables']}
        for var_name in self.cfg['variables'].keys():
            if self.cfg['variables'][var_name] == None:
                self.cfg['variables'][var_name] = histogram_settings['variables'][var_name]
            elif not isinstance(self.cfg['variables'][var_name], dict):
                sys.exit("Format non valid for histogram settings")
            elif set(self.cfg['variables'][var_name].keys()) != {'binning', 'xlim', 'xlabel'}:
                set_ctrl = {'binning', 'xlim', 'xlabel'}
                sys.exit(f"{var_name}: missing keys in histogram settings. Required keys missing: {set_ctrl - set(self.cfg['variables'][var_name].keys())}")
            elif 'n_or_arr' not in set(self.cfg['variables'][var_name]['binning'].keys()):
                sys.exit(f"{var_name}: missing keys in histogram binning. Required keys missing: {'n_or_arr'}")
            elif ( ('n_or_arr' in set(self.cfg['variables'][var_name]['binning'].keys())) &
                   (type(self.cfg['variables'][var_name]['binning']['n_or_arr']) == int)  &
                   (set(self.cfg['variables'][var_name]['binning'].keys()) != {'n_or_arr', 'lo', 'hi'}) ):
                set_ctrl = {'n_or_arr', 'lo', 'hi'}
                sys.exit(f"{var_name}: missing keys in histogram binning. Required keys missing: {set_ctrl - set(self.cfg['variables'][var_name]['binning'].keys())}")


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
        ocfg["workflow"] = self.workflow.__name__
        # Save the serialized configuration in json
        output_cfg = os.path.join(self.output, "config.json")
        print("Saving config file to " + output_cfg)
        json.dump(ocfg, open(output_cfg,"w"), indent=2)
        #Pickle the configurator object in order to be able to reproduce completely the configuration 
        pickle.dump(self, open(os.path.join(self.output,"configurator.pkl"),"wb"))
