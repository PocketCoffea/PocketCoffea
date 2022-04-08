import os
import sys
import json
import pprint
import importlib.util

from parameters.allhistograms import histogram_settings

class Configurator():
    def __init__(self, cfg, plot=False, create_dataset=False):
        # Load config file and attributes
        self.plot    = plot
        self.create_dataset = create_dataset
        self.load_config(cfg)
        self.load_attributes()
        
        if not self.create_dataset:
            # Load dataset
            self.load_dataset()

            # Check if output file exists, and in case add a `_v01` label, make directory
            self.overwrite_check()
            self.mkdir_output()

            # Truncate file list if self.limit is not None
            self.truncate_filelist()

            # Define output file path
            self.define_output()

            # Load histogram settings
            self.load_histogram_settings()
            
            # Load cuts and categories
            self.categories = {}
            self.cuts = []
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
        self.plots = os.path.join( os.path.abspath(self.output), "plots" )

    def load_dataset(self):
        with open(self.input) as f:
            self.fileset = json.load(f)

    def load_cuts_and_categories(self):
        for cat, cuts in self.cfg["categories"].items():
            self.categories[cat] = []                
            for cut in cuts:
                self.cuts.append(cut)
                self.categories[cat].append(cut.name)

        # Unique set of cuts
        self.cuts = set(self.cuts)
        for cat, cuts in self.cfg["categories"].items():
            self.categories[cat] = set(cuts)
        print("Cuts:", self.cuts)
        print("Categories:", self.categories)
    
    def overwrite_check(self):
        if self.plot:
            print(f"The output will be saved to {self.plots}")
            return
        elif self.create_dataset:
            print(f"The output will be saved to {self.json}")
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
                sys.exit("Missing keys in histogram settings. Required keys: {'binning', 'xlim', 'xlabel'}")
            elif set(self.cfg['variables'][var_name]['binning'].keys()) != {'n_or_arr', 'lo', 'hi'}:
                sys.exit("Missing keys in histogram binning. Required keys: {'n_or_arr', 'lo', 'hi'}")

    def load_workflow(self):
        if self.workflow == "base":
            from workflows.base import ttHbbBaseProcessor
            self.processor_instance = ttHbbBaseProcessor(cfg=self)
        elif self.workflow == "mem":
            from workflows.mem import MEMStudiesProcessor
            self.processor_instance = MEMStudiesProcessor(cfg=self)
        else:
            raise NotImplemented

    def save_config(self):
        # functions_to_import = []
        # import_line = "from lib.cuts import "
        # for key in self.cfg['cuts_definition'].keys():
        #     functions_to_import.append(self.cfg['cuts_definition'][key]['f'])
        # buffer = ''.join( ("cfg = ", pprint.pformat(self.cfg, sort_dicts=False)) )
        # for f in functions_to_import:
        #     buffer = buffer.replace(str(f), f.__name__)
        # import_line = ''.join( (import_line, ', '.join([f.__name__ for f in functions_to_import])) )
        # buffer = import_line + '\n\n' + buffer + '\n'

        # if self.plot:
        #     config_file = os.path.join(self.plots, "config.py")
        # else:
        #     config_file = os.path.join(self.output, "config.py")
        # print("Saving config file to " + config_file)
        # with open(config_file, 'w') as f:
        #     f.write(buf    
        # f.close()
        pass
