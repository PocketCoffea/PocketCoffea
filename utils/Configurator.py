import os
import json
import importlib.util

class Configurator():
    def __init__(self, cfg):
        # Load config file and attributes
        self.load_config(cfg)
        self.load_attributes()

        # Load dataset
        with open(self.input) as f:
            self.fileset = json.load(f)

        # Check if output file exists, and in case add a `_v01` label
        self.overwrite_check()

        # Truncate file list if self.limit is not None
        try: self.limit
        except: self.limit = None
        if self.limit:
            for dataset, filelist in self.fileset.items():
                if isinstance(filelist, dict):
                    self.fileset[dataset]["files"] = self.fileset[dataset]["files"][:self.limit]
                elif isinstance(filelist, list):
                    self.fileset[dataset] = self.fileset[dataset][:self.limit]
                else:
                    raise NotImplemented

        # Define output file path
        try: self.output
        except: self.output = f'hists_{self.workflow}_{(self.input).rstrip(".json")}.coffea'

        # Load workflow
        if self.workflow == "base":
            from workflows.base import ttHbbBaseProcessor
            self.processor_instance = ttHbbBaseProcessor(cfg=cfg)
        elif self.workflow == "mem":
            from workflows.mem import MEMStudiesProcessor
            self.processor_instance = MEMStudiesProcessor(cfg=cfg)
        else:
            raise NotImplemented

    def load_config(self, path):
        spec = importlib.util.spec_from_file_location("cfg", path)
        cfg = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cfg)
        self.cfg = cfg.cfg

    def load_attributes(self):
        for key, item in self.cfg.items():
            setattr(self, key, item)

    def overwrite_check(self):
        path = self.output
        version = 1
        while os.path.exists(path):
            tag = str(version).rjust(2, '0')
            path = self.output.replace('.coffea', f'_v{tag}.coffea')
            version += 1
        if path != self.output:
            print(f"The output will be saved to {path}")
        self.output = path
