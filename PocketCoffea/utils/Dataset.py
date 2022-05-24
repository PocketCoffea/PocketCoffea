import os
import sys
import json
import importlib.util

import parsl
from parsl import python_app
from parsl.config import Config
from parsl.executors.threads import ThreadPoolExecutor

class Sample():
    def __init__(self, name, das_names, sample, metadata, prefix="root://xrootd-cms.infn.it//"):
        '''
        Class representing a single analysis sample. 
        - The name is the unique key of the sample in the dataset file.
        - The DAS name is the unique identifier of the sample in CMS
        - The sample represent the category: DATA/Wjets/ttHbb/ttBB. It is used to group the same type of events
        - metadata contains various keys necessary for the analysis --> the dict passed around in the coffea processor
        -- year
        -- isMC: true/false
        -- era: A/B/C/D (only for data)
        '''
        self.name = name
        self.das_names = das_names
        self.metadata = {}
        self.metadata["das_names"] = das_names
        self.metadata["sample"] = sample
        self.metadata.update(metadata)
        self.metadata["nevents"] = 0
        self.metadata["size"] = 0
        self.fileslist = []
        self.get_filelist()

    # Function to get the dataset filelist from DAS
    def get_filelist(self):
        # TODO: Include here a switch to select between DAS and local storage
        for das_name in self.metadata["das_names"]:
            command = f'dasgoclient -json -query="file dataset={das_name}"'
            print(f"Executing query: {command}")
            filesjson = json.loads(os.popen(command).read())
            for fj in filesjson:
                f = fj["file"][0]
                if f["is_file_valid"] == 0:
                    print(f"ERROR: File not valid on DAS: {f['name']}")
                else:
                    self.fileslist.append(f['name'])
                    self.metadata["nevents"] += f['nevents']
                    self.metadata["size"] += f['size']            

    # Function to build the sample dictionary
    def get_sample_dict(self):
        out = {
            self.name : {
            'metadata' : self.metadata,
            'files': self.fileslist
        }}
        return out
            
    def __repr__(self):
        return f"name: {self.name}, sample: {self.sample}, das_name: {self.das_name}, year: {self.year}"

class Dataset():
    def __init__(self,name, cfg, prefix="root://xrootd-cms.infn.it//"):
        self.prefix = prefix
        self.name = name
        self.outfile = cfg["json_output"]
        self.sample = cfg["sample"]
        self.sample_dict = {}
        self.sample_dict_local = {}        
        self.get_samples(cfg["files"])

    # Function to build the dataset dictionary
    def get_samples(self, files):
        for scfg in files:
            name = f"{self.name}_{scfg['metadata']['year']}"
            if not scfg["metadata"]["isMC"]:
                name += f"_Era{scfg['metadata']['era']}"
            sample = Sample(name=name,
                            das_names = scfg["das_names"],
                            sample=self.sample,
                            metadata=scfg["metadata"])
            #sample_local = Sample(name, self.prefix)
            #self.sample_dict.update(sample.sample_dict)
            #self.sample_dict_local.update(sample_local.sample_dict)
            self.sample_dict.update(sample.get_sample_dict())
            self.sample_dict_local.update(sample.get_sample_dict())

    # Function to save the dataset dictionary with xrootd and local prefixes
    def save(self, local=True):
        for outfile, sample_dict in zip([self.outfile, self.outfile.replace('.json', '_local.json')], [self.sample_dict, self.sample_dict_local]):
            print(f"Saving datasets to {outfile}")
            with open(outfile, 'w') as fp:
                json.dump(sample_dict, fp, indent=4)
                fp.close()

    @python_app
    def down_file(fname, out, ith=None):
        if ith is not None:
            print(ith)
        os.system("xrdcp -P " + fname + " " + out)
        return 0

    def download(self):
        # Setup multithreading
        config_parsl = Config(executors=[ThreadPoolExecutor(max_threads=8)])
        parsl.load(config_parsl)

        # Write futures
        out_dict = {} # Output filename list
        run_futures = [] # Future list
        for key in sorted(self.sample_dict.keys()):
            new_list = [] 
            if isinstance(self.sample_dict[key], dict):
                filelist = self.sample_dict[key]['files']
            elif isinstance(self.sample_dict[key], list):
                filelist = self.sample_dict[key]
            else:
                raise NotImplemented
            for i, fname in enumerate(filelist):
                if i%5 == 0: 
                    # print some progress info
                    ith = f'{key}: {i}/{len(filelist)}'
                else:
                    ith = None
                out = os.path.join(os.path.abspath(self.prefix), fname.split("//")[-1].lstrip("/"))
                new_list.append(out)
                if os.path.isfile(out):
                    'File found'
                else:
                    x = self.down_file(fname, out, ith)
                    run_futures.append(x)
            if isinstance(self.sample_dict[key], dict):
                out_dict[key] = {}
                out_dict[key]['files'] = new_list
                out_dict[key]['metadata'] = self.sample_dict[key]['metadata']
            elif isinstance(self.sample_dict[key], list):
                out_dict[key] = new_list
            else:
                raise NotImplemented

        for i, r in enumerate(run_futures):
            r.result()

        outfile = self.outfile.replace('.json', '_download.json')
        print(f"Writing files to {outfile}")
        with open(outfile, 'w') as fp:
            json.dump(out_dict, fp, indent=4)


