import os
import sys
import json
from collections import defaultdict 

import parsl
from parsl import python_app
from parsl.config import Config
from parsl.executors.threads import ThreadPoolExecutor

class Sample():
    def __init__(self, name, das_names, sample, metadata):
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
                    self.metadata["nevents"] +=f['nevents']
                    self.metadata["size"] += f['size']
            if len(self.fileslist)==0:
                raise Exception(f"Found 0 files for sample {self}!")
    # Function to build the sample dictionary
    def get_sample_dict(self, prefix="root://xrootd-cms.infn.it//"):
        out = {
            self.name : {
            'metadata' : { k: str(v) for k,v in self.metadata.items()},
            'files': [prefix + f for f in self.fileslist]
        }}
        return out

    def check_files(self, prefix):
        for f in self.fileslist:
            ff = prefix + f
            if not os.path.exists(ff):
                print(f"Missing file: {ff}")
            
    def __repr__(self):
        return f"name: {self.name}, sample: {self.metadata['sample']}, das_name: {self.metadata['das_names']}, year: {self.metadata['year']}"

class Dataset():
    def __init__(self,name, cfg):
        self.prefix = cfg["storage_prefix"]
        self.name = name
        self.outfile = cfg["json_output"]
        self.sample = cfg["sample"]
        self.sample_dict = {}
        self.sample_dict_local = {}
        self.samples_obj = []
        self.get_samples(cfg["files"])

        
    # Function to build the dataset dictionary
    def get_samples(self, files):
        for scfg in files:
            sname = f"{self.name}_{scfg['metadata']['year']}"
            if not scfg["metadata"]["isMC"]:
                sname += f"_Era{scfg['metadata']['era']}"
            sample = Sample(name=sname,
                            das_names = scfg["das_names"],
                            sample=self.sample,
                            metadata=scfg["metadata"])
            self.samples_obj.append(sample)
            # Get the default prefix and the the one
            self.sample_dict.update(sample.get_sample_dict())
            self.sample_dict_local.update(sample.get_sample_dict(prefix=self.prefix))

    def _write_dataset(self, outfile, sample_dict, append=True, overwrite=False):
        print(f"Saving datasets {self.name} to {outfile}")
        if append and os.path.exists(outfile):
            # Update the same json file
            previous = json.load(open(outfile))
            if overwrite:
                previous.update(sample_dict)
                sample_dict = previous
            else:
                for k,v in sample_dict.values():
                    if k in previous:
                        raise Exception(f"Sample {k} already present in file {outfile}, not overwriting!")
                    else:
                        previous[k] = v
                        sample_dict = previous
        with open(outfile, 'w') as fp:
            json.dump(sample_dict, fp, indent=4)
            fp.close()
        
    # Function to save the dataset dictionary with xrootd and local prefixes
    def save(self, append=True, overwrite=False, split=False):
        if not split:
            for outfile, sample_dict in zip([self.outfile, self.outfile.replace('.json', '_local.json')], [self.sample_dict, self.sample_dict_local]):
                self._write_dataset(outfile, sample_dict, append, overwrite)
        else:
            samples_byyear = defaultdict(dict)
            samples_local_byyear = defaultdict(dict)            
            for k,v in self.sample_dict.items():
                samples_byyear[v["metadata"]["year"]][k] = v
            for k,v in self.sample_dict_local.items():
                samples_local_byyear[v["metadata"]["year"]][k] = v

            for year, sample_dict in samples_byyear.items():
                self._write_dataset(self.outfile.replace(".json",f"_{year}.json"), sample_dict, append, overwrite)
            for year, sample_dict in samples_local_byyear.items():
                self._write_dataset(self.outfile.replace(".json",f"_{year}_local.json"), sample_dict, append, overwrite)
                


    def check_samples(self):
        for sample in self.samples_obj:
            print(f"Checking sample {sample.name}")
            sample.check_files(self.prefix)
                
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


