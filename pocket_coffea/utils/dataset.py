import os
import sys
import json
from collections import defaultdict

from multiprocessing import Pool
from functools import partial
import subprocess
import requests
import parsl
from parsl import python_app
from parsl.config import Config
from parsl.executors.threads import ThreadPoolExecutor

from .network import get_proxy_path
from . import rucio

def do_dataset(key, config, local_prefix, whitelist_sites, blacklist_sites, regex_sites, **kwargs):
    print("*" * 40)
    print("> Working on dataset: ", key)
    if key not in config:
        print("Key: not found in the dataset configuration file")
        exit(1)
    dataset_cfg = config[key]
    if local_prefix:
        dataset_cfg["storage_prefix"] = local_prefix

    try:
        dataset = Dataset(
            name=key,
            cfg=dataset_cfg,
            sites_cfg={
                "whitelist_sites": whitelist_sites,
                "blacklist_sites": blacklist_sites,
                "regex_sites": regex_sites,
            },
        )
    except:
        raise Exception(f"Error getting info about dataset: {key}")

    return dataset

def build_datasets(cfg, keys=None, overwrite=False, download=False, check=False, split_by_year=False, local_prefix=None,
                   whitelist_sites=None, blacklist_sites=None, regex_sites=None, parallelize=4):

    config = json.load(open(cfg))

    if not keys:
        keys = config.keys()
    args = {arg : value for arg, value in locals().items() if arg != "keys"}

    with Pool(parallelize) as pool:
        print(keys)
        datasets = pool.map(partial(do_dataset, **args), keys)

    for dataset in datasets:
        dataset.save(overwrite=overwrite, split=split_by_year)
        if check:
            dataset.check_samples()

        if download:
            dataset.download()

class Sample:
    def __init__(self, name, das_names, sample, metadata, sites_cfg, **kwargs):
        '''
        Class representing a single analysis sample.
        - The name is the unique key of the sample in the dataset file.
        - The DAS name is the unique identifier of the sample in CMS
        - The sample represent the type of events: DATA/Wjets/ttHbb/ttBB. It is used to group the same type of events
        - metadata contains various keys necessary for the analysis --> the dict passed around in the coffea processor
         -- year
         -- isMC: true/false
         -- era: A/B/C/D (only for data)
        - sites_cfg is a dictionary contaning whitelist, blacklist and regex to filter the SITES
        '''
        self.name = name
        self.das_names = das_names
        self.metadata = {}
        self.metadata["das_names"] = das_names
        if "dbs_instance" in kwargs.keys():
            self.metadata["dbs_instance"] = kwargs["dbs_instance"]
        self.metadata["sample"] = sample
        self.metadata.update(metadata)
        self.metadata["nevents"] = 0
        self.metadata["size"] = 0
        self.fileslist_redirector = []
        self.fileslist_concrete = []
        self.parentslist = []
        self.sites_cfg = sites_cfg

        print(
            f">> Query for sample: {self.metadata['sample']},  das_name: {self.metadata['das_names']}"
        )
        self.get_filelist()

    def get_filelist(self):
        '''Function to get the dataset filelist from DAS and from Rucio.
        From DAS we get the general info about the dataset (event count, file size),
        whereas from rucio we get the specific path at the sites without the redirector
        (it helps with xrootd access in coffea).
        '''
        for das_name in self.das_names:
            proxy = get_proxy_path()
            if "dbs_instance" in self.metadata.keys():
                link = f"https://cmsweb.cern.ch:8443/dbs/{self.metadata['dbs_instance']}/DBSReader/files?dataset={das_name}&detail=True"
            else:
                link = f"https://cmsweb.cern.ch:8443/dbs/prod/global/DBSReader/files?dataset={das_name}&detail=True"
            r = requests.get(
                link,
                cert=proxy,
                verify=False,
            )
            filesjson = r.json()
            for fj in filesjson:
                if fj["is_file_valid"] == 0:
                    print(f"ERROR: File not valid on DAS: {f['name']}")
                else:
                    self.fileslist_redirector.append(fj['logical_file_name'])
                    self.metadata["nevents"] += fj['event_count']
                    self.metadata["size"] += fj['file_size']
            if len(self.fileslist_redirector) == 0:
                raise Exception(f"Found 0 files for sample {self}!")

            # Now query rucio to get the concrete dataset passing the sites filtering options
            files_rucio, sites = rucio.get_dataset_files(
                das_name, **self.sites_cfg, output="first"
            )
            self.fileslist_concrete += files_rucio

    # Function to build the sample dictionary
    def get_sample_dict(self, redirector=True, prefix="root://xrootd-cms.infn.it//"):
        if redirector:
            out = {
                self.name: {
                    'metadata': {k: str(v) for k, v in self.metadata.items()},
                    'files': [prefix + f for f in self.fileslist_redirector],
                }
            }
        else:  # rucio
            out = {
                self.name: {
                    'metadata': {k: str(v) for k, v in self.metadata.items()},
                    'files': [f for f in self.fileslist_concrete],
                }
            }
        return out

    def get_parentlist(self, inplace=False):
        '''Function to get the parent dataset filelist from DAS.
        The parent list is included as an additional metadata in the sample's dict.
        '''
        for das_name in self.das_names:
            command = f'dasgoclient -json -query="parent dataset={das_name}"'
            records = json.load(os.popen(command))
            for record in records:
                parent = record['parent']
                assert (
                    len(parent) == 1
                ), f"The dataset {das_name} has none or more than one parent ({len(parent)})."
                parent_name = parent[0]['name']
                parent_format = "MINIAODSIM" if self.metadata["isMC"] else "MINIAOD"
                if parent_name.endswith(parent_format):
                    self.parentslist.append(parent_name)
            assert (
                len(self.parentslist) == 1
            ), f"The dataset {das_name} has none or more than one parent ({len(self.parentslist)})."
            self.metadata["parents"] = self.parentslist
        return self.parentslist

    def check_files(self, prefix):
        for f in self.fileslist:
            ff = prefix + f
            if not os.path.exists(ff):
                print(f"Missing file: {ff}")

    def __repr__(self):
        return f"name: {self.name}, sample: {self.metadata['sample']}, das_name: {self.metadata['das_names']}, year: {self.metadata['year']}"


class Dataset:
    def __init__(self, name, cfg, sites_cfg=None, append_parents=False):
        self.cfg = cfg
        self.prefix = cfg.get("storage_prefix", None)
        self.name = name
        self.outfile = cfg["json_output"]
        self.sample = cfg["sample"]
        self.sample_dict_redirector = {}
        self.sample_dict_concrete = {}
        self.sample_dict_local = {}
        self.samples_obj = []
        self.sites_cfg = (
            sites_cfg
            if sites_cfg
            else {
                "whitelist_sites": None,
                "blacklist_sites": None,
                "regex_sites": None,
            }
        )
        self.append_parents = append_parents
        self.get_samples(self.cfg["files"])

    # Function to build the dataset dictionary
    def get_samples(self, files):
        for scfg in files:
            if 'part' in scfg['metadata']:
                sname = f"{self.name}_{scfg['metadata']['part']}_{scfg['metadata']['year']}"
            else:
                sname = f"{self.name}_{scfg['metadata']['year']}"
            if not scfg["metadata"]["isMC"]:
                sname += f"_Era{scfg['metadata']['era']}"
            if "dbs_instance" in scfg.keys():
                kwargs = {"dbs_instance": scfg['dbs_instance']}
            else:
                kwargs = {}
            sample = Sample(
                name=sname,
                das_names=scfg["das_names"],
                sample=self.sample,
                metadata=scfg["metadata"],
                sites_cfg=self.sites_cfg,
                **kwargs,
            )
            self.samples_obj.append(sample)

            # Save redirector
            self.sample_dict_redirector.update(sample.get_sample_dict(redirector=True))
            # Save concrete
            self.sample_dict_concrete.update(sample.get_sample_dict(redirector=False))

            if self.prefix:
                #  If a storage prefix is specified, save also a local storage file
                self.sample_dict_local.update(
                    sample.get_sample_dict(redirector=True, prefix=self.prefix)
                )

            if self.append_parents:
                parents_names = sample.get_parentlist()
                scfg["das_parents_names"] = parents_names

    def _write_dataset(self, outfile, sample_dict, append=True, overwrite=False):
        print(f"Saving datasets {self.name} to {outfile}")
        if append and os.path.exists(outfile):
            # Update the same json file
            previous = json.load(open(outfile))
            if overwrite:
                previous.update(sample_dict)
                sample_dict = previous
            else:
                for k, v in sample_dict.items():
                    if k in previous:
                        raise Exception(
                            f"Sample {k} already present in file {outfile}, not overwriting!"
                        )
                    else:
                        previous[k] = v
                        sample_dict = previous
        with open(outfile, 'w') as fp:
            json.dump(sample_dict, fp, indent=4)
            fp.close()

    # Function to save the dataset dictionary with xrootd and local prefixes
    def save(self, append=True, overwrite=False, split=False):
        for outfile, sample_dict in zip(
            [
                self.outfile,
                self.outfile.replace('.json', '_redirector.json'),
                self.outfile.replace('.json', '_local.json'),
            ],
            [
                self.sample_dict_concrete,
                self.sample_dict_redirector,
                self.sample_dict_local,
            ],
        ):
            if not sample_dict:
                continue  # it is empty
            if not split:
                self._write_dataset(outfile, sample_dict, append, overwrite)
            else:

                samples_byyear = defaultdict(dict)
                for k, v in sample_dict.items():
                    samples_byyear[v["metadata"]["year"]][k] = v

                for year, sd in samples_byyear.items():
                    self._write_dataset(
                        outfile.replace(".json", f"_{year}.json"),
                        sd,
                        append,
                        overwrite,
                    )

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
        out_dict = {}  # Output filename list
        run_futures = []  # Future list
        for key in sorted(self.sample_dict.keys()):
            new_list = []
            if isinstance(self.sample_dict[key], dict):
                filelist = self.sample_dict[key]['files']
            elif isinstance(self.sample_dict[key], list):
                filelist = self.sample_dict[key]
            else:
                raise NotImplemented
            for i, fname in enumerate(filelist):
                if i % 5 == 0:
                    # print some progress info
                    ith = f'{key}: {i}/{len(filelist)}'
                else:
                    ith = None
                out = os.path.join(
                    os.path.abspath(self.prefix), fname.split("//")[-1].lstrip("/")
                )
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
