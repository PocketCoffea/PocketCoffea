import os
import sys
import json
import importlib.util

from parameters.allhistograms import histogram_settings

class Sample():
    def __init__(self, path, prefix):
        self.prefix = prefix
        self.path = path
        self.sample_dict = {}
        self.load_attributes()
        self.get_filelist()
        self.build_sample_dict()

    def load_attributes(self):
        self.sample = self.path.split('/')[1].split('_')[0]
        self.year = '20' + self.path.split('/')[2].split('UL')[1][:2]
        if self.year not in ['2016', '2017', '2018']:
            sys.exit(f"No dataset available for year '{self.year}'")
        self.name = self.sample + '_' + self.year

    def get_filelist(self):
        command = f'dasgoclient -query="file dataset={self.path}"'
        self.filelist = os.popen(command).read().split('\n')
        self.filelist = [os.path.join(self.prefix, *file.split('/')) for file in self.filelist if file != '']

    def build_sample_dict(self):
        self.sample_dict[self.name] = {}
        self.sample_dict[self.name]['metadata'] = {'sample' : self.sample, 'year' : self.year}
        self.sample_dict[self.name]['files']    = self.filelist

    def Print(self):
        print(f"path: {self.path}, sample: {self.sample}, year: {self.year}")

class Dataset():
    def __init__(self, file, prefix):
        self.prefix = prefix
        self.sample_dict = {}
        with open(file, 'r') as f:
            self.samples = f.read().splitlines()
        self.get_samples()

    def get_samples(self):
        for name in self.samples:
            sample = Sample(name, self.prefix)
            self.sample_dict.update(sample.sample_dict)

    def save(self, outfile, local=True):
        print(f"Saving datasets to {outfile}")
        with open(outfile, 'w') as fp:
            json.dump(self.sample_dict, fp, indent=4)

