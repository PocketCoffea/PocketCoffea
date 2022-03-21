import os
import sys
import json
import importlib.util

from parameters.allhistograms import histogram_settings

class Dataset():
    def __init__(self, name):
        # Load config file and attributes
        self.name = name
        self.load_attributes()
        self.get_filelist()

    def load_attributes(self):
        self.sample = self.name.split('/')[1].split('_')[0]
        self.year = '20' + self.name.split('/')[2].split('UL')[1][:2]
        if self.year not in ['2016', '2017', '2018']:
            sys.exit(f"No dataset available for year '{self.year}'")

    def get_filelist(self):
        self.filelist = os.popen(f'dasgoclient -query="file dataset={self.name}"').read().split('\n')
        self.filelist = [file for file in self.filelist if file != '']

    def Print(self):
        print(f"name: {self.name}, sample: {self.sample}, year: {self.year}")
