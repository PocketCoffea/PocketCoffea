#!/usr/bin/env python

from coffea.util import load, save
from coffea.processor import accumulate 
import argparse

parser = argparse.ArgumentParser(description='Merge datasets from different coffea outputs')
# Inputs
parser.add_argument('-i','--inputfiles', required=True, type=str, nargs="+",
                    help='List of coffea input files')
parser.add_argument("-o", "--outputfile", required=True, type=str,
                    help="Output file")
args = parser.parse_args()

files = [load(f) for f in args.inputfiles]

out = accumulate(files)
save(out, args.outputfile)
