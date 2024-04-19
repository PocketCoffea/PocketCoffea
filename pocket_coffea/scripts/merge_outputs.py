from coffea.util import load, save
from coffea.processor import accumulate 
import click

"""
@click.command()
@click.option(
    '-i',
    '--inputfiles',
    required=True,
    type=str,
    multiple=True,
    help='List of coffea input files',
)
@click.option(
    "-o",
    "--outputfile",
    required=True,
    type=str,
    help="Output file",
)
"""

import argparse
parser = argparse.ArgumentParser(description='Merge datasets from different coffea outputs')
# Inputs
parser.add_argument('-i','--inputfiles', required=True, type=str, nargs="+",
                    help='List of coffea input files')
parser.add_argument("-o", "--outputfile", required=True, type=str,
                    help="Output file")
args = parser.parse_args()

def merge_outputs(inputfiles, outputfile):
    files = [load(f) for f in inputfiles]

    out = accumulate(files)
    save(out, outputfile)


if __name__ == "__main__":
    merge_outputs(args.inputfiles, args.outputfile)
