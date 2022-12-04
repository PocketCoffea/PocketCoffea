from coffea.util import load, save
from coffea.processor import accumulate
import argparse

parser = argparse.ArgumentParser(description='Accumulate coffea outputs')
# Inputs
parser.add_argument('-i','--inputfiles', required=True, type=str, nargs="+",
                    help='List of coffea input files')
parser.add_argument("-o", "--outputfile", required=True, type=str,
                    help="Output file")
args = parser.parse_args()

files = list(set([f for f in args.inputfiles]))

out = accumulate([ load(f) for f in files])
save(out, args.outputfile)
