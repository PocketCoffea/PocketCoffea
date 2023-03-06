from coffea.util import load, save
from pocket_coffea.utils.accumulate import get_joint_accumulator
import argparse

parser = argparse.ArgumentParser(description='Merge datasets from different coffea outputs')
# Inputs
parser.add_argument('-i','--inputfiles', required=True, type=str, nargs="+",
                    help='List of coffea input files')
parser.add_argument("-o", "--outputfile", required=True, type=str,
                    help="Output file")
parser.add_argument("-d", "--dataset", required=True, type=str,
                    help="Joint dataset name")
args = parser.parse_args()

files = list(set([f for f in args.inputfiles]))

out = get_joint_accumulator(files, args.dataset)
save(out, args.outputfile)
