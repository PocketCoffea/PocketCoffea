import sys
from pprint import pprint
from pocket_coffea.utils.output_split import load_output_auto


def load_output(file):
    # Auto-detects the on-disk format: returns the classic monolithic dict for
    # either a monolithic .coffea or a split-format file.
    return load_output_auto(file)


if __name__ == "__main__":
    out = load_output(sys.argv[1])
    pprint(out)
