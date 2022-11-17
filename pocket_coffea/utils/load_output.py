import sys
from pprint import pprint
from coffea.util import load


def load_output(file):
    return load(file)


if __name__ == "__main__":
    out = load_output(sys.argv[1])
    pprint(out)
