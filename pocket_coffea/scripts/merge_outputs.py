from coffea.util import load, save
from coffea.processor import accumulate 
import click
from rich import print
from pocket_coffea.utils.filter_output import compare_dict_types

@click.command()
@click.argument(
    'inputfiles',
    required=True,
    type=str,
    nargs=-1,
)
@click.option(
    "-o",
    "--outputfile",
    required=True,
    type=str,
    help="Output file",
)

def merge_outputs(inputfiles, outputfile):
    '''Merge coffea output files'''
    print(f"[blue]Merging files into {outputfile}[/]")
    print(sorted(inputfiles))
    type_mismatches = []
    f0 = inputfiles[0]
    for f in inputfiles[1:]:
        print(f"[green]Comparing {f0} and {f}")
        type_mismatch_found = compare_dict_types(load(f0), load(f))
        type_mismatches.append(type_mismatch_found)
    if any(type_mismatches):
        raise TypeError("Type mismatch found between the values of the input dictionaries. Please check the input files.")

    out = accumulate([load(f) for f in inputfiles])
    save(out, outputfile)
    print(f"[green]Output saved to {outputfile}")

if __name__ == "__main__":
    merge_outputs()
