from coffea.util import load, save
from coffea.processor import accumulate 
import click
from rich import print

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
    print(inputfiles)
    out = accumulate([load(f) for f in inputfiles])
    save(out, outputfile)
    print(f"[green]Output saved to {outputfile}")

if __name__ == "__main__":
    merge_outputs()
