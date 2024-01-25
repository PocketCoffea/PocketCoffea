from coffea.util import load, save
from coffea.processor import accumulate 
import click


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
def merge_outputs(inputfiles, outputfile):
    files = [load(f) for f in inputfiles]

    out = accumulate(files)
    save(out, outputfile)


if __name__ == "__main__":
    merge_outputs()
