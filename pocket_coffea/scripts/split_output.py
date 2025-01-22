import os
from coffea.util import load, save
import click
from rich import print
from pocket_coffea.utils.filter_output import filter_output_by_year

def split_output(inputfile, outputfile, overwrite):
    '''Split coffea output files'''
    if outputfile is None:
        outputfile = inputfile.replace(".coffea", "_{}.coffea")
    else:
        outputfile = outputfile.replace(".coffea", "_{}.coffea")
    print(f"[blue]Reading input file: {inputfile}")
    out_all = load(inputfile)
    years = list(out_all["datasets_metadata"]["by_datataking_period"].keys())
    outputfiles = {year : outputfile.format(year) for year in years}
    print(f"[blue]Splitting output by year. {len(years)} output files will be saved:[/]")
    print(sorted(outputfiles.values()))
    for year, outputfile in outputfiles.items():
        out = filter_output_by_year(out_all, year)
        if os.path.exists(outputfile) and not overwrite:
            raise FileExistsError(f"Output file {outputfile} already exists. Use --overwrite to overwrite the output files.")
        save(out, outputfile)
        print(f"[green]Output saved to {outputfile}")

@click.command()
@click.argument(
    'inputfile',
    required=True,
    type=str,
    nargs=1,
)
@click.option(
    "-o",
    "--outputfile",
    required=False,
    default=None,
    type=str,
    help="Output file",
)
@click.option(
    "--overwrite",
    is_flag=True,
    help="Overwrite output file if it already exists",
)

def main(inputfile, outputfile, overwrite):
    '''Split coffea output files'''
    split_output(inputfile, outputfile, overwrite)

if __name__ == "__main__":
    main()
