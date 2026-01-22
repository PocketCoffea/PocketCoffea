import os
from coffea.util import load, save
import click
from rich import print
from pocket_coffea.utils.filter_output import filter_output_by_year, filter_output_by_category

def split_output(inputfile, outputfile, by, ncategory_per_file, overwrite):
    '''Split coffea output files'''
    if outputfile is None:
        outputfile = inputfile.replace(".coffea", "_{}.coffea")
    else:
        outputfile = outputfile.replace(".coffea", "_{}.coffea")
    print(f"[blue]Reading input file: {inputfile}")
    out_all = load(inputfile)

    if by == "year":
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

    elif by == "category" or by == "categories":
        allcategories = sorted(list(out_all["sumw"].keys()))
        ncats = len(allcategories)
        categoryblocks = [allcategories[i:i+ncategory_per_file] for i in range(0, ncats, ncategory_per_file)]
        print(f"[blue]Splitting output by category: into {len(categoryblocks)} files, each with {ncategory_per_file} categories, totalling {ncats} categories.[/]")
        for ib, block in enumerate(categoryblocks):
            out = filter_output_by_category(out_all, block)
            thisoutput = outputfile.format(f"category{ib}")
            if os.path.exists(thisoutput) and not overwrite:
                raise FileExistsError(f"Output file {thisoutput} already exists. Use --overwrite to overwrite the output files.")
            save(out, thisoutput)
            print(f"[green]Output {ib+1}/{len(categoryblocks)} saved to {thisoutput}")

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
    "-b",
    "--by",
    required=False,
    default="year",
    type=str,
    help="Split by year or by category",
)
@click.option(
    "-n",
    "--ncategory-per-file",
    required=False,
    default=8,
    type=int,
    help="If splitting by category, specify how many categories go into one output.",
)
@click.option(
    "--overwrite",
    is_flag=True,
    help="Overwrite output file if it already exists",
)

def main(inputfile, outputfile, by, ncategory_per_file, overwrite):
    '''Split coffea output files'''
    split_output(inputfile, outputfile, by, ncategory_per_file, overwrite)

if __name__ == "__main__":
    main()
