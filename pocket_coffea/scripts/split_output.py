import os
from coffea.util import save
import click
from rich import print
from pocket_coffea.utils.filter_output import filter_output_by_year, filter_output_by_category
from pocket_coffea.utils.output_split import load_output_auto, save_split


def _save_output(data, path, output_format="monolithic"):
    """Save an output dict in the requested format.

    - "monolithic": the classic single-blob coffea file (coffea.util.save).
    - "split": the per-variable low-memory format (output_split.save_split) that
      plotting can load one variable at a time.
    """
    if output_format == "split":
        save_split(data, path)
    else:
        save(data, path)


def split_output(inputfile, outputfile, by, ncategory_per_file, overwrite, output_format="monolithic"):
    '''Split coffea output files'''
    if outputfile is None:
        outputfile = inputfile.replace(".coffea", "_{}.coffea")
    else:
        outputfile = outputfile.replace(".coffea", "_{}.coffea")
    print(f"[blue]Reading input file: {inputfile}")
    # load_output_auto transparently reads either the monolithic or the split
    # format, so a file already written in the low-memory split format can also
    # be re-split here.
    out_all = load_output_auto(inputfile)

    if by == "year":
        years = list(out_all["datasets_metadata"]["by_datataking_period"].keys())
        outputfiles = {year : outputfile.format(year) for year in years}
        print(f"[blue]Splitting output by year. {len(years)} output files will be saved ({output_format} format):[/]")
        print(sorted(outputfiles.values()))
        for year, outputfile in outputfiles.items():
            out = filter_output_by_year(out_all, year)
            if os.path.exists(outputfile) and not overwrite:
                raise FileExistsError(f"Output file {outputfile} already exists. Use --overwrite to overwrite the output files.")
            _save_output(out, outputfile, output_format)
            print(f"[green]Output saved to {outputfile}")

    elif by == "category" or by == "categories":
        allcategories = sorted(list(out_all["sumw"].keys()))
        ncats = len(allcategories)
        categoryblocks = [allcategories[i:i+ncategory_per_file] for i in range(0, ncats, ncategory_per_file)]
        print(f"[blue]Splitting output by category: into {len(categoryblocks)} files, each with {ncategory_per_file} categories, totalling {ncats} categories ({output_format} format).[/]")
        for ib, block in enumerate(categoryblocks):
            out = filter_output_by_category(out_all, block)
            thisoutput = outputfile.format(f"category{ib}")
            if os.path.exists(thisoutput) and not overwrite:
                raise FileExistsError(f"Output file {thisoutput} already exists. Use --overwrite to overwrite the output files.")
            _save_output(out, thisoutput, output_format)
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
    "--output-format",
    required=False,
    default="monolithic",
    type=click.Choice(["monolithic", "split"]),
    help="Output format: 'monolithic' (classic single-blob .coffea) or 'split' "
         "(per-variable low-memory format that plotting can stream one variable at a time).",
)
@click.option(
    "--overwrite",
    is_flag=True,
    help="Overwrite output file if it already exists",
)

def main(inputfile, outputfile, by, ncategory_per_file, output_format, overwrite):
    '''Split coffea output files'''
    split_output(inputfile, outputfile, by, ncategory_per_file, overwrite, output_format)

if __name__ == "__main__":
    main()
