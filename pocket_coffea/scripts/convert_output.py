"""Convert PocketCoffea outputs between the monolithic and split formats.

The classic ``.coffea`` output is a single monolithic ``cloudpickle`` blob that
must be fully loaded into memory to read anything. The *split* format
(:mod:`pocket_coffea.utils.output_split`) stores each variable as its own
member so the plotter can load one variable at a time. This CLI converts
between the two, and can merge several monolithic outputs directly into one
split file.

Examples
--------
    # monolithic -> split (default)
    convert-output out_split.coffea -i output_all.coffea

    # split -> monolithic
    convert-output out_mono.coffea -i out_split.coffea --to monolithic

    # merge many per-dataset monolithic outputs into ONE split file
    convert-output out_split.coffea -i "output_*.coffea" -n 5 -m 8
"""

import os
from glob import glob

import click
from rich import print

from pocket_coffea.utils.output_split import (
    save_split,
    is_split_output,
    to_monolithic,
)


def _expand(inputfiles):
    files = []
    for pattern in inputfiles:
        matched = glob(pattern)
        if matched:
            files.extend(sorted(f for f in matched if os.path.isfile(f)))
        elif os.path.isfile(pattern):
            files.append(pattern)
    return files


def convert_output(inputfiles, outputfile, target_format="split", compression="lz4",
                   reduction=5, max_mem_gb=None, cache_dir=None, force=False, verbose=False):
    """Convert/merge ``inputfiles`` into ``outputfile`` in ``target_format``."""
    files = _expand(inputfiles)
    if not files:
        raise SystemExit(f"No valid input files found in {list(inputfiles)}.")
    if os.path.exists(outputfile) and not force:
        raise SystemExit(f"Output file {outputfile} already exists. Use -f to overwrite.")

    if len(files) == 1:
        print(f"[pink]Loading {files[0]}[/]")
        data = to_monolithic(files[0])
    else:
        print(f"[blue]Merging {len(files)} files into one {target_format} output[/]")
        if any(is_split_output(f) for f in files):
            # mixed/split inputs: simple incremental accumulate (+drop)
            from coffea.processor import accumulate
            data = None
            for f in files:
                print(f"[pink]Loading {f}[/]")
                part = to_monolithic(f)
                data = part if data is None else accumulate([data, part])
                del part
        else:
            # all monolithic: reuse the memory-batched merge (dumps to disk if huge)
            from pocket_coffea.scripts.merge_outputs import merge_group_reduction
            if cache_dir is None:
                cache_dir = os.path.join(
                    os.path.dirname(os.path.abspath(outputfile)), "convert_cache")
            data = merge_group_reduction(files, N_reduction=reduction, cachedir=cache_dir,
                                         max_mem_gb=max_mem_gb, verbose=verbose)

    if target_format == "split":
        save_split(data, outputfile, compression=compression)
    else:
        from coffea.util import save
        save(data, outputfile)
    print(f"[green]Wrote {target_format} output to {outputfile}[/]")
    return outputfile


@click.command()
@click.argument("outputfile", type=str)
@click.option("-i", "--inputfiles", type=str, multiple=True, required=True,
              help="Input .coffea file(s) or glob pattern(s). Multiple inputs are merged.")
@click.option("--to", "target_format", type=click.Choice(["split", "monolithic"]),
              default="split", help="Output format (default: split).")
@click.option("--compression", type=click.Choice(["lz4", "none"]), default="lz4",
              help="Per-member compression for the split format.")
@click.option("-n", "--reduction", type=int, default=5,
              help="Files accumulated at a time when merging multiple monolithic inputs.")
@click.option("-m", "--max-mem-gb", type=float, default=None,
              help="Max memory (GB) before intermediate merge results are dumped to disk.")
@click.option("-cd", "--cache-dir", type=str, default=None, help="Cache dir for intermediate dumps.")
@click.option("-f", "--force", is_flag=True, help="Overwrite the output file if it exists.")
@click.option("-v", "--verbose", is_flag=True, help="Verbose memory reporting when merging.")
def main(outputfile, inputfiles, target_format, compression, reduction, max_mem_gb,
         cache_dir, force, verbose):
    """Convert a coffea output between the monolithic and split formats."""
    convert_output(inputfiles, outputfile, target_format=target_format, compression=compression,
                   reduction=reduction, max_mem_gb=max_mem_gb, cache_dir=cache_dir,
                   force=force, verbose=verbose)


if __name__ == "__main__":
    main()
