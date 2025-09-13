
import argparse
import os
import multiprocessing
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from rich import print
from rich.progress import track

def merge_leaf_dir(input_dir, output_file, force):
    # Skip if output exists and not forcing
    if os.path.exists(output_file) and not force:
        print(f"[yellow][Skipped] Output exists: {output_file}[/]")
        return

    # List parquet files that are non-empty
    parquet_files = [
        f for f in os.listdir(input_dir)
        if f.endswith(".parquet") and os.stat(os.path.join(input_dir, f)).st_size > 0
    ]

    if not parquet_files:
        print(f"[red][Skipped] No non-empty parquet files in {input_dir}[/]")
        return

    dataset = ds.dataset([os.path.join(input_dir, f) for f in parquet_files], format="parquet")
    table = dataset.to_table()
    pq.write_table(table, output_file)
    # print(f"[green][Merged] {input_dir} -> {output_file}[/]")

def find_leaf_dirs(root_input, root_output):
    """Return list of (leaf_dir, output_file) pairs."""
    tasks = []
    for current_dir, dirs, files in os.walk(root_input):
        if not dirs:  # leaf dir
            parquet_files = [f for f in files if f.endswith(".parquet")]
            if parquet_files:
                rel_path = os.path.relpath(current_dir, start=root_input)
                output_file = os.path.join(root_output, f"{rel_path}.parquet")
                os.makedirs(os.path.dirname(output_file), exist_ok=True)
                tasks.append((current_dir, output_file))
    return tasks

def worker(task):
    """Wrapper for multiprocessing (task is a tuple: (input_dir, output_file, force))"""
    return merge_leaf_dir(task[0], task[1], task[2])

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge parquet leaves into single files")
    parser.add_argument("-o", "--output-dir", required=True, help="Input directory containing parquet tree")
    parser.add_argument("-j", "--jobs", type=int, default=None, help="Number of parallel jobs (default: all CPUs)")
    parser.add_argument("-f", "--force", action="store_true", help="Overwrite existing output files")
    args = parser.parse_args()

    root_input = os.path.abspath(args.output_dir)
    root_output = root_input.rstrip(os.sep) + "_merged"
    os.makedirs(root_output, exist_ok=True)

    # Collect all leaf directories
    tasks = [(inp, out, args.force) for inp, out in find_leaf_dirs(root_input, root_output)]

    # Merge with progress bar
    with multiprocessing.Pool(processes=args.jobs) as pool:
        for _ in track(pool.imap_unordered(worker, tasks), total=len(tasks), description="[cyan]Merging parquet leaves..."):
            pass

    print("[green][b]Done![/][/] ✅")
