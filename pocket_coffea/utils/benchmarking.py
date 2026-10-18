import json
import math
import os
import time
from pathlib import Path
from tempfile import NamedTemporaryFile

from rich.console import Console
from rich.table import Table


def timeit_dir(config_file):
    return Path(config_file).resolve().parent / "timeit"


def load_sample_throughputs(path):
    path = Path(path)
    if not path.exists():
        return {}
    if not path.is_dir():
        raise ValueError(f"{path} must be a timeit directory")
    throughputs = {}
    for timeit_file in sorted(path.glob("*.json")):
        data = json.loads(timeit_file.read_text())
        if not isinstance(data, dict):
            raise ValueError(f"{timeit_file} must contain a JSON object mapping datasets to events/s")
        for dataset, rate in data.items():
            if (isinstance(rate, bool) or not isinstance(rate, (int, float))
                    or not math.isfinite(rate) or rate <= 0):
                raise ValueError(f"Invalid throughput for dataset {dataset!r} in {timeit_file}: {rate!r}")
            throughputs[str(dataset)] = float(rate)
    return throughputs


def add_sample_processing_stats(stats, output, filesets):
    for dataset, events in output["cutflow"]["initial"].items():
        stats.setdefault(dataset, [0, 0.0])[0] += int(events)
    for dataset, seconds in output.get("processing_time", {}).items():
        stats.setdefault(dataset, [0, 0.0])[1] += float(seconds)


def write_sample_throughputs(path, stats):
    rates = {dataset: events / seconds for dataset, (events, seconds) in stats.items()
             if events > 0 and seconds > 0}
    if not rates:
        raise ValueError("No successful per-dataset timing measurements were produced")

    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    for dataset, rate in rates.items():
        output = path / f"{dataset}.json"
        tmp_name = None
        try:
            with NamedTemporaryFile("w", dir=path, prefix=f".{output.name}.", delete=False) as tmp:
                json.dump({dataset: rate}, tmp, indent=2, sort_keys=True)
                tmp.write("\n")
                tmp_name = tmp.name
            os.replace(tmp_name, output)
        finally:
            if tmp_name:
                Path(tmp_name).unlink(missing_ok=True)
    return rates


def print_sample_processing_stats(stats):
    table = Table(title="Per-dataset processing throughput")
    table.add_column("Dataset", style="cyan")
    table.add_column("Events", justify="right", style="green")
    table.add_column("Processor time (s)", justify="right")
    table.add_column("Events/s/worker", justify="right", style="yellow")
    for dataset, (events, seconds) in sorted(stats.items()):
        if seconds > 0:
            table.add_row(dataset, str(events), f"{seconds:.2f}", f"{events / seconds:.2f}")
    Console().print(table)


def print_processing_stats(output, start_time, workers):
    '''
    Prints processing statistics using rich.Table.
    '''
    stop_time = time.time()
    total_time = stop_time - start_time
    cutflow = output["cutflow"]
    tot_events_initial = sum([v for v in cutflow['initial'].values()])
    tot_events_skim = sum([v for v in cutflow['skim'].values()])
    tot_events_presel = sum([v["nominal"] for v in cutflow['presel'].values()])

    # Create a Table object
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Category", justify="right", style="cyan", no_wrap=True)
    table.add_column("Events", justify="right", style="green")
    table.add_column("Throughput (events/s)", justify="right", style="green")
    table.add_column("Throughput per Worker (events/s/worker)", justify="right", style="yellow")

    # Add rows for total, skimmed, and preselected events
    table.add_row("Total", str(tot_events_initial), f"{tot_events_initial/total_time:.2f}", f"{tot_events_initial/total_time/workers:.2f}")
    table.add_row("Skimmed", str(tot_events_skim), f"{tot_events_skim/total_time:.2f}", f"{tot_events_skim/total_time/workers:.2f}")
    table.add_row("Preselected", str(tot_events_presel), f"{tot_events_presel/total_time:.2f}", f"{tot_events_presel/total_time/workers:.2f}")

    # Create a Console object and print the table
    console = Console()
    console.print(f"Total processing time: {total_time/60.:.2f} minutes", style="bold blue")
    console.print(f"Number of workers: {workers}", style="bold blue")
    console.print(table)
