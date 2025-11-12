   
from rich.table import Table
from rich.console import Console
import time

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
