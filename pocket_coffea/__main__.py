import click
from rich import print
import pocket_coffea
from pocket_coffea.scripts.runner import run
from pocket_coffea.scripts.dataset.build_datasets import build_datasets
from pocket_coffea.scripts.dataset.dataset_query import dataset_discovery_cli
from pocket_coffea.scripts.plot.make_plots import make_plots
from pocket_coffea.scripts.hadd_skimmed_files import hadd_skimmed_files
from pocket_coffea.scripts import merge_outputs
from pocket_coffea.scripts import split_output
from pocket_coffea.scripts.print_parameters import print_parameters
from pocket_coffea.scripts.make_config import make_config
from pocket_coffea.scripts.check_jobs import check_jobs

title = """[dodger_blue1]
    ____             __        __  ______      ________
   / __ \____  _____/ /_____  / /_/ ____/___  / __/ __/__  ____ _
  / /_/ / __ \/ ___/ //_/ _ \/ __/ /   / __ \/ /_/ /_/ _ \/ __ `/
 / ____/ /_/ / /__/ ,< /  __/ /_/ /___/ /_/ / __/ __/  __/ /_/ /
/_/    \____/\___/_/|_|\___/\__/\____/\____/_/ /_/  \___/\__,_/

"""

@click.group(invoke_without_command=True)
@click.pass_context
@click.option("-v","--version", default=False, help="Print PocketCoffea package version")
def cli(ctx, version):
    print(title)

    if ctx.invoked_subcommand is None:
        print(f"Running PocketCoffea version {pocket_coffea.__version__}")
        print(f"- Documentation page:  https://pocketcoffea.readthedocs.io/")
        print(f"- Repository:          https://github.com/PocketCoffea/PocketCoffea")
        print("\nRun with [italic]--help[/] option for the list of available commands ")
    if version:
        print(f"PocketCoffea version: {pocket_coffea.__version__}")
    pass

cli.add_command(make_config)
cli.add_command(build_datasets)
cli.add_command(dataset_discovery_cli)
cli.add_command(run)
cli.add_command(make_plots)
cli.add_command(hadd_skimmed_files)
cli.add_command(merge_outputs.main, name="merge-outputs")
cli.add_command(split_output.main, name="split-output")
cli.add_command(print_parameters)
cli.add_command(check_jobs)


if __name__ == '__main__':
    cli()
