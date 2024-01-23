import click
from pocket_coffea.scripts.runner import run
from pocket_coffea.scripts.dataset.build_datasets import build_datasets
from pocket_coffea.scripts.plot.make_plots import make_plots

title = """
    ____             __        __  ______      ________
   / __ \____  _____/ /_____  / /_/ ____/___  / __/ __/__  ____ _
  / /_/ / __ \/ ___/ //_/ _ \/ __/ /   / __ \/ /_/ /_/ _ \/ __ `/
 / ____/ /_/ / /__/ ,< /  __/ /_/ /___/ /_/ / __/ __/  __/ /_/ /
/_/    \____/\___/_/|_|\___/\__/\____/\____/_/ /_/  \___/\__,_/

"""

class CustomGroup(click.Group):
    def invoke(self, ctx):
        click.echo(title)
        super().invoke(ctx)


@click.group(cls=CustomGroup)
def cli():
    pass

cli.add_command(build_datasets)
cli.add_command(run)
cli.add_command(make_plots)

if __name__ == '__main__':
    cli()
