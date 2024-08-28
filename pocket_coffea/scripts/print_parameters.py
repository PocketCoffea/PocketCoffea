import click
from omegaconf import OmegaConf
from rich.pretty import pprint
from rich import print
from rich.prompt import Prompt
import yaml

from pocket_coffea.utils.utils import load_config
from pocket_coffea.parameters import defaults

@click.command()
@click.option(
    '-c',
    '--cfg',
    type=str,
    required=False,
    help='Config file',
)
@click.option(
    "-d",
    "--dump",
    type=str,
    help="Dump the configuration to a file",
)
@click.option(
    "-lk",
    "--list-keys",
    is_flag=True,
    help="List all the keys in the configuration fragment"
    )
# optional argument to be used for filtering the parameters to print
@click.argument(
    'key',
    required=False,
    type=str,
)
@click.option(
    "--cli",
    is_flag=True,
    help="Run interactively",
    )

def print_parameters(cfg, dump, list_keys,  key, cli):
    '''Print the parameters from the PocketCoffea configuration'''
    if cfg is not None:
        cfg = load_config(cfg, do_load=False, save_config=False)
        params_dict = cfg.parameters
    else:
        # Take the default parameters
        print("[red]No configuration file provided, using the [bold]DEFAULT[/bold] parameters[/]")
        params_dict = defaults.get_default_parameters()
    
    if key is not None:
        try:
            params_dict = OmegaConf.select(params_dict, key, throw_on_missing=True)
        except:
            print(f"[red]Key {key} not found in the configuration[/]")
            return
        if key is not None:
            print(f"[blue]Printing parameters for key: [/]{key}")

    if list_keys:
        print(f"[blue]Listing all the keys in the configuration[/]")
        pprint(list(params_dict.keys()))

    elif cli:
        if key is not None:
            #print the key that was asked
            pprint(OmegaConf.to_container(params_dict, resolve=True), indent_guides=True)

        print(f"Available keys in the configuration")
        pprint(list(params_dict.keys()))
        while True:
            # Printing available keys
            kkey = Prompt.ask("[bold cyan]Enter the key to print the parameters (or 'q' to quit)[/]")
            if kkey == 'q':
                break
            try:
                selected_params_dict = OmegaConf.select(params_dict, kkey, throw_on_missing=True)
                pprint(OmegaConf.to_container(selected_params_dict, resolve=True), indent_guides=True)
                # write available subkeys
                print(f"[yellow]Available sub-keys in the configuration[/]")
                pprint([f"{kkey}.{sub}" for sub in selected_params_dict.keys()])
            except:
                print(f"[red]Key {kkey} not found in the configuration[/]")
    else:
        #just print what has been selected
        pprint(OmegaConf.to_container(params_dict, resolve=True), indent_guides=True)
        
    if dump:
        with open(dump, "w") as f:
            yaml.dump(OmegaConf.to_container(params_dict, resolve=True),
                      f,
                      indent=2,)
            print(f"[green]Parameters saved to {dump}[/]")


if __name__ == "__main__":
    print_parameters()
