import click
from .config_template import *
from rich import print
from rich.prompt import Prompt
import os

def write_config(processor_class: str, datasets_list: list,
                   samples_list: list, years_list: list,
                   output_path: str):

    import pocket_coffea
    config = config_template.replace("{{PROCESSOR_CLASS}}", processor_class)
    config = config.replace("{{DATASETS_LIST}}", str(datasets_list))
    config = config.replace("{{SAMPLES_LIST}}", str(samples_list))
    config = config.replace("{{YEARS_LIST}}", str(years_list))
    config = config.replace("{{VERSION}}", pocket_coffea.__version__)
    workflow = worflow_template.replace("{{PROCESSOR_CLASS}}", processor_class)
    workflow = workflow.replace("{{VERSION}}", pocket_coffea.__version__)
    custom_cut_functions = custom_cut_functions_template.replace("{{VERSION}}", pocket_coffea.__version__)
    dataset_definition = dataset_definition_template.replace("{{VERSION}}", pocket_coffea.__version__)
    
    with open(f"{output_path}/config.py", "w") as f:
        f.write(config)
    with open(f"{output_path}/workflow.py", "w") as f:
        f.write(workflow)
    with open(f"{output_path}/custom_cut_functions.py", "w") as f:
        f.write(custom_cut_functions)
    with open(f"{output_path}/dataset_definition.json", "w") as f:
        f.write(dataset_definition)


@click.command()
@click.option(
    "-o",
    "--output_path",
    required=True,
    type=str,
    help="Output folder for config files",
)
def make_config(output_path):
    '''
    Create a template for an analysis configuration with PocketCoffea
    '''
    if os.path.exists(output_path):
        print(f"[red]The output path {output_path} already exists. Please provide a new path.")
        return
    os.makedirs(output_path)
    
    # Ask questions to the user using Rich
    processor_class = Prompt.ask("Enter the processor class name: ")
    datasets_list = Prompt.ask("Enter the list of datasets: ", default="").split(",")
    samples_list = Prompt.ask("Enter the list of samples: ", default="").split(",")
    years_list = Prompt.ask("Enter the list of years: ", default="").split(",")
    write_config(processor_class, datasets_list, samples_list, years_list, output_path)
    print(f"[green]Config files have been written to {output_path}")
    print("Next steps: ")
    print(f"1. Edit the dataset definition file in {output_path}/dataset_definition.json, or use the dataset discovery command to generate it")
    print(f"2. Edit the workflow.py file in {output_path} to adjust your object preselections. ")
    print(f"3. Edit the custom_cut_functions.py file in {output_path} to add custom cut functions")
    print(f"4. Edit the config.py file in {output_path} to adjust the analysis configuration: datasets, cuts, categories, histograms, columns output, etc.")
    print(f"5. Run the processor using the command: pocket_coffea run --cfg {output_path}/config.py -o outpout --test")
    
if __name__ == "__main__":
    make_config()
    

