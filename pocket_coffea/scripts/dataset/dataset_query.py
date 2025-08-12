import argparse
import gzip
import json
import os
import random
from collections import defaultdict
from typing import List
import copy
import yaml
import click
from rich import print
from rich.console import Console
from rich.prompt import Confirm, IntPrompt, Prompt
from rich.table import Table
from rich.tree import Tree
import re
from pocket_coffea.utils import rucio as rucio_utils
import requests
#from pocket_coffea.parameters.xsection import xsection
def print_dataset_query(query, dataset_list, console, selected=[]):
    table = Table(title=f"Query: [bold red]{query}")
    table.add_column("Name", justify="left", style="cyan", no_wrap=True)
    table.add_column("Tag", style="magenta", no_wrap=True)
    table.add_column("Selected", justify="center")
    table.row_styles = ["dim", "none"]
    j = 1
    for name, conds in dataset_list.items():
        ic = 0
        ncond = len(conds)
        for c, tiers in conds.items():
            dataset = f"/{name}/{c}/{tiers[0]}"
            sel = dataset in selected
            if ic == 0:
                table.add_row(
                    name,
                    f"[bold]({j})[/bold] {c}/{'-'.join(tiers)}",
                    "[green bold]Y" if sel else "[red]N",
                    end_section=ic == ncond - 1,
                )
            else:
                table.add_row(
                    "",
                    f"[bold]({j})[/bold] {c}/{'-'.join(tiers)}",
                    "[green bold]Y" if sel else "[red]N",
                    end_section=ic == ncond - 1,
                )
            ic += 1
            j += 1

    console.print(table)


def get_indices_query(input_str: str, maxN: int) -> List[int]:
    tokens = input_str.strip().split(" ")
    final_tokens = []
    for t in tokens:
        if t.isdigit():
            if int(t) > maxN:
                print(
                    f"[red bold]Requested index {t} larger than available elements {maxN}"
                )
                return False
            final_tokens.append(int(t) - 1)  # index 0
        elif "-" in t:
            rng = t.split("-")
            try:
                for i in range(
                    int(rng[0]), int(rng[1]) + 1
                ):  # including the last index
                    if i > maxN:
                        print(
                            f"[red bold]Requested index {t} larger than available elements {maxN}"
                        )
                        return False
                    final_tokens.append(i - 1)
            except Exception:
                print(
                    "[red]Error! Bad formatting for selection string. Use e.g. 1 4 5-9"
                )
                return False
        elif t == "all":
            final_tokens = list(range(0, maxN))
        else:
            print("[red]Error! Bad formatting for selection string. Use e.g. 1 4 5-9")
            return False
    return final_tokens


class DataDiscoveryCLI:
    def __init__(self):
        self.console = Console()
        self.rucio_client = None
        self.selected_datasets = []
        self.selected_datasets_metadata = []
        self.last_query = ""
        self.last_query_tree = None
        self.last_query_list = None
        self.sites_allowlist = None
        self.sites_blocklist = None
        self.sites_prioritylist = None
        self.sites_regex = None
        self.last_replicas_results = None
        self.final_output = None
        self.preprocessed_total = None
        self.preprocessed_available = None

        self.replica_results = defaultdict(list)
        self.replica_results_metadata = {}
        self.replica_results_bysite = {}
        self.sort_replicas: str = "geoip"

        self.commands = [
            "help",
            "login",
            "query",
            "query-results",
            "select",
            "list-selected",
            "replicas",
            "list-replicas",
            "set-sorting",
            "save",
            "clear",
            "allow-sites",
            "block-sites",
            "priority-sites",
            "regex-sites",
            "sites-filters",
            "quit",
        ]

    def start_cli(self):
        while True:
            command = Prompt.ask(">", choices=self.commands)
            if command == "help":
                print(
                    r"""[bold yellow]Welcome to the datasets discovery coffea CLI![/bold yellow]
Use this CLI tool to query the CMS datasets and to select interactively the grid sites to use for reading the files in your analysis.
Some basic commands:
  - [bold cyan]query[/]: Look for datasets with * wildcards (like in DAS)
  - [bold cyan]select[/]: Select datasets to process further from query results
  - [bold cyan]replicas[/]: Query rucio to look for files replica and then select the preferred sites
  - [bold cyan]query-results[/]: List the results of the last dataset query
  - [bold cyan]list-selected[/]: Print a list of the selected datasets
  - [bold cyan]list-replicas[/]: Print the selected files replicas for the selected dataset
  - [bold cyan][set-sorting][/]: Set the sorting mode for replicas
  - [bold cyan]sites-filters[/]: show the active sites filters and ask to clear them
  - [bold cyan]allow-sites[/]: Restrict the grid sites available for replicas query only to the requested list
  - [bold cyan]block-sites[/]: Exclude grid sites from the available sites for replicas query
  - [bold cyan]priority-sites[/]: Set priority for sites by which to order the replicas
  - [bold cyan]regex-sites[/]: Select sites with a regex for replica queries: e.g.  "T[123]_(FR|IT|BE|CH|DE)_\w+"
  - [bold cyan]save[/]: Save the selected datasets as a dataset definition file and also the replicas query results to file (json or yaml) for further processing
  - [bold cyan]clear[/]: Clear the selected datasets and replicas
  - [bold cyan]help[/]: Print this help message
            """
                )
            elif command == "login":
                self.do_login()
            elif command == "quit":
                print("Bye!")
                break
            elif command == "query":
                self.do_query()
            elif command == "query-results":
                self.do_query_results()
            elif command == "select":
                self.do_select()
            elif command == "list-selected":
                self.do_list_selected()
            elif command == "replicas":
                self.do_replicas()
            elif command == "list-replicas":
                self.do_list_replicas()
            elif command == "set-sorting":
                self.do_set_replicas_sorting()
            elif command == "save":
                self.do_save()
            elif command == "clear":
                self.do_clear()
            elif command == "allow-sites":
                self.do_allowlist_sites()
            elif command == "block-sites":
                self.do_blocklist_sites()
            elif command == "priority-sites":
                self.do_prioritylist_sites()
            elif command == "regex-sites":
                self.do_regex_sites()
            elif command == "sites-filters":
                self.do_sites_filters()
            else:
                break

    def do_login(self, proxy=None):
        """Login to the rucio client. Optionally a specific proxy file can be passed to the command.
        If the proxy file is not specified, `voms-proxy-info` is used"""
        if proxy:
            self.rucio_client = rucio_utils.get_rucio_client(proxy)
        else:
            self.rucio_client = rucio_utils.get_rucio_client()
        print(self.rucio_client)

    def do_whoami(self):
        # Your code here
        if not self.rucio_client:
            print("First [bold]login (L)[/] to the rucio server")
            return
        print(self.rucio_client.whoami())

    def do_query(self, query=None):
        # Your code here
        if query is None:
            query = Prompt.ask(
                "[yellow bold]Query for[/]",
            )
        with self.console.status(f"Querying rucio for: [bold red]{query}[/]"):
            outlist, outtree = rucio_utils.query_dataset(
                query,
                client=self.rucio_client,
                tree=True,
                scope="cms",  # TODO configure scope
            )
            # Now let's print the results as a tree
            print_dataset_query(query, outtree, self.console, self.selected_datasets)
            self.last_query = query
            self.last_query_list = outlist
            self.last_query_tree = outtree
        print("Use the command [bold red]select[/] to selected the datasets")

    def do_query_results(self):
        if self.last_query_list:
            print_dataset_query(
                self.last_query,
                self.last_query_tree,
                self.console,
                self.selected_datasets,
            )
        else:
            print("First [bold red]query (Q)[/] for a dataset")

    def generate_default_metadata(self, dataset):
        year = self.extract_year_from_dataset_name(dataset)
        isMC = self.is_mc_dataset(dataset)
        try:
            xsec = self.extract_xsec_from_dataset_name(dataset)
        except Exception as e:
            xsec = 1.0
        primary_dataset,year_data,era_data = self.extract_era_from_dataset_name(dataset)
        if isMC == True:
            return {
                "year": year,
                "isMC": isMC,
                "xsec": xsec
            }
        else:
            return {
                "year": year_data,
                "isMC": isMC,
                "primaryDataset": primary_dataset,
                "era": era_data
            }

    def extract_year_from_dataset_name(self, dataset_name):
        pattern = r'\/([^\/]+)NanoAOD'
        match = re.search(pattern, dataset_name)
        if not match:
            return ""
        if match.group(1) == 'RunIISummer20UL16NanoAODAPV':
            return '2016_PreVFP'
        elif match.group(1) == 'RunIISummer20UL16NanoAOD':
            return '2016_PostVFP'
        elif match.group(1) == 'RunIISummer20UL17':
            return '2017'
        elif match.group(1) == 'RunIISummer20UL18':
            return '2018'
        elif match.group(1) == 'Run3Summer22':
            return '2022_preEE'
        elif match.group(1) == 'Run3Summer22EE':
            return '2022_postEE'
        elif match.group(1) == 'Run3Summer23':
            return '2023_preBPix'
        elif match.group(1) == 'Run3Summer23BPix':
            return '2023_postBPix'
        else:
            return ""
    
    def extract_era_from_dataset_name(self, dataset_name):
        pattern = r'/([^/]+)/Run(\d{4})([A-Z])'
        match = re.search(pattern, dataset_name)
        
        if match:
            primary_dataset = match.group(1)
            year = match.group(2)
            era = match.group(3)
            return primary_dataset, year, era
        else:
            return "", "", ""
    
    def is_mc_dataset(self, dataset_name):
        parts = dataset_name.split('/')
        if len(parts) > 0 and 'SIM' in parts[-1]:
            return True
        else:
            return False

    def extract_xsec_from_dataset_name(self, dataset_name):
        parts = dataset_name.split('/')
        if len(parts) > 0:
            parts =  parts[1]
        url = 'https://xsdb-temp.app.cern.ch/api/search'
        response = requests.post(url, json={'process_name': parts})
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list) and len(data) > 0:
                return float(data[0]['cross_section'])
            else:
                raise ValueError(f"No data found for process_name '{parts}'")
        else:
            raise ConnectionError(f"Failed to fetch data for process_name '{parts}'. Status code: {response.status_code}")

    def do_select(self, selection=None, metadata=None):
        """Selected the datasets from the list of query results. Input a list of indices
        also with range 4-6 or "all"."""
        if not self.last_query_list:
            print("First [bold red]query (Q)[/] for a dataset")
            return

        if selection is None:
            selection = Prompt.ask(
                "[yellow bold]Select datasets indices[/] (e.g 1 4 6-10)", default="all"
            )
        final_tokens = get_indices_query(selection, len(self.last_query_list))
        if not final_tokens:
            return

        Nresults = len(self.last_query_list)
        print("[cyan]Selected datasets:")

        for s in final_tokens:
            if s < Nresults:
                self.selected_datasets.append(self.last_query_list[s])
                if metadata:
                    self.selected_datasets_metadata.append(metadata)
                else:
                    self.selected_datasets_metadata.append(self.generate_default_metadata(self.last_query_list[s]))
                    #self.selected_datasets_metadata.append({
                    #    "year": "",
                    #    "isMC": True,
                    #    "primaryDataset": "",
                    #    "part": "",
                    #    "era": "",
                    #    "xsec": 1.0
                    #})
                print(f"- ({s+1}) {self.last_query_list[s]}")
            else:
                print(
                    f"[red]The requested dataset is not in the list. Please insert a position <={Nresults}"
                )

    def do_list_selected(self):
        print("[cyan]Selected datasets:")
        table = Table(title="Selected datasets")
        table.add_column("Index", justify="left", style="cyan", no_wrap=True)
        table.add_column("Dataset", style="magenta", no_wrap=True)
        table.add_column("Replicas selected", justify="center")
        table.add_column("N. of files", justify="center")
        for i, ds in enumerate(self.selected_datasets):
            table.add_row(
                str(i + 1),
                ds,
                "[green bold]Y" if ds in self.replica_results else "[red]N",
                (
                    str(len(self.replica_results[ds]))
                    if ds in self.replica_results
                    else "-"
                ),
            )
        self.console.print(table)

    def do_replicas(self, mode=None, selection=None):
        """Query Rucio for replicas.
        mode: - None:  ask the user about the mode
              - round-robin (take files randomly from available sites),
              - choose: ask the user to choose from a list of sites
              - first: take the first site from the rucio query
        selection: list of indices or 'all' to select all the selected datasets for replicas query
        """
        if selection is None:
            selection = Prompt.ask(
                "[yellow bold]Select datasets indices[/] (e.g 1 4 6-10)", default="all"
            )
        indices = get_indices_query(selection, len(self.selected_datasets))
        if not indices:
            return
        datasets = [
            (self.selected_datasets[ind], self.selected_datasets_metadata[ind])
            for ind in indices
        ]

        for dataset, dataset_metadata in datasets:
            with self.console.status(
                f"Querying rucio for replicas: [bold red]{dataset}[/]"
            ):
                try:
                    (
                        outfiles,
                        outsites,
                        sites_counts,
                    ) = rucio_utils.get_dataset_files_replicas(
                        dataset,
                        allowlist_sites=self.sites_allowlist,
                        blocklist_sites=self.sites_blocklist,
                        prioritylist_sites=self.sites_prioritylist,
                        regex_sites=self.sites_regex,
                        mode="full",
                        client=self.rucio_client,
                        sort=self.sort_replicas,
                    )
                except Exception as e:
                    print(f"\n[red bold] Exception: {e}[/]")
                    return
                self.last_replicas_results = (outfiles, outsites, sites_counts)

            print(f"[cyan]Sites availability for dataset: [red]{dataset}")
            table = Table(title="Available replicas")
            table.add_column("Index", justify="center")
            table.add_column("Site", justify="left", style="cyan", no_wrap=True)
            table.add_column("Files", style="magenta", no_wrap=True)
            table.add_column("Availability", justify="center")
            table.row_styles = ["dim", "none"]
            Nfiles = len(outfiles)

            sorted_sites = dict(
                sorted(sites_counts.items(), key=lambda x: x[1], reverse=True)
            )
            for i, (site, stat) in enumerate(sorted_sites.items()):
                table.add_row(
                    str(i), site, f"{stat} / {Nfiles}", f"{stat*100/Nfiles:.1f}%"
                )

            self.console.print(table)
            if mode is None:
                mode = Prompt.ask(
                    "Select sites",
                    choices=["round-robin", "choose", "first", "quit"],
                    default="round-robin",
                )

            files_by_site = defaultdict(list)

            if mode == "choose":
                ind = list(
                    map(
                        int,
                        Prompt.ask(
                            "Enter list of sites index to be used", default="0"
                        ).split(" "),
                    )
                )
                sites_to_use = [list(sorted_sites.keys())[i] for i in ind]
                print(f"Filtering replicas with [green]: {' '.join(sites_to_use)}")

                output = []
                for ifile, (files, sites) in enumerate(zip(outfiles, outsites)):
                    random.shuffle(sites_to_use)
                    found = False
                    # loop on shuffled selected sites until one is found
                    for site in sites_to_use:
                        try:
                            iS = sites.index(site)
                            output.append(files[iS])
                            files_by_site[sites[iS]].append(files[iS])
                            found = True
                            break  # keep only one replica
                        except ValueError:
                            # if the index is not found just go to the next site
                            pass

                    if not found:
                        print(
                            f"[bold red]No replica found compatible with sites selection for file #{ifile}. The available sites are:"
                        )
                        for f, s in zip(files, sites):
                            print(f"\t- [green]{s} [cyan]{f}")
                        return

                self.replica_results[dataset] = output
                self.replica_results_metadata[dataset] = dataset_metadata

            elif mode == "round-robin":
                output = []
                for ifile, (files, sites) in enumerate(zip(outfiles, outsites)):
                    # selecting randomly from the sites
                    iS = random.randint(0, len(sites) - 1)
                    output.append(files[iS])
                    files_by_site[sites[iS]].append(files[iS])
                self.replica_results[dataset] = output
                self.replica_results_metadata[dataset] = dataset_metadata

            elif mode == "first":
                output = []
                for ifile, (files, sites) in enumerate(zip(outfiles, outsites)):
                    output.append(files[0])
                    files_by_site[sites[0]].append(files[0])
                self.replica_results[dataset] = output
                self.replica_results_metadata[dataset] = dataset_metadata

            elif mode == "quit":
                print("[orange]Doing nothing...")
                return

            self.replica_results_bysite[dataset] = files_by_site

            # Now let's print the results
            tree = Tree(label=f"[bold orange]Replicas for [green]{dataset}")
            for site, files in files_by_site.items():
                T = tree.add(f"[green]{site}")
                for f in files:
                    T.add(f"[cyan]{f}")
            self.console.print(tree)

        # Building an uproot compatible output
        self.final_output = {}
        for fileset, files in self.replica_results.items():
            self.final_output[fileset] = {
                "files": [f for f in files],
                "metadata": self.replica_results_metadata[fileset],
            }
        return self.final_output

    @property
    def as_dict(self):
        return self.final_output

    def do_allowlist_sites(self, sites=None):
        if sites is None:
            sites = Prompt.ask(
                "[yellow]Restrict the available sites to (comma-separated list)"
            ).split(",")
        if self.sites_allowlist is None:
            self.sites_allowlist = sites
        else:
            self.sites_allowlist += sites
        print("[green]Allowlisted sites:")
        for s in self.sites_allowlist:
            print(f"- {s}")

    def do_blocklist_sites(self, sites=None):
        if sites is None:
            sites = Prompt.ask(
                "[yellow]Exclude the sites (comma-separated list)"
            ).split(",")
        if self.sites_blocklist is None:
            self.sites_blocklist = sites
        else:
            self.sites_blocklist += sites
        print("[red]Blocklisted sites:")
        for s in self.sites_blocklist:
            print(f"- {s}")

    def do_prioritylist_sites(self, sites=None):
        """Choose prioritised sites by which to order replicas independent of location and availability."""
        if sites is None:
            sites = Prompt.ask(
                "[yellow]List to order the available sites by (space-separated list, overwrites previous priority)"
            ).split(" ")
        if self.sites_prioritylist is not None:
            print("[yellow]Overwriting priority order")
        self.sites_prioritylist = sites
        print("[green]Order of priority for available sites")
        if len(self.sites_prioritylist) > 0:
            for s in self.sites_prioritylist:
                print(f"- {s}")
        print("[yellow]Remember to set sorting to `priority`")

    def do_regex_sites(self, regex=None):
        if regex is None:
            regex = Prompt.ask("[yellow]Regex to restrict the available sites")
        if len(regex):
            self.sites_regex = rf"{regex}"
            print(f"New sites regex: [cyan]{self.sites_regex}")

    def do_sites_filters(self, ask_clear=True):
        print("[green bold]Allow-listed sites:")
        if self.sites_allowlist:
            for s in self.sites_allowlist:
                print(f"- {s}")

        print("[bold red]Block-listed sites:")
        if self.sites_blocklist:
            for s in self.sites_blocklist:
                print(f"- {s}")

        print("[bold green]Priority-listed sites:")
        if self.sites_prioritylist:
            for s in self.sites_prioritylist:
                print(f"- {s}")

        print(f"[bold cyan]Sites regex: [italics]{self.sites_regex}")

        print(f"[bold green]Sorting set to: {self.sort_replicas}")

        if ask_clear:
            if Confirm.ask("Clear sites restrinction?", default=False):
                self.sites_allowlist = None
                self.sites_blocklist = None
                self.sites_regex = None
                self.sites_prioritylist = None
                print("[bold green]Sites filters cleared")


    def do_list_replicas(self):
        selection = Prompt.ask(
            "[yellow bold]Select datasets indices[/] (e.g 1 4 6-10)", default="all"
        )
        indices = get_indices_query(selection, len(self.selected_datasets))
        datasets = [self.selected_datasets[ind] for ind in indices]

        for dataset in datasets:
            if dataset not in self.replica_results:
                print(
                    f"[red bold]No replica info for dataset {dataset}. You need to selected the replicas with [cyan] replicas [/cyan] command[/]"
                )
                return
            tree = Tree(label=f"[bold orange]Replicas for [/][green]{dataset}[/]")
            for site, files in self.replica_results_bysite[dataset].items():
                T = tree.add(f"[green]{site}")
                for f in files:
                    T.add(f"[cyan]{f}")

            self.console.print(tree)

    def do_set_replicas_sorting(self, sort: str = None):
        """Set the sorting mode for the replicas.

        If `sort` is None, it will ask the user for the sorting mode.
        If user input is empty, the sorting mode will not be changed.

        Parameters
        ----------
        sort : str, optional
            how to sort replicas, by default None
            if None, it will ask the user for the sorting mode.
        """
        print(
            f"[bold cyan]Current sorting mode for replicas: [green]{self.sort_replicas}"
        )
        print("[bold cyan]Available sorting options: [yellow] geoip, priority")
        if sort is None:
            sort = Prompt.ask(
                "[yellow]How to sort replicas? (leave empty to make no changes)"
            )
        if sort != "":
            self.sort_replicas = sort
            print(
                f"[bold green]New sorting mode for replicas: [cyan]{self.sort_replicas}"
            )


    def do_save(self, filename=None):
        """Save the replica information in yaml format"""        
        if not filename:
            filename = Prompt.ask(
                "[yellow bold]Output file name (.yaml or .json)", default="output.json"
            )
        format = os.path.splitext(filename)[1]
        if not format:
            print("[red] Please use a .json or .yaml filename for the output")
            return

        # First save the datasets in the format of the PocketCoffea metadata
        groups = defaultdict(list)
        for dataset in self.selected_datasets:
            group, name, tier = dataset[1:].split("/")
            groups[group].append(dataset)
        output_definition = defaultdict(dict)
        output_definition_withreplicas = defaultdict(dict)
        for group, datasets in groups.items():
            dataset_info = {
                "sample": group,
                "json_output": f"datasets/{group}.json",
                "files": []
            }
            for dataset in datasets:
                dataset_info["files"].append({
                "das_names": [dataset],
                "metadata": self.selected_datasets_metadata[self.selected_datasets.index(dataset)]
                })
            output_definition[group] = dataset_info
            # now replica info
            ireplicas = 0
            for dataset in datasets:
                if dataset in self.replica_results:
                    dataset_info_withreplicas = {
                        "json_output": f"datasets/{group}.json",
                    }
                    dataset_info_withreplicas.update(self.final_output[dataset])
                    dataset_info_withreplicas["metadata"]["das_names"] = [dataset]
                    dataset_info_withreplicas["metadata"]["sample"] = group
                    output_definition_withreplicas[f"{group}_{ireplicas}"] = dataset_info_withreplicas
                    ireplicas += 1


        if format == ".yaml":
            with open(filename.replace(".yaml","_replicas.yaml") , "w") as file:
                yaml.dump(output_definition_withreplicas,
                          file,
                          default_flow_style=False)
            with open(filename, "w") as file:
                yaml.dump(output_definition, file, default_flow_style=False)

        elif format == ".json":
            with open(filename.replace(".json","_replicas.json") , "w") as file:
                json.dump(output_definition_withreplicas, file, indent=2)
            with open(filename, "w") as file:
                json.dump(output_definition, file, indent=2)
               
        print(f"[green]File {filename} saved!")
        # Ask to reset the selection
        if Confirm.ask("[red]Do you want to empty your selected samples list?[/]", default=False):
            self.selected_datasets = []
            self.selected_datasets_metadata = []
            self.replica_results = defaultdict(list)
            self.replica_results_metadata = {}
            self.replica_results_bysite = {}
            self.final_output = None
            print(f"[green]Selected datasets list emptied![/]")

    # Define a empty-list function
    def do_clear(self):
        if Confirm.ask("[red]Do you want to empty your selected samples list?[/]", default=False):
            self.selected_datasets = []
            self.selected_datasets_metadata = []
            self.replica_results = defaultdict(list)
            self.replica_results_metadata = {}
            self.replica_results_bysite = {}
            self.final_output = None
            print(f"[green]Selected datasets list emptied![/]")

    def load_dataset_definition(
        self,
        dataset_definition,
        query_results_strategy="all",
        replicas_strategy="round-robin",
    ):
        """
        Initialize the DataDiscoverCLI by querying a set of datasets defined in `dataset_definitions`
        and selected results and replicas following the options.

        - query_results_strategy:  "all" or "manual" to be prompt for selection
        - replicas_strategy:
            - "round-robin": select randomly from the available sites for each file
            - "choose": filter the sites with a list of indices for all the files
            - "first": take the first result returned by rucio
            - "manual": to be prompt for manual decision dataset by dataset
        """
        for dataset_query, dataset_meta in dataset_definition.items():
            print(f"\nProcessing query: {dataset_query}")
            # Adding queries
            self.do_query(dataset_query)
            # Now selecting the results depending on the interactive mode or not.
            # Metadata are passed to the selection function to associated them with the selected dataset.
            if query_results_strategy not in ["all", "manual"]:
                raise ValueError(
                    "Invalid query-results-strategy option: please choose between: manual|all"
                )
            elif query_results_strategy == "manual":
                self.do_select(selection=None, metadata=dataset_meta)
            else:
                self.do_select(selection="all", metadata=dataset_meta)

        # Now list all
        self.do_list_selected()

        # selecting replicas
        self.do_sites_filters(ask_clear=False)
        print("Getting replicas")
        if replicas_strategy == "manual":
            out_replicas = self.do_replicas(mode=None, selection="all")
        else:
            if replicas_strategy not in ["round-robin", "choose", "first"]:
                raise ValueError(
                    "Invalid replicas-strategy: please choose between manual|round-robin|choose|first"
                )
            out_replicas = self.do_replicas(mode=replicas_strategy, selection="all")
        # Now list all
        self.do_list_selected()
        return out_replicas

# This is the main function that will be called by the CLI


@click.command()
@click.option("--dataset-definition", help="Dataset definition file", type=str, required=False)
@click.option("--output", help="Output name for dataset discovery output (no fileset preprocessing)", type=str, required=False, default="output_dataset.json")
@click.option("--fileset-output", help="Output name for fileset", type=str, required=False, default="output_fileset")
@click.option("--allow-sites", help="List of sites to be allowlisted", nargs="+", type=str)
@click.option("--block-sites", help="List of sites to be blocklisted", nargs="+", type=str)
@click.option("--priority-sites", help="List of priority order for sites", nargs="+", type=str)
@click.option("--regex-sites", help="Regex string to be used to filter the sites", type=str)
@click.option("--query-results-strategy", help="Mode for query results selection: [all|manual]", type=str, default="all")
@click.option("--replicas-strategy", help="Mode for selecting replicas for datasets: [manual|round-robin|first|choose]", default="round-robin", required=False)
def dataset_discovery_cli(dataset_definition, output, fileset_output, allow_sites, block_sites, priority_sites, regex_sites, query_results_strategy, replicas_strategy):
    """CLI for interactive dataset discovery."""
    cli = DataDiscoveryCLI()

    if allow_sites:
        cli.sites_allowlist = allow_sites
    if block_sites:
        cli.sites_blocklist = block_sites
    if priority_sites:
        cli.sites_prioritylist = priority_sites
    if regex_sites:
        cli.sites_regex = regex_sites

    if dataset_definition:
        with open(dataset_definition) as file:
            dd = json.load(file)
        cli.load_dataset_definition(
            dd,
            query_results_strategy=query_results_strategy,
            replicas_strategy=replicas_strategy,
        )
        # Save
        if output:
            cli.do_save(filename=output)

    cli.start_cli()


if __name__ == "__main__":
    dataset_discovery_cli()
