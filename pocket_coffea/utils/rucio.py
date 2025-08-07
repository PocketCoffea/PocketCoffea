import os
from multiprocessing import Lock
from collections import defaultdict
import re
import json
import time
import requests
from rucio.client import Client
from rucio.common.client import detect_client_location
from pocket_coffea.utils.network import get_proxy_path

# Rucio needs the default configuration --> taken from CMS cvmfs defaults
if "RUCIO_HOME" not in os.environ:
    os.environ["RUCIO_HOME"] = "/cvmfs/cms.cern.ch/rucio/current"


def get_rucio_client(proxy=None) -> Client:
    """
    Open a client to the CMS rucio server using x509 proxy.

    Parameters
    ----------
        proxy : str, optional
            Use the provided proxy file if given, if not use `voms-proxy-info` to get the current active one.

    Returns
    -------
        nativeClient: rucio.Client
            Rucio client
    """
    try:
        if not proxy:
            proxy = get_proxy_path()
        nativeClient = Client()
        return nativeClient

    except Exception as e:
        print("Wrong Rucio configuration, impossible to create client")
        raise e

   
def get_xrootd_sites_map():
    """
    The mapping between RSE (sites) and the xrootd prefix rules is read
    from `/cvmfs/cms/cern.ch/SITECONF/*site*/storage.json`.

    This function returns the list of xrootd prefix rules for each site.
    """

    sites_xrootd_access = defaultdict(dict)
    # Check if the cache file has been modified in the last 10 minutes
    cache_valid = False
    if os.path.exists(".sites_map.json"):
        file_time = os.path.getmtime(".sites_map.json")
        current_time = time.time()
        sixty_minutes_ago = current_time - 3600
        if file_time > sixty_minutes_ago:
            cache_valid = True

    lock = Lock()

    if not os.path.exists(".sites_map.json") or not cache_valid:
        print("Loading SITECONF info")
        sites = [
            (s, "/cvmfs/cms.cern.ch/SITECONF/" + s + "/storage.json")
            for s in os.listdir("/cvmfs/cms.cern.ch/SITECONF/")
            if s.startswith("T")
        ]
        for site_name, conf in sites:
            if not os.path.exists(conf):
                continue
            try:
                data = json.load(open(conf))
            except Exception:
                continue
            for site in data:
                if site["type"] != "DISK":
                    continue
                if site["rse"] is None:
                    continue
                for proc in site["protocols"]:
                    if proc["protocol"] == "XRootD":
                        if proc["access"] not in ["global-ro", "global-rw"]:
                            continue
                        if "prefix" not in proc:
                            if "rules" in proc:
                                for rule in proc["rules"]:
                                    sites_xrootd_access[site["rse"]][rule["lfn"]] = (
                                        rule["pfn"]
                                    )
                        else:
                            sites_xrootd_access[site["rse"]] = proc["prefix"]

        lock.acquire()
        try:
            json.dump(sites_xrootd_access, open(".sites_map.json", "w"))
        finally:
            lock.release()

    return json.load(open(".sites_map.json"))


def _get_pfn_for_site(path, rules):
    """
    Utility function that converts the file path to a valid pfn matching
    the file path with the site rules (regexes).
    """
    if isinstance(rules, dict):
        for rule, pfn in rules.items():
            if m := re.match(rule, path):
                grs = m.groups()
                for i in range(len(grs)):
                    pfn = pfn.replace(f"${i+1}", grs[i])
                return pfn
    else:
        # not adding any slash as the path usually starts with it
        if path.startswith("/"):
            path = path[1:]
        return rules + "/" + path


def get_dataset_files_replicas(
        dataset,
        allowlist_sites=None,
        include_redirector=False,
        blocklist_sites=None,
        prioritylist_sites=None,
        regex_sites=None,
        mode="full",
        partial_allowed=False,
        client=None,
        scope="cms",
        sort: str = "geoip",
        invalid_list=[],
):
    """Query the Rucio server to get information about the location of all the replicas of the files in a CMS dataset.

    The sites can be filtered in 3 different ways:
    - `allowlist_sites`: list of sites to select from. If the file is not found there, raise an Exception.
    - `blocklist_sites`: list of sites to avoid. If the file has no left site, raise an Exception
    - `prioritylist_sites`: list of priorised sites. Sorts these sites to front if available and sort is 'priority'
    - `regex_sites`: regex expression to restrict the list of sites.

    The fileset returned by the function is controlled by the `mode` parameter:
    - "full": returns the full set of replicas and sites (passing the filtering parameters)
    - "first": returns the first replica found for each file
    - "best": to be implemented (ServiceX..)
    - "roundrobin": try to distribute the replicas over different sites

    Parameters
    ----------
        dataset: str
        allowlist_sites: list
        blocklist_sites: list
        prioritylist_sites: list
        regex_sites: list
        mode:  str, default "full"
        client: rucio Client, optional
        partial_allowed: bool, default False
        scope:  rucio scope, "cms"
        sort: str, default 'geoip'
            Sort replicas (for details check rucio documentation)
        invalid_list: list
            A list of invalid files for this dataset (to be exluded in the output).
            Rucio does not know of invalid files, so these need to be obtained beforehand from DAS.

    Returns
    -------
        files: list
           depending on the `mode` option.
           - If `mode=="full"`, returns the complete list of replicas for each file in the dataset
           - If `mode=="first"`, returns only the first replica for each file.

        sites: list
           depending on the `mode` option.
           - If `mode=="full"`, returns the list of sites where the file replica is available for each file in the dataset
           - If `mode=="first"`, returns a list of sites for the first replica of each file.

        sites_counts: dict
           Metadata counting the coverage of the dataset by site

    """
    sites_xrootd_prefix = get_xrootd_sites_map()
    client = client if client else get_rucio_client()
    outsites = []
    outfiles = []
    for filedata in client.list_replicas(
        [{"scope": scope, "name": dataset}],
        client_location=detect_client_location(),
        sort=sort if sort in ["geoip", "custom_table", "random"] else None,
    ):
        outfile = []
        outsite = []
        rses = filedata["rses"]
        # NOTE: rses are not sorted!
        # pfns are sorted (https://rucio.cern.ch/documentation/html/client_api/replicaclient.html#rucio.client.replicaclient.ReplicaClient.list_replicas)
        pfns = filedata["pfns"]
        rses_sorted = [pfn["rse"] for pfn in pfns.values()]
        rses = {rse: rses[rse] for rse in rses_sorted}
        found = False
        if filedata["name"] in invalid_list:
            #print(f"The following file is invalid, we skip it:\n {filedata['name']}")
            continue

        if allowlist_sites:
            for site in allowlist_sites:
                if site in rses:
                    # Check actual availability
                    meta = filedata["pfns"][rses[site][0]]
                    if (
                        meta["type"] != "DISK"
                        or meta["volatile"]
                        or filedata["states"][site] != "AVAILABLE"
                        or site not in sites_xrootd_prefix
                    ):
                        continue
                    outfile.append(
                        _get_pfn_for_site(filedata["name"], sites_xrootd_prefix[site])
                    )
                    outsite.append(site)
                    found = True

        else:
            possible_sites = list(rses.keys())
            if blocklist_sites:
                possible_sites = list(
                    filter(lambda key: (
                        (key not in blocklist_sites) and (key.replace("_Disk", "") not in blocklist_sites)
                        ), possible_sites)
                )

            if len(possible_sites) == 0 and not partial_allowed and not include_redirector:
                raise Exception(f"No SITE available for file {filedata['name']}")

            # now check for regex
            for site in possible_sites:
                if regex_sites:
                    if re.search(regex_sites, site):
                        # Check actual availability
                        meta = filedata["pfns"][rses[site][0]]
                        if (
                            meta["type"] != "DISK"
                            or meta["volatile"]
                            or filedata["states"][site] != "AVAILABLE"
                            or site not in sites_xrootd_prefix
                        ):
                            continue
                        outfile.append(
                            _get_pfn_for_site(
                                filedata["name"], sites_xrootd_prefix[site]
                            )
                        )
                        outsite.append(site)
                        found = True
                else:
                    # Just take the first one
                    # Check actual availability
                    meta = filedata["pfns"][rses[site][0]]
                    if (
                        meta["type"] != "DISK"
                        or meta["volatile"]
                        or filedata["states"][site] != "AVAILABLE"
                        or site not in sites_xrootd_prefix
                    ):
                        continue
                    outfile.append(
                        _get_pfn_for_site(filedata["name"], sites_xrootd_prefix[site])
                    )
                    outsite.append(site)
                    found = True

        if not found and include_redirector:
            # The file was not found at any of the allowed sites
            # But with this option we add the INFN redirector prefix
            if len(list(rses.keys())) != 0:
                # Only makes sense if the file exists at least somewhere
                outfile.append(
                    _get_pfn_for_site(filedata["name"], "root://xrootd-cms.infn.it//")
                )
                outsite.append('INFN')
                print("\t WARNING! The file was NOT found at any of the allowed sites. Setting its prefix to INFN! \n ", outfile)
                found = True

        if not found and not partial_allowed:
            raise Exception(f"No SITE available for file: \n {filedata['name']}")
        else:
            # Sort by prioritylist if applicable
            if prioritylist_sites and sort.lower() == "priority":
                for priority in prioritylist_sites[::-1]:
                    if priority in outsite:
                        original_idx = outsite.index(priority)
                        outsite.insert(0, outsite.pop(original_idx))
                        outfile.insert(0, outfile.pop(original_idx))
            if mode == "full":
                outfiles.append(outfile)
                outsites.append(outsite)
            elif mode == "first":
                outfiles.append(outfile[0])
                outsites.append(outsite[0])
            else:
                raise NotImplementedError(f"Mode {mode} not yet implemented!")

    # Computing replicas by site:
    sites_counts = defaultdict(int)
    if mode == "full":
        for sites_by_file in outsites:
            for site in sites_by_file:
                sites_counts[site] += 1
    elif mode == "first":
        for site_by_file in outsites:
            sites_counts[site] += 1

    return outfiles, outsites, sites_counts


def get_dataset_files_from_dbs(
        dataset_name: str,
        dbs_instance: str = "prod/global"):
    '''
    This function queries the DBS server to get information about the location
    of each block in a CMS dataset.
    It is used instead of the rucio replica query when the dataset is not available in rucio.
    '''

    # Get the site of the blocks
    proxy = get_proxy_path()
    sites_xrootd_prefix = get_xrootd_sites_map()
    link = f"https://cmsweb.cern.ch:8443/dbs/{dbs_instance}/DBSReader/blocks?dataset={dataset_name}&detail=True"
    r = requests.get(link, cert=proxy, verify=False)
    outputfiles, outputsites = [], []

    if r.status_code == 200:
        data = r.json()
    
        for block in data:
            #now query for files
            link = f"https://cmsweb.cern.ch:8443/dbs/{dbs_instance}/DBSReader/files?block_name={block['block_name'].replace('#', '%23')}"
            rfiles = requests.get( link,  cert=proxy, verify=False)
            site = block["origin_site_name"]

            for f in rfiles.json():
                outputfiles.append(_get_pfn_for_site(f["logical_file_name"], sites_xrootd_prefix[site]))
                outputsites.append(site)
            
    else:
        raise Exception(f"Dataset {dataset_name} not found on dbs_instance {dbs_instance}")
    
    return outputfiles, outputsites



def query_dataset(
    query: str, client=None, tree: bool = False, datatype="container", scope="cms"
):
    """
    This function uses the rucio client to query for containers or datasets.

    Parameters
    ---------
        query: str = query to filter datasets / containers with the rucio list_dids functions
        client: rucio client
        tree: bool = if True return the results splitting the dataset name in parts parts
        datatype: "container/dataset":  rucio terminology. "Container"==CMS dataset. "Dataset" == CMS block.
        scope: "cms". Rucio instance

    Returns
    -------
       list of containers/datasets

       if tree==True, returns the list of dataset and also a dictionary decomposing the datasets
       names in the 1st command part and a list of available 2nd parts.

    """
    client = client if client else get_rucio_client()
    out = list(
        client.list_dids(
            scope=scope, filters={"name": query, "type": datatype}, long=False
        )
    )
    if tree:
        outdict = {}
        for dataset in out:
            split = dataset[1:].split("/")
            if split[0] not in outdict:
                outdict[split[0]] = defaultdict(list)
            outdict[split[0]][split[1]].append(split[2])
        return out, outdict
    else:
        return out
