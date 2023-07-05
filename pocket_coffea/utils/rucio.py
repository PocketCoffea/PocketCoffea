from pocket_coffea.utils.network import get_proxy_path
import os
import getpass
import re
import json
from rucio.client import Client
from collections import defaultdict

os.environ["RUCIO_HOME"] = "/cvmfs/cms.cern.ch/rucio/x86_64/rhel7/py3/current"


def get_rucio_client():
    try:
        nativeClient = Client(
            rucio_host="https://cms-rucio.cern.ch",
            auth_host="https://cms-rucio-auth.cern.ch",
            account=getpass.getuser(),
            creds={"client_cert": get_proxy_path(), "client_key": get_proxy_path()},
            auth_type='x509',
        )
        return nativeClient

    except Exception as e:
        print("Wrong Rucio configuration, impossible to create client")
        raise e


def get_xrootd_sites_map():
    sites_xrootd_access = defaultdict(dict)
    if not os.path.exists(".sites_map.json"):
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
            except:
                continue
            for site in data:
                if site["type"] != "DISK":
                    continue
                if site["rse"] == None:
                    continue
                for proc in site["protocols"]:
                    if proc["protocol"] == "XRootD":
                        if proc["access"] not in ["global-ro", "global-rw"]:
                            continue
                        if "prefix" not in proc:
                            if "rules" in proc:
                                for rule in proc["rules"]:
                                    sites_xrootd_access[site["rse"]][rule["lfn"]] = rule["pfn"]
                        else:
                            sites_xrootd_access[site["rse"]] = proc["prefix"]
        json.dump(sites_xrootd_access, open(".sites_map.json", "w"))

    return json.load(open(".sites_map.json"))


def _get_pfn_for_site(path, rules):
    if isinstance(rules, dict):
        for rule, pfn in rules.items():
            if m:=re.match(rule, path):
                grs = m.groups()
                for i in range(len(grs)):
                    pfn = pfn.replace(f"${i+1}", grs[i])
                return pfn
    else: 
        return rules + "/"+ path
    

def get_dataset_files(
    dataset,
    whitelist_sites=None,
    blacklist_sites=None,
    regex_sites=None,
    output="first",
):
    '''
    This function queries the Rucio server to get information about the location
    of all the replicas of the files in a CMS dataset.

    The sites can be filtered in 3 different ways:
    - `whilist_sites`: list of sites to select from. If the file is not found there, raise an Expection.
    - `blacklist_sites`: list of sites to avoid. If the file has no left site, raise an Exception
    - `regex_sites`: regex expression to restrict the list of sites.

    The function can return all the possible sites for each file (`output="all"`)
    or the first site found for each file (`output="first"`, by default)
    '''
    sites_xrootd_prefix = get_xrootd_sites_map()
    client = get_rucio_client()
    outsites = []
    outfiles = []
    for filedata in client.list_replicas([{"scope": "cms", "name": dataset}]):
        outfile = []
        outsite = []
        rses = filedata["rses"]
        found = False
        if whitelist_sites:
            for site in whitelist_sites:
                if site in rses:
                    # Check actual availability
                    meta = filedata["pfns"][rses[site][0]]
                    if (
                        meta["type"] != "DISK"
                        or meta["volatile"] == True
                        or filedata["states"][site] != "AVAILABLE"
                        or site not in sites_xrootd_prefix
                    ):
                        continue
                    outfile.append(_get_pfn_for_site(filedata["name"], sites_xrootd_prefix[site]))
                    outsite.append(site)
                    found = True

            if not found:
                raise Exception(
                    f"No SITE available in the whitelist for file {filedata['name']}"
                )
        else:
            possible_sites = list(rses.keys())
            if blacklist_sites:
                possible_sites = list(
                    filter(lambda key: key not in blacklist_sites, possible_sites)
                )

            if len(possible_sites) == 0:
                raise Exception(f"No SITE available for file {filedata['name']}")

            # now check for regex
            for site in possible_sites:
                if regex_sites:
                    if re.match(regex_sites, site):
                        # Check actual availability
                        meta = filedata["pfns"][rses[site][0]]
                        if (
                            meta["type"] != "DISK"
                            or meta["volatile"] == True
                            or filedata["states"][site] != "AVAILABLE"
                            or site not in sites_xrootd_prefix
                        ):
                            continue
                        outfile.append(_get_pfn_for_site(filedata["name"], sites_xrootd_prefix[site]))
                        outsite.append(site)
                        found = True
                else:
                    # Just take the first one
                    # Check actual availability
                    meta = filedata["pfns"][rses[site][0]]
                    if (
                        meta["type"] != "DISK"
                        or meta["volatile"] == True
                        or filedata["states"][site] != "AVAILABLE"
                        or site not in sites_xrootd_prefix
                    ):
                        continue
                    outfile.append(_get_pfn_for_site(filedata["name"], sites_xrootd_prefix[site]))
                    outsite.append(site)
                    found = True

        if not found:
            raise Exception(f"No SITE available for file {filedata['name']}")
        else:
            if output == "all":
                outfiles.append(outfile)
                outsites.append(outsite)
            elif output == "first":
                outfiles.append(outfile[0])
                outsites.append(outsite[0])

    return outfiles, outsites
