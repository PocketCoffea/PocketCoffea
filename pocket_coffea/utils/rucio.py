from pocket_coffea.utils.network import get_proxy_path
import os
import getpass
import re
from rucio.client import Client

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
    client = get_rucio_client()
    outsites = []
    outfiles = []
    for filedata in client.list_replicas(
        [{"scope": "cms", "name": dataset}], schemes=["root"]
    ):
        outfile = []
        outsite = []
        rses = filedata["rses"]
        found = False
        if whitelist_sites:
            for site in whitelist_sites:
                if site in rses:
                    outfile.append(rses[site][0])
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
            if regex_sites:
                for site in possible_sites:
                    if re.match(regex_sites, site):
                        outfile.append(rses[site][0])
                        outsite.append(site)
                        found = True
            else:
                # Just take the first one
                outfile.append(rses[possible_sites[0]][0])
                outsite.append(possible_sites[0])
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
