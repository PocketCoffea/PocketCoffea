import argparse
from copy import deepcopy
import sys

import cloudpickle

from pocket_coffea.utils.rucio import get_rucio_client, get_xrootd_sites_map
from pocket_coffea.utils.site_rewrite import (
    _site_of_url,
    _split_lfn,
    extract_failed_url,
    find_other_file,
    normalize_rse,
)


EXIT_REWRITTEN = 0
EXIT_HELPER_ERROR = 1
EXIT_NO_XROOTD = 2
EXIT_NO_CHANGE = 3

def site_prefix(filepath):
    rootpref, _ = _split_lfn(filepath)
    return rootpref.rstrip("/") if rootpref else None


def _load_failed_sites(path):
    if not path:
        return set()
    try:
        with open(path) as handle:
            return {normalize_rse(line.strip()) for line in handle if line.strip()}
    except FileNotFoundError:
        return set()


def _save_failed_sites(path, failed_sites):
    if not path:
        return
    with open(path, "w") as handle:
        for site in sorted({normalize_rse(site) for site in failed_sites}):
            handle.write(f"{site}\n")


def rewrite_files_from_failed_site(filesets, failed_url, sitemap, blocklist_sites=None,
                                   rucio_client=None):
    failed_prefix = site_prefix(failed_url)
    if failed_prefix is None:
        return deepcopy(filesets), 0, 0

    blocklist_sites = {normalize_rse(site) for site in (blocklist_sites or [])}
    new_filesets = deepcopy(filesets)
    ntarget = 0
    nchanged = 0
    for sample, dct in new_filesets.items():
        rewritten = []
        for filepath in dct["files"]:
            if site_prefix(filepath) != failed_prefix:
                rewritten.append(filepath)
                continue
            ntarget += 1
            new_filepath = find_other_file(
                filepath, sitemap, blocklist=blocklist_sites,
                rucio_client=rucio_client,
            )
            if new_filepath != filepath:
                nchanged += 1
            rewritten.append(new_filepath)
        dct["files"] = rewritten
    return new_filesets, ntarget, nchanged


def rewrite_config_from_log(config_file, log_file, failed_sites_file=None):
    with open(log_file) as handle:
        failed_url = extract_failed_url(handle.read())
    if failed_url is None:
        return EXIT_NO_XROOTD

    config = cloudpickle.load(open(config_file, "rb"))
    sitemap = get_xrootd_sites_map()
    try:
        rucio_client = get_rucio_client()
    except Exception as exc:
        print(f"WARNING: could not open a rucio client ({exc}); replica lookups may fall back.")
        rucio_client = None

    failed_sites = _load_failed_sites(failed_sites_file)
    failed_site = _site_of_url(failed_url, sitemap)
    if failed_site is not None:
        failed_sites.add(failed_site)
        _save_failed_sites(failed_sites_file, failed_sites)

    new_filesets, ntarget, nchanged = rewrite_files_from_failed_site(
        config.filesets, failed_url, sitemap, blocklist_sites=failed_sites,
        rucio_client=rucio_client,
    )
    if nchanged == 0:
        print(f"XRootD failure found at {site_prefix(failed_url)}, but no config URLs changed.")
        return EXIT_NO_CHANGE

    config.set_filesets_manually(new_filesets)
    cloudpickle.dump(config, open(config_file, "wb"))
    print(f"Rewrote {nchanged}/{ntarget} files from failed XRootD site {site_prefix(failed_url)}.")
    return EXIT_REWRITTEN


def main(argv=None):
    parser = argparse.ArgumentParser(description="Rewrite XRootD URLs in a job config after an XRootD failure.")
    parser.add_argument("--config", required=True, help="Path to the job configurator pickle.")
    parser.add_argument("--log", required=True, help="Path to the runner attempt log.")
    parser.add_argument("--failed-sites-file", default=None,
                        help="Optional state file tracking failed XRootD sites across attempts.")
    args = parser.parse_args(argv)

    try:
        return rewrite_config_from_log(args.config, args.log, args.failed_sites_file)
    except Exception as exc:
        print(f"ERROR: XRootD rewrite helper failed: {exc}")
        return EXIT_HELPER_ERROR


if __name__ == "__main__":
    sys.exit(main())
