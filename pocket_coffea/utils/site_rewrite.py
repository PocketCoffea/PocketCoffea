"""Helpers for rewriting xrootd URLs in a manual-job fileset.

Used by `--recreate-jobs` on the manual-jobs executors
(`executors_lxplus.py`, `executors_rubin.py`) and by
`scripts/check_jobs.py` to migrate files away from a blocklisted CMS site
and to recover from per-file XRootD errors.

Replica lookups go through the Rucio client (same path as
`pocket_coffea.utils.rucio.get_dataset_files_replicas`), so the tools
work on any machine that can talk to the CMS Rucio server with a valid
X509 proxy — `dasgoclient` is **not** required.

Rucio is imported lazily inside `_query_replicas` so this module stays
importable in environments without the rucio package (the unit tests
monkey-patch `_query_replicas` directly).
"""
from copy import deepcopy


GLOBAL_XROOTD_REDIRECTOR = "root://xrootd-cms.infn.it//"


def _split_lfn(filepath):
    """Split an xrootd PFN into (rootpref, "/store/..." LFN).
    Returns (None, filepath) if no /store/ segment is found."""
    if filepath.startswith("root:/") and "/store/" in filepath:
        rootpref = filepath.split("/store/")[0]
        return rootpref, "/store/" + filepath.split("/store/")[1]
    return None, filepath


def _site_of_url(filepath, sitemap):
    """Return the site name in `sitemap` that serves `filepath`, or None.
    Only string-prefix sitemap entries are considered (rule-based sites are
    skipped — same convention as find_other_file)."""
    rootpref, _ = _split_lfn(filepath)
    if rootpref is None:
        return None
    for site, sitepath in sitemap.items():
        if not isinstance(sitepath, str):
            continue
        if rootpref in sitepath or sitepath in rootpref:
            return site
    return None


def _query_replicas(lfn, client=None, scope="cms", sort="random"):
    """Return the ordered list of site names (RSEs) hosting `lfn`.

    Uses `rucio.Client.list_replicas` — the same call used by
    `pocket_coffea.utils.rucio.get_dataset_files_replicas`. Returns an
    empty list when rucio is unavailable or the lookup fails, so callers
    can decide how to fall back.

    `sort` is forwarded to rucio. The default is ``"random"`` because
    this helper is used to find an *alternative* replica for a file that
    just failed: geoip sorting would deterministically pick the same
    nearby site for every file in a recreate-jobs pass, which both
    concentrates load on one site and is likely to re-hit the same
    unhealthy replica the user is trying to escape. Pass ``sort="geoip"``
    explicitly if you want the closest replica.

    Tests monkey-patch this function directly to avoid the network round
    trip and the rucio dependency."""
    try:
        from pocket_coffea.utils.rucio import get_rucio_client
        from rucio.common.client import detect_client_location
    except ImportError as e:
        print(f"WARNING: rucio not importable ({e}); cannot look up replicas for {lfn}.")
        return []
    if client is None:
        try:
            client = get_rucio_client()
        except Exception as e:
            print(f"WARNING: could not open a rucio client ({e}); cannot look up replicas for {lfn}.")
            return []
    try:
        replicas = list(client.list_replicas(
            [{"scope": scope, "name": lfn}],
            client_location=detect_client_location(),
            sort=sort if sort in ("geoip", "custom_table", "random") else None,
        ))
    except Exception as e:
        print(f"WARNING: rucio replica lookup failed for {lfn}: {e}")
        return []
    if not replicas:
        return []
    filedata = replicas[0]
    # `pfns` is sorted by rucio (per the `sort` arg above); preserve that order.
    pfns = filedata.get("pfns", {})
    return [pfn["rse"] for pfn in pfns.values()]


def find_other_file(filepath, sitemap, blocklist=None,
                    fallback_redirector=GLOBAL_XROOTD_REDIRECTOR,
                    rucio_client=None):
    """Find an alternative xrootd location for `filepath`.

    Asks Rucio for the file's replicas (via `_query_replicas`) and returns
    the first one served by a site that is (a) present in `sitemap`,
    (b) not in `blocklist`, and (c) different from the file's current
    site. If no such site is found, falls back to
    `fallback_redirector + LFN`. If the URL has no /store/ segment to
    extract, returns it unchanged with a warning.

    Every site change is logged so the user can audit what was rewritten."""
    blocklist = set(blocklist or [])
    rootpref, file = _split_lfn(filepath)

    if rootpref is None:
        print(f"WARNING: cannot extract LFN from {filepath}; leaving unchanged.")
        return filepath

    cur_site = _site_of_url(filepath, sitemap)
    cur_site_str = cur_site or f"<unknown:{rootpref}>"

    sites = _query_replicas(file, client=rucio_client)
    for site in sites:
        if site in blocklist or site.replace("_Disk", "") in blocklist:
            continue
        if site not in sitemap:
            continue
        sitepath = sitemap[site]
        if not isinstance(sitepath, str):
            continue
        if rootpref in sitepath or sitepath in rootpref:
            continue
        new_url = sitepath + file
        print(f"  [site rewrite] {file}  {cur_site_str} -> {site}  ({rootpref or '<none>'} -> {sitepath})")
        return new_url

    new_url = fallback_redirector + file.lstrip("/")
    print(f"  [site rewrite] {file}  {cur_site_str} -> GLOBAL_REDIRECTOR  "
          f"({rootpref or '<none>'} -> {fallback_redirector})  "
          f"[no per-site replica; blocklist={sorted(blocklist) or None}]")
    return new_url


def rewrite_fileset_to_redirector(fileset, redirector=GLOBAL_XROOTD_REDIRECTOR):
    '''Return a deepcopy of `fileset` with every file URL rewritten so the
    redirector prefix is replaced by `redirector + LFN`.

    No Rucio lookup is performed — every file is unconditionally pointed
    at the redirector. Files that don't carry a recognisable `/store/...`
    LFN are left untouched (with a warning). Order of datasets and of
    files within each dataset is preserved.'''
    new_fileset = deepcopy(fileset)
    for sample, dct in new_fileset.items():
        n_rewritten = 0
        n_unchanged = 0
        newfllist = []
        for fl in dct['files']:
            rootpref, file = _split_lfn(fl)
            if rootpref is None:
                print(f"WARNING: cannot extract LFN from {fl}; leaving unchanged.")
                newfllist.append(fl)
                n_unchanged += 1
                continue
            new_url = redirector + file.lstrip("/")
            if new_url != fl:
                n_rewritten += 1
            else:
                n_unchanged += 1
            newfllist.append(new_url)
        dct['files'] = newfllist
        if n_rewritten:
            print(f"[redirector] {sample}: rewrote {n_rewritten}/{n_rewritten + n_unchanged} files "
                  f"to {redirector}")
    return new_fileset


def rewrite_fileset_blocklist(fileset, sitemap, blocklist,
                              fallback_redirector=GLOBAL_XROOTD_REDIRECTOR,
                              rucio_client=None):
    """Return a deepcopy of `fileset` with every file currently served by a
    site in `blocklist` rewritten via `find_other_file`. Files at non-
    blocklisted sites are left untouched. Order of datasets and of files
    within each dataset is preserved.

    A shared `rucio_client` is created lazily on the first lookup so that
    a single rewrite over many files reuses the same authenticated
    client."""
    blocklist = set(blocklist or [])
    if not blocklist:
        return fileset
    new_fileset = deepcopy(fileset)
    for sample, dct in new_fileset.items():
        n_rewritten = 0
        n_kept = 0
        newfllist = []
        for fl in dct['files']:
            cur_site = _site_of_url(fl, sitemap)
            if cur_site is not None and cur_site in blocklist:
                print(f"[blocklist] {sample}: file at {cur_site} is blocklisted, looking for alternative...")
                newfllist.append(find_other_file(fl, sitemap, blocklist=blocklist,
                                                 fallback_redirector=fallback_redirector,
                                                 rucio_client=rucio_client))
                n_rewritten += 1
            else:
                newfllist.append(fl)
                n_kept += 1
        dct['files'] = newfllist
        if n_rewritten:
            print(f"[blocklist] {sample}: rewrote {n_rewritten}/{n_rewritten + n_kept} files away from blocklisted sites.")
    return new_fileset
