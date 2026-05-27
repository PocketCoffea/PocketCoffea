import os
import pathlib
import shutil
import json
import awkward as ak
from typing import Any, Dict, List, Optional


def is_rootcompat(a):
    """Is it a flat or 1-d jagged array?"""
    t = ak.type(a)
    if isinstance(t, ak._ext.ArrayType):
        if isinstance(t.type, ak._ext.PrimitiveType):
            return True
        if isinstance(t.type, ak._ext.ListType) and isinstance(
            t.type.type, ak._ext.PrimitiveType
        ):
            return True
    return False


def uproot_writeable(events):
    """Restrict to columns that uproot can write compactly"""
    out = {}
    for bname in events.fields:
        if events[bname].fields:
            out[bname] = ak.zip(
                {
                    n: ak.packed(ak.without_parameters(events[bname][n]))
                    for n in events[bname].fields
                    if is_rootcompat(events[bname][n])
                }
            )
        else:
            out[bname] = ak.packed(ak.without_parameters(events[bname]))
    return out


def copy_file(
    fname: str,
    localdir: str,
    location: str,
    subdirs: Optional[List[str]] = None,
):
    subdirs = subdirs or []
    xrd_prefix = "root://"
    pfx_len = len(xrd_prefix)
    xrootd = False
    if xrd_prefix in location:
        try:
            import XRootD
            import XRootD.client

            xrootd = True
        except ImportError as err:
            raise ImportError(
                "Install XRootD python bindings with: conda install -c conda-forge xroot"
            ) from err
    local_file = (
        os.path.abspath(os.path.join(localdir, fname))
        if xrootd
        else os.path.join(localdir, fname)
    )
    merged_subdirs = "/".join(subdirs) if xrootd else os.path.sep.join(subdirs)
    destination = (
        f"{location}/" + merged_subdirs + f"/{fname}"
        if xrootd
        else os.path.join(location, os.path.join(merged_subdirs, fname))
    )

    if xrootd:
        copyproc = XRootD.client.CopyProcess()
        copyproc.add_job(local_file, destination, force=True)
        copyproc.prepare()
        status, response = copyproc.run()
        if status.status != 0:
            raise Exception(status.message)
        del copyproc
    else:
        dirname = os.path.dirname(destination)
        if not os.path.exists(dirname):
            pathlib.Path(dirname).mkdir(parents=True, exist_ok=True)
        shutil.copy(local_file, destination)
        assert os.path.isfile(destination)
    pathlib.Path(local_file).unlink()



def apply_skim_sumgenweights_override(accumulator, filesets):
    '''Override `accumulator['sum_genweights']` and
    `accumulator['sum_signOf_genweights']` from the authoritative dataset-level
    totals embedded in the dataset metadata at skim time.

    Background: when a skim drops every event of an input chunk, that chunk's
    contribution to the original sum_genweight is lost from the per-chunk
    reconstruction in `BaseProcessorABC.process` (the
    ``sum(skimRescaleGenWeight * genWeight)`` line that runs only on surviving
    events), because no ROOT file is written for the empty chunk. To recover it,
    the skim job persists the **pre-skim** dataset-level total into the new
    dataset JSON via `save_skimed_dataset_definition`; we read it back here and
    replace the (possibly under-counted) reconstructed total before
    `rescale_sumgenweights` runs.

    No-op when the dataset is not flagged `isSkim` or when the metadata
    doesn't carry the new fields — older skim outputs continue to use the
    per-chunk reconstruction.

    Returns the list of datasets whose totals were overridden, for logging.
    Kept dependency-free (stdlib only) so it can be unit-tested without the
    heavy executor / omegaconf stack.
    '''
    overridden = []
    sum_gw = accumulator.get("sum_genweights", {})
    sum_signof = accumulator.get("sum_signOf_genweights", {})
    for dataset in list(sum_gw.keys()):
        if dataset not in filesets:
            continue
        meta = filesets[dataset].get("metadata", {})
        if meta.get("isSkim") not in (True, "True", "true"):
            continue
        changed = False
        if "sum_genweights" in meta:
            sum_gw[dataset] = float(meta["sum_genweights"])
            changed = True
        if "sum_signOf_genweights" in meta and dataset in sum_signof:
            sum_signof[dataset] = float(meta["sum_signOf_genweights"])
            changed = True
        if changed:
            overridden.append(dataset)
    return overridden


def save_skimed_dataset_definition(processing_out, fileout, check_initial_events=True,
                                   skip_initial_events_check_datasets=None):
    '''Build the skimmed dataset JSON from a (merged) processing output.

    By default the number of initial events in the dataset metadata must match
    the ``cutflow["initial"]`` count for every dataset, otherwise an exception
    is raised (some input chunk was lost). ``skip_initial_events_check_datasets``
    is a list of dataset names for which this mismatch is tolerated: a warning is
    printed instead of raising, which is useful when a corrupted input file had
    to be skipped on purpose. ``check_initial_events=False`` disables the check
    entirely for all datasets.
    '''
    skip_initial_events_check_datasets = set(skip_initial_events_check_datasets or [])
    datasets_info = {}
    datasets_metadata = processing_out["datasets_metadata"]["by_dataset"]
    # Pre-skim totals computed in BaseProcessorABC.process() *before* the skim mask, so
    # they correctly include every input chunk — including the ones whose events were
    # entirely cut by the skim and produced no ROOT file. Carrying these into the new
    # dataset metadata lets the downstream postprocess recover correct cross-section
    # normalisation even when some input chunks are dropped entirely.
    sum_genweights_total = processing_out.get("sum_genweights", {})
    sum_signOf_genweights_total = processing_out.get("sum_signOf_genweights", {})
    # Now add the files
    for key in datasets_metadata.keys():
        # We first check that the total number of initial events
        # corresponds to the initial number of the events in the metadata
        # to check if we are not missing any event
        if check_initial_events and int(datasets_metadata[key]["nevents"]) != processing_out["cutflow"]["initial"][key]:
            if key in skip_initial_events_check_datasets:
                print(f"WARNING: The number of initial events in the metadata ({datasets_metadata[key]['nevents']}) is different from the number of initial events in the cutflow ({processing_out['cutflow']['initial'][key]}) for dataset {key}, but the check was explicitly skipped for it.")
            else:
                print(f"ERROR: The number of initial events in the metadata is different from the number of initial events in the cutflow for dataset {key}")
                raise Exception("Inconsistent number of initial events in the output of the skimming processing")

        # Count the remaining events
        datasets_info[key] =  {
            "metadata": datasets_metadata[key],
            "files": processing_out["skimmed_files"][key]
        }
        datasets_info[key]["metadata"]["isSkim"] = "True"
        datasets_info[key]["metadata"]["nevents"] = str(sum(processing_out["nskimmed_events"][key]))
        skim_efficiency = processing_out["cutflow"]["skim"][key] / processing_out["cutflow"]["initial"][key]
        datasets_info[key]["metadata"]["skim_efficiency"] = str(skim_efficiency)

        # Authoritative pre-skim totals — survive zero-event chunks because they were
        # accumulated before the skim mask in base.py::process().
        if key in sum_genweights_total:
            datasets_info[key]["metadata"]["sum_genweights"] = float(sum_genweights_total[key])
        if key in sum_signOf_genweights_total:
            datasets_info[key]["metadata"]["sum_signOf_genweights"] = float(sum_signOf_genweights_total[key])

    # Save the json
    with open(fileout, "w") as f:
        json.dump(datasets_info, f, indent=4)
