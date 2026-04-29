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


def uproot_writeable(events, branches=None):
    """Restrict to columns that uproot can write compactly.

    branches: dict mapping top-level collection names to lists of sub-field names
              to keep, or None to keep all sub-fields of that collection.
              If branches is None entirely, all collections are written.
              Collections absent from the dict are dropped.
    """
    out = {}
    for bname in events.fields:
        if branches is not None and bname not in branches:
            continue
        if events[bname].fields:
            keep = branches[bname] if branches is not None else None
            out[bname] = ak.zip(
                {
                    n: ak.packed(ak.without_parameters(events[bname][n]))
                    for n in events[bname].fields
                    if is_rootcompat(events[bname][n])
                    and (keep is None or n in keep)
                }
            )
        else:
            if is_rootcompat(events[bname]):
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



def save_skimed_dataset_definition(processing_out, fileout, check_initial_events=True):
    datasets_info = {}
    datasets_metadata = processing_out["datasets_metadata"]["by_dataset"]
    # Now add the files
    for key in datasets_metadata.keys():
        # We first check that the total number of initial events
        # corresponds to the initial number of the events in the metadata
        # to check if we are not missing any event
        if check_initial_events and  int(datasets_metadata[key]["nevents"]) != processing_out["cutflow"]["initial"][key]:
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

      
        
    # Save the json
    with open(fileout, "w") as f:
        json.dump(datasets_info, f, indent=4)
