"""Split-histogram output format for low-memory plotting.

The default PocketCoffea/coffea output is a single monolithic ``cloudpickle``
blob (``coffea.util.save``): opening it with ``coffea.util.load`` always
deserialises *every* histogram into memory at once, because pickle has no index
and no random access.  For large analyses (many variables x many systematic
variations) this costs gigabytes of RAM just to plot a handful of variables.

This module defines an alternative **split** on-disk format that stores each
variable in its own, independently loadable member so that consumers (e.g. the
plotter) can read **one variable at a time** and never hold the whole file.

Layout (a single ``.zip`` container, kept with the ``.coffea`` name so it is a
drop-in replacement; it is auto-detected by its magic bytes)::

    output_all.coffea                # a zip (magic PK\\x03\\x04)
    |-- format.json                  # human-readable index (see below)
    |-- metadata                     # cloudpickle of the output dict WITHOUT
    |                                #  'variables'/'columns' (small; datasets_metadata,
    |                                #  cutflow, sumw, sumw2, sum_genweights, ...)
    |-- variables/<var>              # cloudpickle of {sample:{dataset:hist.Hist}}
    `-- columns/<sample>             # optional; cloudpickle of the per-sample columns

``format.json`` carries the format name/version, the list of variables, columns
and categories, and the per-member compression used.  Member file names are
``urllib.parse.quote``-encoded so any character in a variable/sample name is
safe.

Public API
----------
- :func:`save_split`      -- write an output dict to the split format.
- :class:`SplitOutput`    -- lazy reader (``.metadata``, ``.variables``,
                             ``.categories``, ``.get_variable(name)``).
- :func:`is_split_output` -- sniff whether a path is a split file.
- :func:`load_metadata`   -- read the metadata (no histograms) of either format.
- :func:`to_monolithic`   -- reconstruct the full in-memory dict (compat).

The code uses ``cloudpickle`` / ``zipfile`` directly (no coffea-version-specific
API) so it works across coffea versions (validated against coffea 0.7.x).
"""

import io
import json
import os
import zipfile
from urllib.parse import quote, unquote

import cloudpickle

try:  # lz4 is a coffea dependency; fall back gracefully if unavailable
    import lz4.frame as _lz4frame
except Exception:  # pragma: no cover - environment without lz4
    _lz4frame = None

__all__ = [
    "save_split",
    "SplitOutput",
    "is_split_output",
    "load_metadata",
    "to_monolithic",
    "load_output_auto",
    "MonolithicProvider",
    "SplitProvider",
    "make_provider",
    "FORMAT_NAME",
    "FORMAT_VERSION",
]

FORMAT_NAME = "pocketcoffea-split"
FORMAT_VERSION = 1

# Keys of the output dict that are stored as their own (per-entry) members
# instead of being lumped into the small `metadata` member.
_SPLIT_KEYS = ("variables", "columns")

_ZIP_MAGIC = b"PK\x03\x04"
_FORMAT_MEMBER = "format.json"
_METADATA_MEMBER = "metadata"
_VARIABLES_PREFIX = "variables/"
_COLUMNS_PREFIX = "columns/"


# ---------------------------------------------------------------------------
# compression helpers
# ---------------------------------------------------------------------------
def _resolve_compression(compression):
    """Return (member_compression, zip_compression).

    - "lz4"  -> lz4-compress each member, store uncompressed in the zip.
    - "none" -> raw cloudpickle members, let the zip deflate them.
    Falls back to "none"+DEFLATED if lz4 is requested but unavailable.
    """
    if compression == "lz4" and _lz4frame is None:
        compression = "none"
    if compression == "lz4":
        return "lz4", zipfile.ZIP_STORED
    if compression in (None, "none"):
        return "none", zipfile.ZIP_DEFLATED
    raise ValueError(f"Unknown compression {compression!r}. Use 'lz4' or 'none'.")


def _encode_member(obj, member_compression):
    data = cloudpickle.dumps(obj)
    if member_compression == "lz4":
        return _lz4frame.compress(data)
    return data


def _decode_member(raw, member_compression):
    if member_compression == "lz4":
        raw = _lz4frame.decompress(raw)
    return cloudpickle.loads(raw)


# ---------------------------------------------------------------------------
# introspection helpers
# ---------------------------------------------------------------------------
def _extract_categories(variables):
    """Read the list of categories from the 'cat' axis of any one histogram."""
    for samples in variables.values():
        for datasets in samples.values():
            for h in datasets.values():
                for ax in h.axes:
                    if getattr(ax, "name", None) == "cat":
                        return list(ax)
                return []
    return []


# ---------------------------------------------------------------------------
# writing
# ---------------------------------------------------------------------------
def save_split(output, path, compression="lz4"):
    """Write an accumulator ``output`` dict to the split format at ``path``.

    ``output`` is the usual PocketCoffea output dict
    (``{'variables': {var: {sample: {dataset: hist.Hist}}}, 'datasets_metadata': ..., ...}``).
    Only ``variables`` and ``columns`` are split into per-entry members; every
    other key goes into the small ``metadata`` member.
    """
    member_compression, zip_compression = _resolve_compression(compression)

    variables = output.get("variables", {}) or {}
    columns = output.get("columns", {}) or {}
    present_split_keys = [k for k in _SPLIT_KEYS if k in output]
    metadata = {k: v for k, v in output.items() if k not in _SPLIT_KEYS}

    fmt = {
        "format": FORMAT_NAME,
        "version": FORMAT_VERSION,
        "compression": member_compression,
        "split_keys_present": present_split_keys,
        "variables": list(variables.keys()),
        "columns": list(columns.keys()),
        "categories": _extract_categories(variables),
    }

    # write to a temporary path then atomically replace, so an interrupted
    # write never leaves a half-written file that looks valid.
    tmp_path = f"{path}.tmp-{os.getpid()}"
    try:
        with zipfile.ZipFile(tmp_path, "w", compression=zip_compression, allowZip64=True) as zf:
            zf.writestr(_FORMAT_MEMBER, json.dumps(fmt, indent=2))
            zf.writestr(_METADATA_MEMBER, _encode_member(metadata, member_compression))
            for var, h_dict in variables.items():
                zf.writestr(_VARIABLES_PREFIX + quote(var, safe=""),
                            _encode_member(h_dict, member_compression))
            for sample, c_dict in columns.items():
                zf.writestr(_COLUMNS_PREFIX + quote(sample, safe=""),
                            _encode_member(c_dict, member_compression))
        os.replace(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
    return path


# ---------------------------------------------------------------------------
# reading
# ---------------------------------------------------------------------------
def is_split_output(path):
    """Return True if ``path`` is a split-format file (a zip container).

    Monolithic coffea files are lz4 frames (magic ``\\x04\\x22\\x4d\\x18``);
    split files are zips (magic ``PK\\x03\\x04``), so the first bytes are a
    reliable discriminator.
    """
    try:
        with open(path, "rb") as f:
            return f.read(4) == _ZIP_MAGIC
    except OSError:
        return False


class SplitOutput:
    """Lazy reader for a split-format output file.

    Only ``format.json`` is read at construction. ``metadata`` is read (and
    cached) on first access; each ``get_variable`` opens its own read-only
    ``ZipFile`` handle, which makes the reader safe to use from forked worker
    processes (no shared file descriptor).
    """

    def __init__(self, path):
        self.path = path
        with zipfile.ZipFile(path) as zf:
            self._format = json.loads(zf.read(_FORMAT_MEMBER).decode("utf-8"))
        if self._format.get("format") != FORMAT_NAME:
            raise ValueError(
                f"{path} is not a {FORMAT_NAME} file (format={self._format.get('format')!r})."
            )
        self._member_compression = self._format.get("compression", "lz4")
        self._metadata = None

    # -- index (cheap, from format.json) --
    @property
    def variables(self):
        return list(self._format.get("variables", []))

    @property
    def columns(self):
        return list(self._format.get("columns", []))

    @property
    def categories(self):
        return list(self._format.get("categories", []))

    @property
    def version(self):
        return self._format.get("version")

    # -- payloads --
    @property
    def metadata(self):
        """The output dict without 'variables'/'columns' (loaded once, cached)."""
        if self._metadata is None:
            with zipfile.ZipFile(self.path) as zf:
                raw = zf.read(_METADATA_MEMBER)
            self._metadata = _decode_member(raw, self._member_compression)
        return self._metadata

    def get_variable(self, name):
        """Return ``{sample: {dataset: hist.Hist}}`` for a single variable."""
        member = _VARIABLES_PREFIX + quote(name, safe="")
        with zipfile.ZipFile(self.path) as zf:
            raw = zf.read(member)
        return _decode_member(raw, self._member_compression)

    def get_columns(self, sample):
        """Return the columns subtree for a single sample."""
        member = _COLUMNS_PREFIX + quote(sample, safe="")
        with zipfile.ZipFile(self.path) as zf:
            raw = zf.read(member)
        return _decode_member(raw, self._member_compression)

    def to_monolithic(self):
        """Reconstruct the full in-memory output dict (loads everything)."""
        out = dict(self.metadata)
        present = self._format.get("split_keys_present", list(_SPLIT_KEYS))
        if "variables" in present:
            out["variables"] = {v: self.get_variable(v) for v in self.variables}
        if "columns" in present:
            out["columns"] = {s: self.get_columns(s) for s in self.columns}
        return out


# ---------------------------------------------------------------------------
# format-agnostic convenience helpers
# ---------------------------------------------------------------------------
def load_metadata(path):
    """Return the output metadata (everything except variables/columns).

    Cheap for split files (reads the small ``metadata`` member only). For a
    monolithic file this must fall back to a full ``coffea.util.load`` and then
    strip the histograms, so it is *not* cheap there.
    """
    if is_split_output(path):
        return SplitOutput(path).metadata
    from coffea.util import load
    out = load(path)
    return {k: v for k, v in out.items() if k not in _SPLIT_KEYS}


def to_monolithic(source):
    """Return a full monolithic output dict from a split file/reader or a path.

    Accepts a :class:`SplitOutput`, or a path to either a split or a monolithic
    file (a monolithic path is simply loaded as-is).
    """
    if isinstance(source, SplitOutput):
        return source.to_monolithic()
    if isinstance(source, (str, os.PathLike)):
        if is_split_output(source):
            return SplitOutput(source).to_monolithic()
        from coffea.util import load
        return load(source)
    raise TypeError(f"Cannot convert {type(source)!r} to a monolithic output.")


def load_output_auto(path):
    """Load either format, always returning a full monolithic dict.

    Provided for ad-hoc scripts that expect the classic dict. Prefer
    :class:`SplitOutput` / providers when you want the low-memory behaviour.
    """
    return to_monolithic(path)


# ---------------------------------------------------------------------------
# variable providers (the low-memory plotting abstraction)
# ---------------------------------------------------------------------------
# A provider exposes a uniform interface over the two on-disk formats so the
# plotter can pull histograms one variable at a time:
#   .variables          -> list[str]
#   .categories         -> list[str]
#   .datasets_metadata  -> dict
#   .get_variable(name) -> {sample: {dataset: hist.Hist}}
# MonolithicProvider keeps the full dict in memory (loaded once); SplitProvider
# reads each variable from disk on demand, so peak memory is bounded to the
# variables currently being plotted.

class MonolithicProvider:
    """Provider backed by a fully-loaded monolithic output dict."""

    kind = "monolithic"

    def __init__(self, output):
        self._output = output

    @property
    def variables(self):
        return list(self._output["variables"].keys())

    @property
    def datasets_metadata(self):
        return self._output["datasets_metadata"]

    @property
    def categories(self):
        return _extract_categories(self._output["variables"])

    def get_variable(self, name):
        return self._output["variables"][name]

    def restrict_variables(self, keep):
        """Drop the histograms of variables not in ``keep`` to free memory."""
        keep = set(keep)
        vd = self._output["variables"]
        for v in [v for v in vd if v not in keep]:
            del vd[v]


class SplitProvider:
    """Provider backed by one or more split-format files (read per variable).

    With several input files (e.g. plotting multiple split outputs) a variable
    is read from each file that contains it and accumulated on the fly, mirroring
    the old ``accumulate(files)`` behaviour but only one variable at a time.
    """

    kind = "split"

    def __init__(self, paths):
        if isinstance(paths, (str, os.PathLike)):
            paths = [paths]
        self._readers = [SplitOutput(p) for p in paths]
        self._reader_var_sets = [set(r.variables) for r in self._readers]
        # ordered union of variable names across files
        self._variables = list(dict.fromkeys(
            v for r in self._readers for v in r.variables
        ))
        self._datasets_metadata = None

    @property
    def variables(self):
        return list(self._variables)

    @property
    def categories(self):
        return list(dict.fromkeys(
            c for r in self._readers for c in r.categories
        ))

    @property
    def datasets_metadata(self):
        if self._datasets_metadata is None:
            mds = [r.metadata.get("datasets_metadata", {}) for r in self._readers]
            if len(mds) == 1:
                self._datasets_metadata = mds[0]
            else:
                from coffea.processor import accumulate
                self._datasets_metadata = accumulate(mds)
        return self._datasets_metadata

    def get_variable(self, name):
        parts = [
            r.get_variable(name)
            for r, vset in zip(self._readers, self._reader_var_sets)
            if name in vset
        ]
        if not parts:
            raise KeyError(name)
        if len(parts) == 1:
            return parts[0]
        from coffea.processor import accumulate
        return accumulate(parts)

    def restrict_variables(self, keep):
        """No-op: variables are read lazily from disk, nothing to free."""
        return


def make_provider(paths):
    """Build the right provider for a list of input files.

    If *every* input is a split file, returns a :class:`SplitProvider` (lazy,
    low-memory). Otherwise loads/accumulates the monolithic inputs incrementally
    (dropping each file after adding it) and returns a :class:`MonolithicProvider`.
    """
    if isinstance(paths, (str, os.PathLike)):
        paths = [paths]
    paths = list(paths)
    if paths and all(is_split_output(p) for p in paths):
        return SplitProvider(paths)

    # monolithic (or mixed) path: incremental accumulate + drop to avoid the
    # "all files + a merged copy" double-buffer of the old loader.
    from coffea.util import load
    from coffea.processor import accumulate
    accumulator = None
    for p in paths:
        part = to_monolithic(p) if is_split_output(p) else load(p)
        accumulator = part if accumulator is None else accumulate([accumulator, part])
        del part
    if accumulator is None:
        raise ValueError("No input files provided to make_provider.")
    return MonolithicProvider(accumulator)
