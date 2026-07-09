"""Process-local cache of parsed correctionlib CorrectionSets.

The correction JSONs (b-tagging, JERC, lepton SFs, pileup, jet-veto maps, ...) are looked
up by year/era through ``params[...][year][...]["file"]``, so the resolved file path
already encodes the chunk metadata. Parsing a multi-MB JSON with
``correctionlib.CorrectionSet.from_file`` is expensive, and it is done far more often than
necessary: the ``WeightsManager`` and its wrappers are rebuilt per chunk and ``compute()``
re-runs once per shape variation, so each SF JSON is currently re-parsed once per
(chunk x variation).

Caching on the resolved path fixes both: the same file requested for many chunks and
variations is parsed once per worker process. Because the path already carries the
year/era, chunks with different metadata resolve to different paths and get the right file,
while chunks that share a year hit the cache.

The cache lives at module scope, so it is per worker *process* (``from_file`` objects are
never pickled). It is unbounded because the distinct-file set is small -- a handful of
JSONs per year a worker sees -- and a bounded LRU could evict and re-parse a recurring
file for no memory benefit.

The returned evaluator is SHARED, so callers must treat it as read-only (all SF/scale
consumers only call ``.evaluate()``). The JER correction set, which is filtered in place,
is handled by its own memoized builder in ``jets.get_jer_correction_set``.
"""
import functools

import correctionlib


@functools.lru_cache(maxsize=None)
def load_correction_set(path):
    """Return the correctionlib evaluator for ``path``, parsed once per process."""
    return correctionlib.CorrectionSet.from_file(path)
