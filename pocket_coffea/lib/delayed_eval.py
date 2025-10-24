import numpy as np
import awkward as ak


class DelayedEvalBranchManager:
    """
    Manage branches that are independent of shape systematics and should be
    computed only for events that pass final selections in any variation.
    Usage pattern:
      - Register branches with `register(name, compute_fn, default_value)`.
        The `compute_fn(processor, ev_subset)` must return an array (numpy/ak)
        of length len(ev_subset) with the branch values computed ON NOMINAL objects.
      - On the first (nominal) variation, call `prepare_nominal_snapshot(events)` BEFORE
        preselections are applied.
      - For each variation after categories are defined, call
        `update_for_current_variation(events, categories)` to compute for newly
        passing events and attach the cached values to current `events`.
    """

    def __init__(self, processor):
        self.processor = processor
        self.registered = {}  # name -> dict(compute_fn, default)
        self.prepared = False

    def register(self, name, compute_fn, default_value: float = 1.0):
        """Register a branch or a group of branches for delayed computation.
        - Single: name is a str and compute_fn returns 1D array.
        - Group: name is a list/tuple of names and compute_fn returns a dict {name: array}.
        """
        if isinstance(name, (list, tuple)):
            key = tuple(name)
            self.registered[key] = {
                "compute_fn": compute_fn,
                "default": default_value,
                "multi": True,
                "names": list(name),
            }
        else:
            self.registered[name] = {
                "compute_fn": compute_fn,
                "default": default_value,
                "multi": False,
            }

    def ensure_nominal_index(self, events):
        """Attach a stable nominal index column used for vectorized mapping across variations."""
        idx_name = "__poco_nom_idx_delayed_eval__"
        if idx_name not in ak.fields(events):
            # Use a simple per-event counter on the outer axis; ensures stable mapping across variations
            events[idx_name] = ak.Array(np.arange(len(events), dtype=np.int64))
        return idx_name

    def prepare_nominal_snapshot(self, events):
        if self.prepared or len(self.registered) == 0:
            return
        self.nominal_events = events
        # Ensure a persistent nominal index column is present
        self.nom_idx_field = self.ensure_nominal_index(events)
        n = len(events)
        # Per-branch caches and bookkeeping
        self.cache = {}
        self.computed_nom_mask = np.zeros(n, dtype=bool)
        for key, cfg in self.registered.items():
            if cfg.get("multi", False):
                for nm in cfg["names"]:
                    self.cache[nm] = np.full(n, cfg["default"], dtype=float)
            else:
                self.cache[key] = np.full(n, cfg["default"], dtype=float)
        self.prepared = True

    def union_final_categories_mask(self, events, categories):
        # Build union-of-final-category mask using the categories masks
        union = events.event != events.event  # False
        for _, mask in categories.get_masks():
            if getattr(categories, "is_multidim", False) and getattr(mask, "ndim", 1) > 1:
                mask_on_events = ak.any(mask, axis=1)
            else:
                mask_on_events = mask
            union = np.logical_or(union, mask_on_events)
        return union

    def update_for_current_variation(self, events, categories):
        if not self.prepared or len(self.registered) == 0:
            return

        # Which current events pass final selections in this variation
        passing_any = self.union_final_categories_mask(events, categories)
        # Nominal indices for the current (filtered) events are carried along as a field
        idxs_curr = ak.to_numpy(events[self.nom_idx_field])
        valid = idxs_curr >= 0

        # Newly passing nominal indices
        passing_idxs = idxs_curr[passing_any]
        passing_valid = passing_idxs >= 0
        nom_idxs_pass = passing_idxs[passing_valid]
        if nom_idxs_pass.size > 0:
            # Filter to not-yet-computed and unique indices
            not_done_mask = ~self.computed_nom_mask[nom_idxs_pass]
            nom_idxs_new = np.unique(nom_idxs_pass[not_done_mask])
            if nom_idxs_new.size > 0:
                ev_sub = self.nominal_events[nom_idxs_new]
                for key, cfg in self.registered.items():
                    if cfg.get("multi", False):
                        out = cfg["compute_fn"](self.processor, ev_sub)
                        for nm in cfg["names"]:
                            vals_np = ak.to_numpy(out[nm])
                            self.cache[nm][nom_idxs_new] = vals_np
                    else:
                        vals = cfg["compute_fn"](self.processor, ev_sub)
                        vals_np = ak.to_numpy(vals)
                        self.cache[key][nom_idxs_new] = vals_np
                # Mark as computed
                self.computed_nom_mask[nom_idxs_new] = True

        # Attach cached values onto current events for all registered branches
        n_curr = len(events)
        valid = idxs_curr >= 0

        for key, cfg in self.registered.items():
            if cfg.get("multi", False):
                for nm in cfg["names"]:
                    out = np.full(n_curr, cfg["default"], dtype=float)
                    out[valid] = self.cache[nm][idxs_curr[valid]]
                    events[nm] = ak.Array(out)
            else:
                out = np.full(n_curr, cfg["default"], dtype=float)
                out[valid] = self.cache[key][idxs_curr[valid]]
                events[key] = ak.Array(out)