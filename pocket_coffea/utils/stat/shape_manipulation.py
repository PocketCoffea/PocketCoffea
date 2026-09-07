"""Utilities to inject artificial shape variations into pocket_coffea histograms.

These helpers operate on the histogram dictionaries stored in the coffea output
(``df["variables"][variable] -> {sample: {dataset: hist.Hist}}``) *before* they
are handed to :class:`pocket_coffea.utils.stat.combine.Datacard`, by adding new
entries to the ``variation`` StrCategory axis. The new entries can then be
declared as regular ``shape`` :class:`SystematicUncertainty` in the datacard.

Two flavors are provided:

- :func:`add_norm_variation`: Up = nominal scaled by one factor per category
  (pure normalization effect, all bins including under/overflow).
- :func:`add_binwise_variation`: Up = nominal reweighted bin by bin with a
  per-category weight array (generic shape reweighting); a scalar entry behaves
  like a normalization factor.
"""

import hist
import numpy as np


def _resolve_bin_weights(weight_spec, variable_axis, category):
    """Normalize a weight specification to an array over the ``flow=True`` bins
    of the (last) variable axis.

    - A scalar broadcasts to every bin, under/overflow included (pure
      normalization of the category).
    - An array of length ``variable_axis.size`` weights the in-range bins;
      under/overflow keep weight 1.0.
    - An array of length ``variable_axis.extent`` also weights the flow slots
      explicitly (underflow first, when the axis has one).
    """
    if np.ndim(weight_spec) == 0:
        return np.full(variable_axis.extent, float(weight_spec))
    weights = np.asarray(weight_spec, dtype=float)
    if weights.ndim != 1:
        raise ValueError(
            f"Weights for category {category!r} must be a scalar or a 1D array, "
            f"got shape {weights.shape}"
        )
    if len(weights) == variable_axis.extent:
        return weights
    if len(weights) == variable_axis.size:
        underflow = int(variable_axis.traits.underflow)
        overflow = int(variable_axis.traits.overflow)
        return np.concatenate([np.ones(underflow), weights, np.ones(overflow)])
    raise ValueError(
        f"Weights for category {category!r} have length {len(weights)}, expected "
        f"{variable_axis.size} (in-range bins of axis {variable_axis.name!r}, "
        f"flow bins keep weight 1) or {variable_axis.extent} (flow slots included)"
    )


def add_binwise_variation(
    histograms: dict[str, dict[str, hist.Hist]],
    variation_name: str,
    samples: list[str],
    weights_by_category: dict,
    default_weight=1.0,
    down_mode: str = "nominal",
    preserve_norm: bool = False,
    datasets: list[str] | None = None,
) -> dict[str, dict[str, hist.Hist]]:
    """Add an artificial per-bin reweighting variation to the variation axis.

    For every histogram of the requested ``samples``, two new entries are
    appended to the ``variation`` axis:

    - ``{variation_name}Up``: the nominal content multiplied, in each category,
      bin by bin along the (last) variable axis by the weights given in
      ``weights_by_category`` (variances scale with the squared weight).
    - ``{variation_name}Down``: by default an exact copy of the nominal
      (``down_mode="nominal"``, i.e. a one-sided variation). With
      ``down_mode="mirror"`` the nominal is instead divided by the same weights
      (log-mirror, matching the lnN convention; requires strictly positive
      weights).

    The weights refer to the histogram's own binning **before** any
    ``bins_edges`` rebinning done by the ``Datacard`` (rebinning then merges the
    reweighted contents). See :func:`_resolve_bin_weights` for the accepted
    weight formats: scalar (all bins, flow included), in-range array (flow
    untouched), or flow-inclusive array. Categories not listed get
    ``default_weight`` (default 1.0: Up = nominal there).

    Samples not listed in ``samples`` are left untouched (their histograms keep
    the original variation axis; :meth:`Datacard.rearrange_histograms` falls
    back to the nominal for them). The input dictionary is not modified: a new
    outer dict is returned, sharing the untouched histogram objects with the
    input.

    Typical use — reweight the ttbb shape as a function of the fit variable in
    the signal region only::

        histograms = add_binwise_variation(
            histograms,
            variation_name="ttbb_shape_rw",
            samples=[s for s in histograms if s.startswith("TTBB")],
            weights_by_category={"SR": weights_array},  # len = n bins of the variable
        )

    and declare ``SystematicUncertainty(name="ttbb_shape_rw", typ="shape", ...)``.

    .. note::
       If the affected process has a free rateParam and the datacard is built
       with ``shape_only_for_rateparam=True``, the overall yield change of the
       reweighting is removed with the common factor summed over all
       ``rateparam_norm_categories``: only the bin-by-bin shape and the
       relative differences between categories survive.

    .. warning::
       When several per-category datacards are combined, the shape-only factor
       is only identical across cards if the varied *total yield per category*
       is the same in every card's variable — automatic for scalar weights, but
       for per-bin weights it requires the arrays applied to the different
       variables to represent the same underlying event-level reweighting. Use
       ``preserve_norm=True`` to sidestep this entirely: the varied totals then
       equal the nominal ones per category everywhere, the rateParam factor is
       exactly 1 in every card, and the variation need not be injected into the
       other cards' variables at all (they fall back to nominal, which is the
       same thing). Otherwise, if the weights are known only versus one
       observable, apply at least the resulting per-category yield ratio (a
       scalar) to the other cards' histograms so the category totals agree.

    :param histograms: pocket_coffea histograms, ``{sample: {dataset: hist.Hist}}``
        with axes ``(cat, variation, ..., variable)``; weights act on the last axis.
    :param variation_name: name of the new variation; entries ``{name}Up`` and
        ``{name}Down`` are created. Must match the ``name`` (or
        ``coffea_name_alias``) of the SystematicUncertainty declared for it.
    :param samples: samples to which the variation is added. Must all be present
        in ``histograms``.
    :param weights_by_category: mapping ``category -> weights`` (scalar or
        per-bin array) applied to the nominal to build the Up template. Every
        key must exist on the ``cat`` axis of the histograms.
    :param default_weight: weight specification for categories not listed in
        ``weights_by_category``, defaults to 1.0.
    :param down_mode: ``"nominal"`` (default, one-sided: Down = nominal) or
        ``"mirror"`` (Down = nominal / weights).
    :param preserve_norm: if True, rescale each varied template — per dataset
        and per category, flow bins included — so its total yield matches the
        nominal one, making the variation a pure within-category shape effect
        with no normalization component (the common rescaling is applied to all
        bins, flow included, like an event-level weight would be). Guarantees
        yield neutrality exactly for every dataset, which hand-tuned weight
        arrays cannot (neutrality depends on the nominal distribution they
        multiply). Scalar weight entries then become no-ops by construction.
        Defaults to False.
    :param datasets: optionally restrict the injection to these datasets
        (within the selected samples); histograms of other datasets are carried
        over untouched. Use it to apply different weights per year by calling
        the function once per year with that year's datasets::

            for year, weights in weights_by_year.items():
                year_datasets = [
                    d
                    for s in samples
                    for d in datasets_metadata["by_datataking_period"][year].get(s, [])
                ]
                histograms = add_binwise_variation(
                    histograms, "rw", samples, {"SR": weights},
                    preserve_norm=True, datasets=year_datasets,
                )

        Repeated calls with the same ``variation_name`` are allowed as long as
        they touch disjoint datasets (overlaps raise "already present").
        Datasets in the list that are absent from the histogram dicts are
        ignored (e.g. empty datasets), but if nothing matches at all a
        ValueError is raised. Datasets never covered by any call simply fall
        back to nominal in the Datacard. Defaults to None (all datasets).
    :return: new ``{sample: {dataset: hist.Hist}}`` dict with the extended
        variation axis on the requested samples.
    """
    if down_mode not in ("nominal", "mirror"):
        raise ValueError(
            f"Unknown down_mode {down_mode!r}, expected 'nominal' or 'mirror'"
        )

    missing_samples = sorted(set(samples) - set(histograms))
    if missing_samples:
        raise ValueError(
            f"Samples {missing_samples} not found in histograms "
            f"(available: {sorted(histograms)})"
        )

    up_name = f"{variation_name}Up"
    down_name = f"{variation_name}Down"

    dataset_filter = None if datasets is None else set(datasets)
    matched_datasets = 0

    new_histograms = dict(histograms)
    for sample in set(samples):
        new_histograms[sample] = {}
        for dataset, histogram in histograms[sample].items():
            if dataset_filter is not None and dataset not in dataset_filter:
                # carried over untouched; a later call (same variation_name,
                # different weights) can cover it
                new_histograms[sample][dataset] = histogram
                continue
            matched_datasets += 1
            axis_names = [axis.name for axis in histogram.axes]
            if "cat" not in axis_names or "variation" not in axis_names:
                raise ValueError(
                    f"Histogram for sample {sample}, dataset {dataset} has no "
                    f"'cat'/'variation' axes (axes: {axis_names}); "
                    "is this a data histogram?"
                )
            try:
                storage_type = histogram.storage_type
            except AttributeError:  # hist < 2.8
                storage_type = histogram._storage_type
            if storage_type() != hist.storage.Weight():
                raise NotImplementedError(
                    f"Histogram for sample {sample}, dataset {dataset} does not "
                    "use Weight storage"
                )
            cat_index = axis_names.index("cat")
            variation_index = axis_names.index("variation")
            cat_axis = histogram.axes["cat"]
            variation_axis = histogram.axes["variation"]
            variable_axis = histogram.axes[-1]

            unknown_categories = sorted(set(weights_by_category) - set(cat_axis))
            if unknown_categories:
                raise ValueError(
                    f"Categories {unknown_categories} not found on the 'cat' axis "
                    f"of sample {sample}, dataset {dataset} "
                    f"(available: {list(cat_axis)})"
                )
            if up_name in variation_axis or down_name in variation_axis:
                raise ValueError(
                    f"Variation {variation_name!r} already present in histogram "
                    f"for sample {sample}, dataset {dataset}"
                )
            if "nominal" not in variation_axis:
                raise ValueError(
                    f"No 'nominal' variation in histogram for sample {sample}, "
                    f"dataset {dataset}"
                )

            new_variation_axis = hist.axis.StrCategory(
                list(variation_axis) + [up_name, down_name],
                name="variation",
                label=variation_axis.label,
            )
            new_axes = list(histogram.axes)
            new_axes[variation_index] = new_variation_axis
            new_histogram = hist.Hist(
                *new_axes, name=histogram.name, storage=storage_type()
            )

            old_view = histogram.view(flow=True)
            new_view = new_histogram.view(flow=True)

            def indexer(cat_i, variation_i, ndim=old_view.ndim):
                index = [slice(None)] * ndim
                index[cat_index] = cat_i
                index[variation_index] = variation_i
                return tuple(index)

            # Copy the existing variations (same order, first entries of the new
            # axis). Categorical axes have a trailing overflow slot in the
            # flow=True view, so slice the variation axis on both sides to drop
            # it; the (empty) overflow slots of the new view stay zero.
            copy_index = indexer(slice(None), slice(0, len(variation_axis)))
            new_view[copy_index] = old_view[copy_index]

            nominal_i = variation_axis.index("nominal")
            up_i = new_variation_axis.index(up_name)
            down_i = new_variation_axis.index(down_name)
            for category in cat_axis:
                #breakpoint()
                weights = _resolve_bin_weights(
                    weights_by_category.get(category, default_weight),
                    variable_axis,
                    category,
                )
                if down_mode == "mirror" and np.any(weights <= 0):
                    raise ValueError(
                        f"down_mode='mirror' requires positive weights, got "
                        f"minimum {weights.min()} for category {category!r}"
                    )
                cat_i = cat_axis.index(category)
                # weights broadcast along the last (variable) axis
                nominal = old_view[indexer(cat_i, nominal_i)]

                def effective_weights(raw_weights):
                    """Per-bin weights, rescaled to preserve the category yield
                    of this dataset (flow included) when preserve_norm is on."""
                    if not preserve_norm:
                        return raw_weights
                    varied_total = (nominal["value"] * raw_weights).sum()
                    nominal_total = nominal["value"].sum()
                    if varied_total == 0:
                        return raw_weights
                    return raw_weights * (nominal_total / varied_total)

                up_weights = effective_weights(weights)
                up = new_view[indexer(cat_i, up_i)]
                up["value"] = nominal["value"] * up_weights
                up["variance"] = nominal["variance"] * up_weights**2
                down = new_view[indexer(cat_i, down_i)]
                if down_mode == "nominal":
                    down["value"] = nominal["value"]
                    down["variance"] = nominal["variance"]
                else:
                    down_weights = effective_weights(1.0 / weights)
                    down["value"] = nominal["value"] * down_weights
                    down["variance"] = nominal["variance"] * down_weights**2

            new_histograms[sample][dataset] = new_histogram

    if dataset_filter is not None and matched_datasets == 0:
        raise ValueError(
            f"None of the requested datasets {sorted(dataset_filter)} found in "
            f"the histograms of samples {sorted(set(samples))}"
        )

    return new_histograms


def add_norm_variation(
    histograms: dict[str, dict[str, hist.Hist]],
    variation_name: str,
    samples: list[str],
    scale_by_category: dict[str, float],
    default_scale: float = 1.0,
    down_mode: str = "nominal",
    datasets: list[str] | None = None,
) -> dict[str, dict[str, hist.Hist]]:
    """Add an artificial per-category normalization variation to the variation axis.

    Special case of :func:`add_binwise_variation` with one scalar factor per
    category: ``{variation_name}Up`` is the nominal scaled by
    ``scale_by_category[category]`` in every bin of that category (under/overflow
    included; ``default_scale`` for categories not listed), so the bin-by-bin
    shape within a category is untouched. ``{variation_name}Down`` is the
    nominal itself by default (one-sided), or nominal divided by the factor with
    ``down_mode="mirror"``.

    Typical use — model a 4FS vs 5FS normalization difference on the ttbb
    samples::

        histograms = add_norm_variation(
            histograms,
            variation_name="4Fvs5F",
            samples=[s for s in histograms if s.startswith("TTBB")],
            scale_by_category={"CR": 1.0081, "SR": 0.9143, "CR_ttlf": 1.1309},
        )

    and declare ``SystematicUncertainty(name="4Fvs5F", typ="shape", ...)``.

    .. note::
       If the affected process has a free rateParam and the datacard is built
       with ``shape_only_for_rateparam=True``, the overall normalization of this
       variation is absorbed by the rateParam rescaling: only the *relative*
       differences between the per-category factors survive. Equal factors in
       all fit categories then make the variation a no-op.

    See :func:`add_binwise_variation` for the parameter documentation.
    """
    for category, scale in scale_by_category.items():
        if np.ndim(scale) != 0:
            raise ValueError(
                f"scale_by_category[{category!r}] must be a scalar, got "
                f"{scale!r}; use add_binwise_variation for per-bin weights"
            )
    return add_binwise_variation(
        histograms,
        variation_name=variation_name,
        samples=samples,
        weights_by_category=scale_by_category,
        default_weight=default_scale,
        down_mode=down_mode,
        datasets=datasets,
    )
