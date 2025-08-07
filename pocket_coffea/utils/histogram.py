import hist
import numpy as np


# Taken from https://gist.github.com/kdlong/d697ee691c696724fc656186c25f8814
def rebin_hist(
    bins_edges: list[float] | int, histograms: dict[str, dict[str, hist.Hist]]
) -> dict[str, dict[str, hist.Hist]]:
    edges = bins_edges
    h_dict_new = {}
    for sample, dict_dataset in histograms.items():
        h_dict_new[sample] = {}
        for dataset, h in dict_dataset.items():
            axis_name = h.axes[-1].name
            if type(edges) is int:
                return h[{axis_name: hist.rebin(edges)}]

            ax = h.axes[axis_name]
            ax_idx = [a.name for a in h.axes].index(axis_name)
            if not all([np.isclose(x, ax.edges).any() for x in edges]):
                raise ValueError(
                    f"Cannot rebin histogram due to incompatible edges for axis '{ax.name}'\n"
                    f"Edges of histogram are {ax.edges}, requested rebinning to {edges}"
                )

            # If you rebin to a subset of initial range, keep the overflow and underflow
            overflow = ax.traits.overflow or (
                edges[-1] < ax.edges[-1] and not np.isclose(edges[-1], ax.edges[-1])
            )
            underflow = ax.traits.underflow or (
                edges[0] > ax.edges[0] and not np.isclose(edges[0], ax.edges[0])
            )
            flow = overflow or underflow
            new_ax = hist.axis.Variable(
                edges, name=ax.name, overflow=overflow, underflow=underflow
            )
            axes = list(h.axes)
            axes[ax_idx] = new_ax

            hnew = hist.Hist(*axes, name=h.name, storage=h._storage_type())

            # Offset from bin edge to avoid numeric issues
            offset = 0.5 * np.min(ax.edges[1:] - ax.edges[:-1])
            edges_eval = edges + offset
            edge_idx = ax.index(edges_eval)
            # Avoid going outside the range, reduceat will add the last index anyway
            if edge_idx[-1] == ax.size + ax.traits.overflow:
                edge_idx = edge_idx[:-1]

            if underflow:
                # Only if the original axis had an underflow should you offset
                if ax.traits.underflow:
                    edge_idx += 1
                edge_idx = np.insert(edge_idx, 0, 0)

            # Take is used because reduceat sums i:len(array) for the last entry, in the case
            # where the final bin isn't the same between the initial and rebinned histogram, you
            # want to drop this value. Add tolerance of 1/2 min bin width to avoid numeric issues
            hnew.values(flow=flow)[...] = np.add.reduceat(
                h.values(flow=flow), edge_idx, axis=ax_idx
            ).take(indices=range(new_ax.size + underflow + overflow), axis=ax_idx)
            if hnew._storage_type() == hist.storage.Weight():
                hnew.variances(flow=flow)[...] = np.add.reduceat(
                    h.variances(flow=flow), edge_idx, axis=ax_idx
                ).take(indices=range(new_ax.size + underflow + overflow), axis=ax_idx)
            h_dict_new[sample][dataset] = hnew
    return h_dict_new
