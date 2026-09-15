"""Compare two outputs of config.py (before/after the JER fix).

    python compare_outputs.py out_before/output_all.coffea out_after/output_all.coffea -o cmp

Prints, per sample and variation: the fraction of matched jets whose pt changed, the pt
ratio after/before per |eta| region and the migration of the gen-match flag (pt_gen);
saves before/after/ratio plots of the histograms in the output directory.
"""
import argparse
import os

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from coffea.util import load

ETA_EDGES = [0.0, 1.3, 2.5, 3.0, 5.0]
HISTS_1D = ["JetGood_pt", "JetGood_eta", "Jet1_pt", "Jet2_pt", "nJetGood", "MET_pt", "MET_phi"]
HISTS_2D = ["JetGood_pt_eta", "JetGood_pt_over_raw_eta"]


def jet_table(cols, coll="Jet"):
    """Per-jet arrays of one (category, variation) column output, with the event id repeated."""
    n = cols[f"{coll}_N"].value
    event = np.repeat(cols["events_event"].value, n)
    key = event.astype(np.int64) * 10_000 + cols[f"{coll}_pocket_sortidx"].value.astype(np.int64)
    return key, {c: cols[f"{coll}_{c}"].value for c in ("pt", "eta", "pt_raw", "pt_gen")}


def compare_columns(before, after, sample, dataset, category, coll="Jet"):
    cb, ca = before[category], after[category]
    for variation in sorted(set(cb) & set(ca)):
        kb, tb = jet_table(cb[variation], coll)
        ka, ta = jet_table(ca[variation], coll)
        _, ib, ia = np.intersect1d(kb, ka, return_indices=True)
        if len(ib) == 0:
            print(f"  {variation}: no matched jets"); continue
        b = {c: v[ib] for c, v in tb.items()}
        a = {c: v[ia] for c, v in ta.items()}
        ratio = a["pt"] / b["pt"]
        changed = ~np.isclose(ratio, 1.0, rtol=1e-6, atol=0)
        print(f"  {variation}: {len(ib)} matched jets ({len(kb)} before, {len(ka)} after), "
              f"pt changed for {changed.mean()*100:.2f}%")
        aeta = np.abs(b["eta"])
        for lo, hi in zip(ETA_EDGES[:-1], ETA_EDGES[1:]):
            m = (aeta >= lo) & (aeta < hi)
            if m.sum() == 0:
                continue
            r = ratio[m]
            print(f"    {lo:.1f} <= |eta| < {hi:.1f}: n={m.sum():6d}  changed={changed[m].mean()*100:5.2f}%  "
                  f"<pt_a/pt_b>={r.mean():.4f}  std={r.std():.4f}  min={r.min():.3f}  max={r.max():.3f}")
        # gen-match migration: before, 0 = no match; after, -1 = no match (also dR > R/2)
        nomatch_b, nomatch_a = b["pt_gen"] <= 0, a["pt_gen"] < 0
        print(f"    gen match: no-match before {nomatch_b.mean()*100:.1f}%, after {nomatch_a.mean()*100:.1f}%; "
              f"matched->unmatched (dR > R/2) {(~nomatch_b & nomatch_a).mean()*100:.2f}%")
        # breakdown of the changed jets: dR migration (2.1), unmatched in both = sigma > 1/3
        # bug (2.2) or clamped negative factor (2.3), negative pt before the fix (2.3)
        neg_b, neg_a = b["pt"] < 0, a["pt"] < 0
        migr = ~nomatch_b & nomatch_a
        both = nomatch_b & nomatch_a
        other = changed & ~migr & ~both
        print(f"    pt < 0: before n={neg_b.sum()} ({neg_b.mean()*100:.3f}%), after n={neg_a.sum()}")
        print(f"    changed jets: n={changed.sum()} = matched->unmatched {(changed & migr).sum()} "
              f"+ unmatched in both {(changed & both).sum()} (of {both.sum()}) + other {other.sum()}")
        for lo, hi in zip(ETA_EDGES[:-1], ETA_EDGES[1:]):
            m = (aeta >= lo) & (aeta < hi)
            if m.sum():
                print(f"      {lo:.1f} <= |eta| < {hi:.1f}: pt<0 before {neg_b[m].sum():5d}  "
                      f"changed: dR-migration {(changed & migr & m).sum():6d}  "
                      f"unmatched-both {(changed & both & m).sum():6d}  other {(other & m).sum():6d}")


def plot_1d(hb, ha, name, tag, outdir):
    ax = hb.axes[0]
    edges, centers = ax.edges, ax.centers
    vb, va = hb.values(), ha.values()
    fig, (top, bot) = plt.subplots(2, 1, figsize=(6, 6), sharex=True,
                                   gridspec_kw={"height_ratios": [3, 1], "hspace": 0.05})
    top.step(edges, np.r_[vb, vb[-1]], where="post", label="before")
    top.step(edges, np.r_[va, va[-1]], where="post", label="after", ls="--")
    top.set_ylabel("events"); top.legend(); top.set_title(f"{name} {tag}")
    with np.errstate(divide="ignore", invalid="ignore"):
        r = np.where(vb > 0, va / vb, np.nan)
    bot.plot(centers, r, "o", ms=3); bot.axhline(1, color="gray", lw=0.8)
    bot.set_ylim(0.8, 1.2); bot.set_ylabel("after/before"); bot.set_xlabel(ax.label)
    fig.savefig(os.path.join(outdir, f"{name}_{tag}.png"), dpi=120, bbox_inches="tight"); plt.close(fig)


def compare_hists(before, after, sample, dataset, category, outdir):
    for name in HISTS_1D + HISTS_2D:
        if name not in before["variables"] or name not in after["variables"]:
            continue
        hb_all = before["variables"][name][sample][dataset]
        ha_all = after["variables"][name][sample][dataset]
        # data histograms have no variation axis
        has_var = "variation" in hb_all.axes.name
        variations = list(hb_all.axes["variation"]) if has_var else ["nominal"]
        for variation in variations:
            if has_var and variation not in ha_all.axes["variation"]:
                continue
            sel = {"cat": category, **({"variation": variation} if has_var else {})}
            hb, ha = hb_all[sel], ha_all[sel]
            tag = f"{dataset}_{category}_{variation}"
            vb, va = hb.values(), ha.values()
            with np.errstate(divide="ignore", invalid="ignore"):
                maxdev = np.nanmax(np.abs(np.where(vb > 0, va / vb - 1, np.nan)))
            print(f"  {name:28s} {variation:22s} sum before={vb.sum():12.2f} after={va.sum():12.2f} "
                  f"max |after/before - 1| = {maxdev:.3f}")
            if name in HISTS_1D:
                plot_1d(hb, ha, name, tag, outdir)
            else:
                # 2D: one plot per eta bin, projected on the first axis
                eta_ax = hb.axes[1]
                for i in range(len(eta_ax.centers)):
                    lo, hi = eta_ax.edges[i], eta_ax.edges[i + 1]
                    plot_1d(hb[:, i], ha[:, i], f"{name}_eta{lo:+.1f}_{hi:+.1f}", tag, outdir)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("before"); ap.add_argument("after")
    ap.add_argument("-o", "--outdir", default="cmp_jer_fix")
    ap.add_argument("-c", "--category", default="baseline")
    args = ap.parse_args()
    os.makedirs(args.outdir, exist_ok=True)
    before, after = load(args.before), load(args.after)

    for sample in before["columns"]:
        for dataset in before["columns"][sample]:
            if sample not in after["columns"] or dataset not in after["columns"][sample]:
                print(f"{sample}/{dataset}: missing in after"); continue
            print(f"\n=== {sample} / {dataset} / {args.category}: matched-jet comparison (Jet collection)")
            compare_columns(before["columns"][sample][dataset], after["columns"][sample][dataset],
                            sample, dataset, args.category)
            print(f"\n=== {sample} / {dataset} / {args.category}: histograms")
            compare_hists(before, after, sample, dataset, args.category, args.outdir)
    print(f"\nplots in {args.outdir}/")


if __name__ == "__main__":
    main()
