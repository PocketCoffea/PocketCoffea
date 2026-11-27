import awkward as ak
import numpy as np


def event_uniform(event, luminosityBlock):
    '''Deterministic uniform number in [0, 1) for each event, from a splitmix64 hash
    of (event, luminosityBlock). In MC `run` is always 1 and the event number is unique
    in a luminosity block: the same event gets the same number in every job and rerun.'''
    with np.errstate(over="ignore"):
        x = (ak.to_numpy(event).astype(np.uint64) * np.uint64(0x9E3779B97F4A7C15)
             ^ ak.to_numpy(luminosityBlock).astype(np.uint64) * np.uint64(0xC2B2AE3D27D4EB4F))
        x = (x ^ (x >> np.uint64(30))) * np.uint64(0xBF58476D1CE4E5B9)
        x = (x ^ (x >> np.uint64(27))) * np.uint64(0x94D049BB133111EB)
        x = x ^ (x >> np.uint64(31))
    return (x >> np.uint64(11)).astype(np.float64) / 2.0**53


def mc_split_mask(events, year, params):
    '''Mask of the MC events that belong to `year`, when the same MC sample is used
    for more than one data-taking year (parameter `mc_split`: list of groups of years).

    Each event goes to exactly one year of the group, with a probability proportional
    to `params.lumi.picobarns[year].tot`: the masks of the years of a group are
    disjoint and complete. Returns None if `year` is not in a group (no split).'''
    # str: a year without quotes in a yaml file is an integer
    groups = [g for g in ([str(y) for y in g] for g in params.get("mc_split", [])) if year in g]
    if not groups:
        return None
    if len(groups) > 1:
        raise ValueError(f"mc_split: the year {year} is in more than one group: {groups}")
    years = groups[0]
    lumi = np.cumsum([params.lumi.picobarns[y].tot for y in years])
    edges = np.concatenate([[0.0], lumi / lumi[-1]])
    i = years.index(year)
    u = event_uniform(events.event, events.luminosityBlock)
    return (u >= edges[i]) & (u < edges[i + 1])
