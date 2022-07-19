import numpy as np
import uproot
import correctionlib

from coffea.util import load
from coffea import lookup_tools

from ..parameters.pileup import pileupJSONfiles, pileupJSONfiles_EOY

def sf_pileup_reweight(events, year):
    puFile = pileupJSONfiles[year]['file']
    puName = pileupJSONfiles[year]['name']

    puWeightsJSON = correctionlib.CorrectionSet.from_file(puFile)

    sf     = puWeightsJSON[puName].evaluate(events.Pileup.nPU.to_numpy(), 'nominal')
    sfup   = puWeightsJSON[puName].evaluate(events.Pileup.nPU.to_numpy(), 'up')
    sfdown = puWeightsJSON[puName].evaluate(events.Pileup.nPU.to_numpy(), 'down')

    return sf, sfup, sfdown

def sf_pileup_reweight_EOY(events, nTrueFile, sample, year):
    '''Based on https://github.com/andrzejnovak/coffeandbacon/blob/master/analysis/compile_corrections.py#L166-L192'''

    puFile = pileupJSONfiles_EOY[year]['file']
    nTrueIntLoad = load(nTrueFile)
    #print([y for x,y in nTrueIntLoad[sample].sum('sample').values().items()])
    nTrueInt = [y for x,y in nTrueIntLoad[sample].sum('sample').values().items()][0]  ## not sure is the best way

    with uproot.open(puFile) as file_pu:
        norm = lambda x: x / x.sum()
        data = norm(file_pu['pileup'].counts())
        mc_pu = norm(nTrueInt)
        mask = mc_pu > 0.
        corr = data.copy()
        corr[mask] /= mc_pu[mask]
        pileup_corr = lookup_tools.dense_lookup.dense_lookup(corr, file_pu["pileup"].axis().edges())
        sf = pileup_corr(events.Pileup.nPU)
    return sf
