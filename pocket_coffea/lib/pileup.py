import correctionlib

from ..parameters.pileup import pileupJSONfiles


def sf_pileup_reweight(events, year):
    puFile = pileupJSONfiles[year]['file']
    puName = pileupJSONfiles[year]['name']

    puWeightsJSON = correctionlib.CorrectionSet.from_file(puFile)

    nPu = events.Pileup.nPU.to_numpy()
    sf = puWeightsJSON[puName].evaluate(nPu, 'nominal')
    sfup = puWeightsJSON[puName].evaluate(nPu, 'up')
    sfdown = puWeightsJSON[puName].evaluate(nPu, 'down')

    return sf, sfup, sfdown
