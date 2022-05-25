import correctionlib

from ..parameters.pileup import pileupJSONfiles

def sf_pileup_reweight(events, year):
    puFile = pileupJSONfiles[year]['file']
    puName = pileupJSONfiles[year]['name']

    puWeightsJSON = correctionlib.CorrectionSet.from_file(puFile)

    sf     = puWeightsJSON[puName].evaluate(events.Pileup.nPU.to_numpy(), 'nominal')
    sfup   = puWeightsJSON[puName].evaluate(events.Pileup.nPU.to_numpy(), 'up')
    sfdown = puWeightsJSON[puName].evaluate(events.Pileup.nPU.to_numpy(), 'down')

    return sf, sfup, sfdown
