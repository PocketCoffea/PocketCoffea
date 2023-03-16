print('''
______  _                 _____ ______               _  _  _                  _    _               
| ___ \| |               /  ___||  ___|             | |(_)| |                | |  (_)              
| |_/ /| |_  __ _   __ _ \ `--. | |_      ___  __ _ | | _ | |__   _ __  __ _ | |_  _   ___   _ __  
| ___ \| __|/ _` | / _` | `--. \|  _|    / __|/ _` || || || '_ \ | '__|/ _` || __|| | / _ \ | '_ \ 
| |_/ /| |_| (_| || (_| |/\__/ /| |     | (__| (_| || || || |_) || |  | (_| || |_ | || (_) || | | |
\____/  \__|\__,_| \__, |\____/ \_|      \___|\__,_||_||_||_.__/ |_|   \__,_| \__||_| \___/ |_| |_|
                    __/ |                                                                          
                   |___/                                                                           


''')
'''Script to extract the btagSF calibration and plot the shapes comparison.
The script works on histograms with (sample,category,year) axes and it expect the following
categories to be present:
- no_btagSF: nominal shapes
- btagSF:  btagSF from bpog applied
- btagSF_calib: additional calibration applied (for the validation only).

N.B. NO btag requirements must be applied to extract the shapes used for this calibration.
The 2D variables used to extract the SF can be speficied in the options.

The SF is exported in correctionlib json format. 
'''

import numpy as np
import awkward as ak
import hist
from itertools import *
from coffea.util import load
import os

import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
import mplhep as hep

hep.style.use(hep.style.ROOT)

from pocket_coffea.utils.plot_utils import plot_shapes_comparison

import argparse

parser = argparse.ArgumentParser(description="Extract btagSF calibration and validate it with plots")
parser.add_argument("-i","--input", type=str, required=True, help="Input coffea files with shapes")
parser.add_argument("-o","--output", type=str, required=True, help="Output folder")
parser.add_argument("-v","--validate", action="store_true", help="Use this switch to plot validation shapes")
parser.add_argument("-c","--compute", action="store_true", help="Use this switch to compute the SF")
parser.add_argument("--sf-hist", type=str, help="Histogram to be used for SF computation", default="Njet_Ht")
args = parser.parse_args()

os.makedirs(args.output, exist_ok=True)
output = load(args.input)["variables"]

variables_to_plot = [
    #'jets_Ht',"jet_pt","jet_eta","nJets", "nBJets", "jet_btagDeepFlavB"
    'jets_Ht', "nJets", "nBJets",
    "jet_pt_1", "jet_eta_1", "jet_btagDeepFlavB_1",
    "jet_pt_2", "jet_eta_2", "jet_btagDeepFlavB_2",
    "jet_pt_3", "jet_eta_3", "jet_btagDeepFlavB_3",
    "jet_pt_4", "jet_eta_4", "jet_btagDeepFlavB_4",
]

samples = list(output["nJets"].keys())
years = list(output["nJets"][samples[0]].axes[2])

if args.compute:
    # Plot only shapes with and without btagSF and compute the SF
    for var, sample, year in product(variables_to_plot, samples, years):
        print(var, sample)
        shapes = [
            (sample, 'no_btagSF', year, "nominal", "no btag SF"),
            (sample, 'btagSF', year,"nominal", "btag SF"),
        ]
        plot_shapes_comparison(output, f"{var}", shapes, ylog=True,
                               lumi_label=f"{sample} {year}",
                               outputfile=f"{args.output}/hist_btagSFeffect_{year}_{var}_{sample}.*")

    # Compute the SF in one go
    ratios = [ ]
    for sample in samples:
        print("Computing SF for sample: ", sample)
        A = output[f"{args.sf_hist}"][sample]
        w_num, _, x, y = A['no_btagSF','nominal',:,:,:].to_numpy()
        num_var = A['no_btagSF','nominal',:,:,:].variances()
        w_denom, _, x, y = A['btagSF', 'nominal',:,:,:].to_numpy()
        denom_var = A['no_btagSF', 'nominal',:,:,:].variances()

        ratio= np.where( (w_denom>0)&(w_num>0),
                         w_num/w_denom,
                         1.) 
        ratio_err =  np.where( (w_denom>0)&(w_num>0),
                               np.sqrt((1/w_denom)**2 * num_var + (w_num/w_denom**2)**2 * denom_var),
                               0.)
        ratios.append((ratio, ratio_err))

    
    sample_axis = hist.axis.StrCategory(samples, name="sample", label="Sample")
    sfhist = hist.Hist(sample_axis,A.axes[2],A.axes[3],A.axes[4], data=np.stack([r[0] for r in ratios]))
    sfhist_err = hist.Hist(sample_axis,A.axes[2],A.axes[3],A.axes[4], data=np.stack([r[1] for r in ratios]))

    # Exporting it to correctionlib
    import correctionlib, rich
    import correctionlib.convert
    # without a name, the resulting object will fail validation
    sfhist.name = "btagSF_norm_correction"
    sfhist.label = "out"
    clibcorr = correctionlib.convert.from_histogram(sfhist)
    clibcorr.description = "SF to correct the overall normalization after the application of btagSF weights"
    
    # set overflow bins behavior (default is to raise an error when out of bounds)
    
    for sample_cat in clibcorr.data.content:
        for year_cat in sample_cat.value.content:
            year_cat.value.flow = "clamp"


    cset = correctionlib.schemav2.CorrectionSet(
        schema_version=2,
        description="btagSF normalization corrections",
        corrections=[clibcorr],
    )
    rich.print(cset)

    with open(f"{args.output}/btagSF_calibrationSF.json", "w") as fout:
        fout.write(cset.json(exclude_unset=True))


    #Plotting the scale factor for each sample/year
    for sample, year in product(samples, years):
        print(f"Plotting the SF for {sample} {year}")
        fig,( ax,ay) = plt.subplots(1, 2, figsize=(18, 7), dpi=100)
        plt.subplots_adjust(wspace=0.3)
        
        ax.set_title(f"{sample} {year}")
        I = hep.hist2dplot(sfhist[sample,year, :,:], ax=ax, cmap="cividis", cbarextend=True)
        ax.set_xlabel("N jets")
        ax.set_ylabel("Jet $H_T$")

        ay.set_title("stat. error")
        I = hep.hist2dplot(sfhist_err[sample,year, :,:], ax=ay, cmap="cividis", cbarextend=True)
        ay.set_xlabel("N jets")
        ay.set_ylabel("Jet $H_T$")

        fig.savefig(f"{args.output}/plot_SFoutput_{sample}_{year}.png")
        fig.savefig(f"{args.output}/plot_SFoutput_{sample}_{year}.pdf")


######################################################
if args.validate:
    # Plot the shape with validation
    for var, sample, year in product(variables_to_plot, samples, years):
        print(f"Plotting validation for {var} {sample} {year}")
        shapes = [
            (sample, 'no_btagSF', year, "nominal", "no btag SF"),
            (sample, 'btagSF', year, "nominal", "btag SF"),
            (sample, 'btagSF_calib', year, "nominal", "btag SF calibrated"),
        ]
        plot_shapes_comparison(output, f"{var}", shapes, ylog=True,
                               lumi_label=f"{sample} {year}",
                               outputfile=f"{args.output}/hist_btagSFcalibrated_{year}_{var}_{sample}.*")
