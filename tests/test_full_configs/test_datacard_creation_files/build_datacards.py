import json
import logging
import os
import numpy as np
import re

import hist
from coffea.util import load
from pocket_coffea.utils.stat import (
    Datacard,
    DataProcess,
    DataProcesses,
    MCProcess,
    MCProcesses,
    Systematics,
    SystematicUncertainty,
)


logging.basicConfig(format='%(asctime)s,%(msecs)03d %(name)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)
logger = logging.getLogger()

def get_year(pocketcoffea_year, year_type=0):
    """Get year string for datacard from PocketCoffea version.

    :param year_type: There are three different ways, how year is used.
                      - separated by pre/post eras (year_type=0)
                      - actual year (year_type=1)
                      - separated by parking/no-parking (year_type=2)
    """
    # Different categories for how to separated years/datataking periods
    dict_year_type = [
            {"2022_preEE": "2022", "2022_postEE": "2022EE", "2023_preBPix": "2023", "2023_postBPix": "2023BPIX"},
            {"2022_preEE": "2022", "2022_postEE": "2022", "2023_preBPix": "2023", "2023_postBPix": "2023"},
            {"2022_preEE": "2022", "2022_postEE": "2022", "2023_preBPix": "2023", "2023_postBPix": "2023ParkingHH"}  # <-- Probably wrong. But used for background datadriven
            ]
    try:
        return dict_year_type[year_type][pocketcoffea_year]
    except:
        raise ValueError(f"Wrong values for either input year: {pocketcoffea_year} or year_type: {year_type}")


def get_uncertainty_name(pocketcoffea_name, year):
    """Get conventional datacard name for systematic uncertainties"""
    # Try to remove unnecessary things in front (e.g. "AK4PFPuppi")
    if "Puppi" in pocketcoffea_name:
        pocketcoffea_name = pocketcoffea_name.split("_", 1)[-1]
    unc_map = {
        "JES_Total": f"CMS_scale_j_{get_year(year, year_type=0)}",
        "JER": f"CMS_res_j_{get_year(year, year_type=0)}",
        "pileup": f"CMS_pileup_{get_year(year, year_type=0)}",
        "luminosity": f"lumi_13TeV_{get_year(year, year_type=1)}",
        "sf_ele_reco": "CMS_eff_e_reco_13p6TeV",
        "sf_ele_id": "CMS_eff_e_id_13p6TeV",
        "sf_mu_reco": f"CMS_eff_m_reco_{get_year(year, year_type=0)}",
        "sf_mu_id": "CMS_eff_m_id_total",
        "sf_mu_iso": "CMS_eff_m_iso_total",
        }
    if pocketcoffea_name in unc_map.keys():
        return unc_map[pocketcoffea_name]
    # Variations connected to b-tag SF
    elif "btag" in pocketcoffea_name:
        if "SF" in pocketcoffea_name:
            btype = "fixedWP"
        else:
            btype = "fullShape"
        variation = pocketcoffea_name.split("_")[-1]
        if "heavy" in pocketcoffea_name or "hf" in pocketcoffea_name:
            if btype == "fullShape":
                return "CMS_btag_fullShape_hf"
            return f"CMS_btag_{btype}_bc_{variation}_{year}"
        elif "light" in pocketcoffea_name or "lf" in pocketcoffea_name:
            if btype == "fullShape":
                return "CMS_btag_fullShape_lf"
            return f"CMS_btag_{btype}_light_{variation}_{year}"
        elif "cferr" in pocketcoffea_name:
            return f"CMS_btag_{btype}_{variation}"
        else:
            logger.warn(f"Unclear flavour type for b-tag correction {pocketcoffea_name}. Should contain 'light' or 'heavy' in name. Using the name from pocketcoffea")
            return pocketcoffea_name
    else:
        logger.warn(f"Did not found name mapping for {pocketcoffea_name}. Using the name from pocketcoffea")

def add_variation_axis(histogram):
    """Return a histogram with an extra variation axis ('nominal'), preserving values+variances."""
    import hist

    # build new axes
    axes = list(histogram.axes)
    var_axis = hist.axis.StrCategory(["nominal"], name="variation", label="Variation")

    cat_idx = next((i for i, ax in enumerate(axes) if getattr(ax, "name", None) == "cat"), None)
    if cat_idx is None:
        new_axes = axes + [var_axis]
    else:
        new_axes = axes[:cat_idx+1] + [var_axis] + axes[cat_idx+1:]

    # create new histogram
    new_hist = hist.Hist(*new_axes, storage=hist.storage.Weight())

    # --- copy content ---
    for cat in histogram.axes[cat_idx]:
        idx_old = histogram.axes[cat_idx].index(cat)
        idx_new = new_hist.axes[cat_idx].index(cat)

        sl_old = [slice(None)] * histogram.ndim
        sl_new = [slice(None)] * new_hist.ndim
        sl_old[cat_idx] = idx_old
        sl_new[cat_idx] = idx_new
        sl_new[1] = 0  # idx 1="variations", 0="nominal"

        vals = histogram.view(flow=True)[tuple(sl_old)]
        new_hist.view(flow=True)[tuple(sl_new)] = vals

    return new_hist


def build_datacard(input_dir, output="./datacards"):
    if not os.path.exists(output):
        os.makedirs(output)

    # Define signal, background, and data datasets
    sig_bkg_dict = {
            "signal": {
                "TTTo2L2Nu": ["TTTo2L2Nu_2023_postBPix"],
                },
            "data": {
                # This needs to have this name, and only be one category
                "data_obs": ["DATA_EGamma_2023_EraD"]
                },
            "background": {
                "backgroundSample": ["background"]
                }
            }
    # Define the new region name
    region_name = "2btag"

    # I want to use the coffea file with all outputs. Therefore, it should be merged beforehand.
    coffea_list = [file for file in os.listdir(input_dir) if file.endswith(".coffea")]
    if "output_all.coffea" in coffea_list:
        coffea_file = os.path.join(input_dir, "output_all.coffea")
    else:
        raise NameError(f"No combined coffea file found in {coffea_list}")

    # -- Load Coffea file and config.json --
    coffea_file = load(coffea_file)

    # -- Histograms --
    histograms_dict = {}
    for key, sob_hist in coffea_file["variables"].items():
        if "MET" in key:
            histograms_dict[key] = sob_hist
            # "SoB": coffea_file["variables"]["sig_bkg_dnn_score"],
    # -- Create Processes
    meta_dict = coffea_file['datasets_metadata']['by_dataset']

    for dataset_list in sig_bkg_dict["signal"].values():
        for dataset in dataset_list:
            if dataset not in meta_dict.keys():
                raise Exception(f"Signal dataset {dataset} not found in file")
    for dataset in sig_bkg_dict["data"].values():
        for dataset in dataset_list:
            if dataset not in meta_dict.keys():
                raise Exception(f"Data dataset {dataset} not found in file")
    for dataset in sig_bkg_dict["background"].values():
        for dataset in dataset_list:
            if dataset not in meta_dict.keys():
                raise Exception(f"Background dataset {dataset} not found in file")

    logger.info(f"These are the MC samples: {sig_bkg_dict['signal']}")
    logger.info(f"These are the Data samples: {sig_bkg_dict['data']}")
    logger.info(f"These are the Data background samples: {sig_bkg_dict['background']}")

    # -- Filling metadata into the respective objects --
    mc_process = []
    for name, datasets in sig_bkg_dict["signal"].items():
        mc_process.append(MCProcess(
                name=name,
                # All these ugly catings are to get a list with unique values.
                samples=set([meta_dict[dataset]["sample"] for dataset in datasets]),
                years=set([meta_dict[dataset]["year"] for dataset in datasets]),
                is_signal=True,
                ))
    data_bg_process = []
    for name, datasets in sig_bkg_dict["background"].items():
        data_bg_process.append(MCProcess(
                name=name,
                samples=set([meta_dict[dataset]["sample"] for dataset in datasets]),
                years=set([meta_dict[dataset]["year"] for dataset in datasets]),
                is_signal=False,
                ))
    mc_processes = MCProcesses(mc_process + data_bg_process)

    if len(sig_bkg_dict["data"].keys()) > 1:
        raise Exception("Only one single data process is allowed with fixed name 'data_obs'")
    for name, datasets in sig_bkg_dict["data"].items():
        data_process = DataProcess(
                name=name,
                samples=set([meta_dict[dataset]["sample"] for dataset in datasets]),
                years=set([meta_dict[dataset]["year"] for dataset in datasets]),
                )
    data_processes = DataProcesses([data_process])

    # -- Systematics --
    # common_systematics = [
    #     "JES_Total_AK4PFPuppi", "JER_AK4PFPuppi"
    # ]

    # Trying to make this generic. Ideally, we want exactly one single set of variations at the moment because we are using MC only for signal. This has to be improved im some shape or form.
    # Essentially, right now, all MC sets belong to "GluGluHHto4b"
    for hist_cat, sob_hist in histograms_dict.items():
        # Get to the variation infos in the histograms for MC signal:
        systematics_list = []
        # Iterate through different signal types
        for sig_type, datasets in sig_bkg_dict["signal"].items():
            # Iterate through the datasets in a particular signal type (often a signle one)
            variations_updown = list(sob_hist[meta_dict[datasets[0]]["sample"]][datasets[0]].axes['variation'])
            for var in variations_updown:
                sliced = sob_hist[meta_dict[datasets[0]]["sample"]][datasets[0]][{"variation": var, "cat": region_name}]
                print(f"Variation: {var}")
                print(sliced.values())
            variations = set([re.sub(r'(Up|Down)$', '', var) for var in variations_updown])
            try:
                variations.remove("nominal")
            except:
                raise ValueError(f"Variations list {variations} does not contain 'nominal'.")
            logger.info(f"Found variations: {variations}")
            for syst in variations:
                systematics_list.append(SystematicUncertainty(name=syst, datacard_name=get_uncertainty_name(syst, meta_dict[datasets[0]]['year']), typ="shape", processes=list(sig_bkg_dict["signal"].keys()), years=[meta_dict[datasets[0]]["year"]], value=1.0))
            systematics = Systematics(systematics_list)
        for bkg_type, datasets in sig_bkg_dict["background"].items():
            # Iterate through the datasets in a particular background type (often a signle one)
            variations_updown = list(sob_hist[meta_dict[datasets[0]]["sample"]][datasets[0]].axes['variation'])
            for var in variations_updown:
                sliced = sob_hist[meta_dict[datasets[0]]["sample"]][datasets[0]][{"variation": var, "cat": region_name}]
                print(f"Variation: {var}")
                print(sliced.values())
            variations = set([re.sub(r'(Up|Down)$', '', var) for var in variations_updown])
            try:
                variations.remove("nominal")
            except:
                raise ValueError(f"Variations list {variations} does not contain 'nominal'.")
            logger.info(f"Found variations: {variations}")
            for syst in variations:
                systematics_list.append(SystematicUncertainty(name=syst, datacard_name=get_uncertainty_name(syst, meta_dict[datasets[0]]['year']), typ="shape", processes=list(sig_bkg_dict["background"].keys()), years=[meta_dict[datasets[0]]["year"]], value=1.0))

            # Adding some lnN systematics:
            lnN_systs = {"luminosity": 1.015, "alpha_s": 1.017}
            for syst, value in lnN_systs.items():
                dataset = next(iter(sig_bkg_dict["signal"].values()))[0]
                systematics_list.append(SystematicUncertainty(
                    name=syst,
                    datacard_name=get_uncertainty_name(syst, meta_dict[dataset]['year']),
                    typ="lnN",
                    processes=list(sig_bkg_dict["background"].keys()) + list(sig_bkg_dict["signal"].keys()),
                    years=[meta_dict[dataset]["year"]],
                    value=value)
                )

            # Make position of the systematics deterministic
            systematics_list = sorted(systematics_list, key=lambda syst: syst.datacard_name)
            # Removing some random systematics to test uncorrelated systs.
            systematics_list.pop(1)
            systematics_list.pop(-5)
            systematics = Systematics(systematics_list)

        _label = "run3"
        _datacard_name = f"datacard_combined_{_label}"
        _workspace_name = f"workspace_{_label}.root"

        auto_mc_stats = {
            "threshold": 10,
            "include_signal": 0,
            "hist_mode": 1,
        }

        datacard = Datacard(
                histograms=sob_hist,
                datasets_metadata=coffea_file["datasets_metadata"],
                cutflow=coffea_file["cutflow"],
                systematics=systematics,
                # This might have to change. Right now I am binding the year to the data year...
                years=set([meta_dict[dataset]["year"] for dataset in sig_bkg_dict["data"]["data_obs"]]),
                mc_processes=mc_processes,
                mcstat=auto_mc_stats,
                data_processes=data_processes,
                category=region_name,
                single_year=False,
                )
        datacard.dump(directory=f"{output}/{hist_cat}", card_name=f"{region_name}_{_label}.txt", shapes_name=f"shapes_{region_name}_{_label}.root")
