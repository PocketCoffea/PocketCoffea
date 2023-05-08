hatch_density = 4

style_cfg = {
    "fontsize": 22,
    "fontsize_legend_ratio": 12,
    "opts_figure": {
        "datamc": {
            'figsize': (12, 9),
        },
        "datamc_ratio": {
            'figsize': (12, 12),
            'gridspec_kw': {"height_ratios": (3, 1)},
            'sharex': True,
        },
        "partial": {
            'figsize': (12, 15),
            'gridspec_kw': {"height_ratios": (3, 1)},
            'sharex': True,
        },
    },
    "opts_mc": {'histtype': 'fill', 'stack': True},
    "opts_data": {
        'linestyle': 'solid',
        'linewidth': 0,
        'marker': '.',
        'markersize': 5.0,
        'color': 'black',
        'elinewidth': 1,
        'label': 'Data',
    },
    "opts_unc": {
        "total": {
            "step": "post",
            "color": (0, 0, 0, 0.4),
            "facecolor": (0, 0, 0, 0.0),
            "linewidth": 0,
            "hatch": '/' * hatch_density,
            "zorder": 2,
        },
        'Up': {
            'linestyle': 'dashed',
            'linewidth': 1,
            'marker': '.',
            'markersize': 1.0,
            #'color': 'red',
            'elinewidth': 1,
        },
        'Down': {
            'linestyle': 'dotted',
            'linewidth': 1,
            'marker': '.',
            'markersize': 1.0,
            #'color': 'red',
            'elinewidth': 1,
        },
    },
    "opts_syst": {
        'nominal': {
            'linestyle': 'solid',
            'linewidth': 1,
            'color': 'black',
        },
        'up': {
            'linestyle': 'dashed',
            'linewidth': 1,
            'color': 'red',
        },
        'down': {
            'linestyle': 'dotted',
            'linewidth': 1,
            'color': 'blue',
        },
    },
    "collapse_datasets": True,
    "samples_map": {},
    "labels": {
        "ttHTobb": "$t\\bar{t}H\\rightarrow b\\bar{b}$",
        "TTToSemiLeptonic": "$t\\bar{t}$ semilep.",
        "TTTo2L2Nu": "$t\\bar{t}$ dilepton",
        "SingleTop": "Single t",
        "WJetsToLNu_HT": "W+jets",
    },
    "colors": {
        'ttHTobb': 'pink',
        'TTTo2L2Nu': (0.51, 0.79, 1.0),  # blue
        'TTToSemiLeptonic': (1.0, 0.71, 0.24),  # orange
        'SingleTop': (1.0, 0.4, 0.4),  # red
        'ST': (1.0, 0.4, 0.4),  # red
        'WJetsToLNu_HT': '#cc99ff',  # violet
    },
    "plot_upper_label":{
        "by_year": {
            "2018": "XX /fb",
        }
    }
}
