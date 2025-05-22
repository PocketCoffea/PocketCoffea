import law
import luigi


class plottingconfig(luigi.Config):
    plot_dir = luigi.Parameter(
        default=law.NO_STR, description="Output folder for plots"
    )
    plot_verbose = luigi.IntParameter(
        default=0, description="verbosity level for PlotManager (default: 0)"
    )
    plot_style = luigi.Parameter(
        default=law.NO_STR, description="yaml file with plotting style"
    )
    blind = luigi.BoolParameter(
        default=True, description="If True, only MC is plotted. default=True"
    )
    plot_workers = luigi.IntParameter(
        default=4, description="number of workers to plot in parallel"
    )
    plot_syst = luigi.BoolParameter(
        default=False, description="Wether to plot systematic uncertainty bands"
    )
    log_scale_x = luigi.BoolParameter(
        default=False, description="Set x-axis to log scale"
    )
    log_scale_y = luigi.BoolParameter(
        default=False, description="Set y-axis to log scale"
    )
    plot_format = luigi.Parameter(
        description="Output format of the plots", default="pdf"
    )
    variables = law.CSVParameter(description="List of variables to plot", default=())


class plottingsystematicsconfig(luigi.Config):
    ratio = luigi.BoolParameter(
        default=True, description="Plot the ratio of the systematic shifts"
    )
