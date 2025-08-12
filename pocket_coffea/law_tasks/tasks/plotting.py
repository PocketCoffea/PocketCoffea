import law
import luigi
import luigi.util

from pocket_coffea.law_tasks.configuration.plotting import (
    plottingconfig,
    plottingsystematicsconfig,
)
from pocket_coffea.law_tasks.tasks.base import BaseTask
from pocket_coffea.law_tasks.tasks.runner import Runner
from pocket_coffea.law_tasks.utils import (
    exclude_samples_from_plotting,
    filter_items_by_regex,
    load_plotting_style,
    load_sample_names,
)
from pocket_coffea.utils.plot_utils import PlotManager

# load contrib packages for law
law.contrib.load("coffea")


def get_log_scale_str(log_scale_x: bool = False, log_scale_y: bool = False) -> str:
    """Get the appropriate log scale string for file targets.

    Parameters
    ----------
    log_scale_x : bool, optional
        if x axis is log scale, by default False
    log_scale_y : bool, optional
        if y axis is log scale, by default False

    Returns
    -------
    str
        depending on the log scale, the string will be:
        "log-log", "log-x", "log-y" or "lin"
    """
    if log_scale_x and log_scale_y:
        return "log-log"
    elif log_scale_x:
        return "log-x"
    elif log_scale_y:
        return "log-y"
    else:
        return "lin"


class NoMatchingVariableError(Exception):
    """Exception raised when no matching variable is found in coffea output"""


@luigi.util.inherits(plottingconfig)
@luigi.util.inherits(Runner)
class PlotterBase(BaseTask):
    """Base class for plotting tasks"""

    def requires(self):
        return Runner.req(self)

    def store_parts(self) -> tuple[str]:
        if self.test:
            return super().store_parts() + ("test",)
        return super().store_parts()

    def setup_plot_manager(self):
        inp = self.input()

        # load histograms
        output_coffea = inp["coffea"].load()

        # load plotting parameters
        plotting_parameters = load_plotting_style(
            inp["parameters"].abspath, self.plot_style
        )
        if self.blind:
            data_samples = load_sample_names(inp["config"].path, prefix="DATA")
            plotting_parameters = exclude_samples_from_plotting(
                plotting_parameters, data_samples
            )

        available_variables = list(output_coffea["variables"].keys())
        if self.variables:
            # get variables that should not be plotted
            vars_to_pop = []
            for variable in self.variables:
                vars_to_pop.extend(
                    filter_items_by_regex(
                        variable, output_coffea["variables"], match=False
                    )
                )

            for key in vars_to_pop:
                output_coffea["variables"].pop(key, None)

        if not output_coffea["variables"]:
            raise NoMatchingVariableError(
                f"Could not find any matching variable of {self.variables} in the coffea output.\n"
                f"Available variables are: {available_variables}"
            )

        # plot histograms
        return PlotManager(
            variables=output_coffea["variables"].keys(),
            hist_objs=output_coffea["variables"],
            datasets_metadata=output_coffea["datasets_metadata"],
            plot_dir=self.output().absdirname,
            style_cfg=plotting_parameters,
            verbose=self.plot_verbose,
            workers=self.plot_workers,
            log_x=self.log_scale_x,
            log_y=self.log_scale_y,
        )


class Plotter(PlotterBase):
    """Create plots from coffea output
    requires Runner task
    """

    def output(self):
        blind_str = "blind" if self.blind else "data-mc"
        logscale_str = get_log_scale_str(self.log_scale_x, self.log_scale_y)
        if self.plot_dir != law.NO_STR:
            return self.local_file_target(
                self.plot_dir, blind_str, logscale_str, ".plots_done"
            )
        return self.local_file_target(blind_str, logscale_str, ".plots_done")

    def run(self):
        plot_manager = self.setup_plot_manager()

        plot_manager.plot_datamc_all(syst=self.plot_syst, format=self.plot_format)

        # touch output
        self.output().touch()


@luigi.util.inherits(plottingsystematicsconfig)
class PlotSystematics(PlotterBase):
    """Plot the nominal, up and down shift of systematics"""

    def output(self):
        syst_str = "systematics_ratio" if self.ratio else "systematics"
        logscale_str = get_log_scale_str(self.log_scale_x, self.log_scale_y)
        if self.plot_dir != law.NO_STR:
            return self.local_file_target(
                self.plot_dir, syst_str, logscale_str, ".plots_done"
            )
        return self.local_file_target(syst_str, logscale_str, ".plots_done")

    def run(self):
        plot_manager = self.setup_plot_manager()

        plot_manager.plot_systematic_shifts_all(
            format=self.plot_format, ratio=self.ratio
        )

        # mark task as done
        self.output().touch()
