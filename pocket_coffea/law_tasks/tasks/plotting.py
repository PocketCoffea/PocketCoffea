import os

import law
import luigi
import luigi.util
from pocket_coffea.law_tasks.configuration.general import (
    plottingconfig,
    plottingsystematicsconfig,
)
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


@luigi.util.inherits(plottingconfig)
@luigi.util.inherits(Runner)
class PlotterBase(law.Task):
    """Base class for plotting tasks"""

    @property
    def full_output_dir(self): ...

    def requires(self):
        return Runner.req(self)

    def output(self):
        return law.LocalFileTarget(os.path.join(self.full_output_dir, ".plots_done"))

    def setup_plot_manager(self):
        inp = self.input()

        # load histograms
        output_coffea = inp["coffea"].load()

        # load plotting parameters
        plotting_parameters = load_plotting_style(
            inp["parameters"].abspath, self.plot_style
        )
        if self.blind:
            data_samples = load_sample_names(
                os.path.join(self.output_dir, "config.json"), prefix="DATA"
            )
            plotting_parameters = exclude_samples_from_plotting(
                plotting_parameters, data_samples
            )

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

        # plot histograms
        return PlotManager(
            variables=output_coffea["variables"].keys(),
            hist_objs=output_coffea["variables"],
            datasets_metadata=output_coffea["datasets_metadata"],
            plot_dir=self.output().abs_dirname,
            style_cfg=plotting_parameters,
            verbose=self.plot_verbose,
            workers=self.plot_workers,
            log=self.log_scale,
        )


class Plotter(PlotterBase):
    """Create plots from coffea output
    requires Runner task
    """

    @property
    def full_output_dir(self):
        blind_str = "blind" if self.blind else "data-mc"
        yscale_str = "log" if self.log_scale else "lin"
        return os.path.join(
            os.path.abspath(self.output_dir), self.plot_dir, blind_str, yscale_str
        )

    def run(self):
        plot_manager = self.setup_plot_manager()

        plot_manager.plot_datamc_all(syst=self.plot_syst, format=self.plot_format)

        # touch output
        self.output().touch()


@luigi.util.inherits(plottingsystematicsconfig)
class PlotSystematics(PlotterBase):
    """Plot the nominal, up and down shift of systematics"""

    @property
    def full_output_dir(self):
        syst_str = "systematics_ratio" if self.ratio else "systematics"
        yscale_str = "log" if self.log_scale else "lin"
        return os.path.join(
            os.path.abspath(self.output_dir), self.plot_dir, syst_str, yscale_str
        )

    def run(self):
        plot_manager = self.setup_plot_manager()

        plot_manager.plot_systematic_shifts_all(
            format=self.plot_format, ratio=self.ratio
        )

        # mark task as done
        self.output().touch()
