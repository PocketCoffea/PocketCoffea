from collections.abc import Callable
from pathlib import Path

import pytest
from coffea.util import load
from omegaconf import OmegaConf
from pocket_coffea.parameters.defaults import get_default_parameters
from pocket_coffea.utils.plot_utils import PlotManager, Shape, Style


@pytest.fixture(scope="module")
def coffea_output():
    """Load pre-generated coffea output for plotting tests.

    Uses the output_run3.coffea file from test_shape_variations which contains
    histograms ready for plotting.
    """
    output_path = (
        Path(__file__).parent
        / "test_full_configs/test_shape_variations/comparison_arrays/output_run3.coffea"
    )

    return load(output_path)


@pytest.fixture(scope="module")
def default_plotting_parameters():
    """Provide default parameters for plotting tests."""
    return get_default_parameters()["plotting_style"]


@pytest.fixture(scope="function")
def plot_manager(tmp_path: Path, coffea_output: dict) -> Callable[..., PlotManager]:
    def _plot_manager(plotting_parameters: dict) -> PlotManager:
        assert coffea_output is not None
        assert "variables" in coffea_output

        variables = coffea_output["variables"]

        return PlotManager(
            variables=variables.keys(),
            hist_objs=variables,
            datasets_metadata=coffea_output["datasets_metadata"],
            plot_dir=tmp_path,
            style_cfg=plotting_parameters,
            workers=1,  # Use single worker for testing
            verbose=1,
        )

    return _plot_manager


def test_plot_manager(plot_manager, default_plotting_parameters, coffea_output: dict):
    """Test PlotManager initialization."""
    variables = coffea_output["variables"]
    # Test that the plot manager can generate a plot for one variable
    plot_mngr = plot_manager(default_plotting_parameters)
    for var_name in variables:
        assert any(shape.startswith(var_name) for shape in plot_mngr.shape_objects), (
            f"{var_name} not found in shape_objects of plot_manager"
        )


class TestHistogramPlotting:
    def test_plot_datamc_all(
        self,
        plot_manager: Callable[..., PlotManager],
        default_plotting_parameters: dict,
    ):
        """Test data/MC plotting for all shape objects."""
        plot_mngr = plot_manager(default_plotting_parameters)
        plot_mngr.plot_datamc_all(format="png")
        for shape_object in plot_mngr.shape_objects.values():
            for category in shape_object.categories:
                plot_path = (
                    plot_mngr.plot_dir
                    / category
                    / f"{shape_object.name}_{category}.png"
                )
                assert plot_path.exists(), f"Plot {plot_path} was not created"

    def test_plot_mc_only(
        self,
        plot_manager: Callable[..., PlotManager],
        default_plotting_parameters: dict,
    ):
        """Test MC-only plotting for all shape objects."""
        OmegaConf.update(
            default_plotting_parameters, "exclude_samples", ["DATA_SingleEle"]
        )
        plot_mngr = plot_manager(default_plotting_parameters)
        plot_mngr.plot_datamc_all(format="png")
        for shape_object in plot_mngr.shape_objects.values():
            for category in shape_object.categories:
                plot_path = (
                    plot_mngr.plot_dir
                    / category
                    / f"{shape_object.name}_{category}.png"
                )
                assert plot_path.exists(), f"Plot {plot_path} was not created"


# UserWarning is raised when the systematic shift is flat.
# we do not want to test for this here
@pytest.mark.filterwarnings("ignore:The ratio plot for:UserWarning:")
class TestSystematicsPlotting:
    def test_plot_systematic_shifts(
        self,
        plot_manager: Callable[..., PlotManager],
        default_plotting_parameters: dict,
    ):
        """Test systematic shift plotting for shape objects"""
        plot_mngr = plot_manager(default_plotting_parameters)
        # get one shape object to test systematic shift plotting
        shape: Shape = next(iter(plot_mngr.shape_objects.values()))
        category = shape.categories[0]  # Use the first category for testing
        # plot systematic shifts for this shape
        plot_mngr.plot_systematic_shifts(shape=shape, format="png")

        # check that plots were created for each systematic variation
        for variation in shape.syst_manager.systematics:
            plot_path = (
                plot_mngr.plot_dir
                / category
                / variation
                / f"{shape.name}_{category}_{variation}.png"
            )
            assert plot_path.exists(), f"Plot {plot_path} was not created"


class TestDeprecationWarnings:
    """Test that deprecation warnings are raised for deprecated plotting parameters."""

    def test_print_info_year_deprecation_warning(self, default_plotting_parameters):
        """Test that a DeprecationWarning is raised when print_info.year is enabled."""

        deprecation_message = (
            "The 'print_info.year' option is deprecated. "
            "Use 'cms_label.year: true' to display year info instead."
        )

        # Set print_info.year to True to trigger the deprecation warning
        OmegaConf.update(default_plotting_parameters, "print_info.year", True)
        with pytest.deprecated_call(match=deprecation_message):
            Style(
                style_cfg=default_plotting_parameters,
            )

        OmegaConf.update(default_plotting_parameters, "print_info.year", False)
        with pytest.deprecated_call(match=deprecation_message):
            Style(
                style_cfg=default_plotting_parameters,
            )

    def test_experiment_label_loc_deprecation_warning(
        self, default_plotting_parameters
    ):
        """Test that a DeprecationWarning is raised when experiment_label_loc is used."""

        deprecation_message = (
            "The 'experiment_label_loc' option is deprecated. "
            "Use 'cms_label.loc' instead."
        )

        # Set experiment_label_loc to trigger the deprecation warning
        OmegaConf.update(default_plotting_parameters, "experiment_label_loc", 2)
        with pytest.deprecated_call(match=deprecation_message):
            Style(
                style_cfg=default_plotting_parameters,
            )
