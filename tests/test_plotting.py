from collections.abc import Callable
from pathlib import Path

import pytest
from coffea.util import load
from omegaconf import OmegaConf
from pocket_coffea.parameters.defaults import get_default_parameters
from pocket_coffea.utils.plot_utils import PlotManager, Shape


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
    # TODO: test with MC only, i.e. filter DATA samples
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

    def test_plot_with_custom_cms_label(
        self,
        plot_manager: Callable[..., PlotManager],
        default_plotting_parameters: dict,
    ):
        """Test plotting with a custom title."""
        OmegaConf.update(default_plotting_parameters, "cms_label", "Custom Plot Title")
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
