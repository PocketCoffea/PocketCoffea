from pathlib import Path

import pytest
from coffea.util import load
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
def plotting_parameters():
    """Provide default parameters for plotting tests."""
    return get_default_parameters()["plotting_style"]


@pytest.fixture(scope="function")
def plot_manager(coffea_output, plotting_parameters, tmp_path):
    """Fixture to create a PlotManager instance for testing."""
    assert coffea_output is not None
    assert "variables" in coffea_output

    variables = coffea_output["variables"]

    return PlotManager(
        variables=variables.keys(),
        hist_objs=variables,
        datasets_metadata=coffea_output["datasets_metadata"],
        plot_dir=tmp_path / "plots",
        style_cfg=plotting_parameters,
        workers=1,  # Use single worker for testing
        verbose=1,
    )


def test_plot_manager(plot_manager: PlotManager, coffea_output: dict):
    """Test PlotManager initialization."""
    variables = coffea_output["variables"]
    # Test that the plot manager can generate a plot for one variable
    for var_name in variables:
        assert any(
            shape.startswith(var_name) for shape in plot_manager.shape_objects
        ), f"{var_name} not found in shape_objects of plot_manager"


class TestHistogramPlotting:
    def test_plot_datamc_all(self, plot_manager: PlotManager):
        """Test data/MC plotting for all shape objects."""
        plot_manager.plot_datamc_all(format="png")
        for shape_object in plot_manager.shape_objects:
            for category in shape_object.categories:
                plot_path = (
                    plot_manager.plot_dir / category / f"{shape_object.name}.png"
                )
                assert plot_path.exists(), f"Plot {plot_path} was not created"


class TestSystematicsPlotting:
    def test_plot_systematic_shifts(self, plot_manager: PlotManager):
        """Test systematic shift plotting for shape objects"""
        # get one shape object to test systematic shift plotting
        shape: Shape = next(iter(plot_manager.shape_objects.values()))
        category = shape.categories[0]  # Use the first category for testing
        # plot systematic shifts for this shape
        plot_manager.plot_systematic_shifts(shape=shape, format="png")

        # check that plots were created for each systematic variation
        for variation in shape.syst_manager.systematics:
            plot_path = (
                plot_manager.plot_dir
                / category
                / variation
                / f"{shape.name}_{category}_{variation}.png"
            )
            assert plot_path.exists(), f"Plot {plot_path} was not created"
