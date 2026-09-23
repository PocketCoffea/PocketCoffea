from collections.abc import Callable
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import mplhep as hep
import pytest
from coffea.util import load
from omegaconf import OmegaConf
from pocket_coffea.parameters.defaults import get_default_parameters
from pocket_coffea.utils.cutflow_utils import (
    plot_cutflow_from_output,
    plot_sample_cutflow,
)
from pocket_coffea.utils.plot_utils import (
    PlotManager,
    Shape,
    Style,
    build_cms_label_kwargs,
)

# use non-interactive backend for tests
matplotlib.use("agg")


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


@pytest.fixture(scope="function")
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


# from pocket_coffea.utils.plot_functions import plot_shapes_comparison

# def test_plot_shapes_comparison(self, coffea_output: dict, tmp_path: Path):
#     """Test plot_shapes_comparison function."""
#     df = coffea_output["variables"]
#     var = "ElectronGood_eta"
#     shapes = [
#         ("TTTo2L2Nu", "baseline", "2023_postBPix", "nominal", "TT nominal"),
#         (
#             "TTTo2L2Nu",
#             "baseline",
#             "2023_postBPix",
#             "AK8PFPuppi_JES_TotalUp",
#             "TT JES up",
#         ),
#         (
#             "TTTo2L2Nu",
#             "baseline",
#             "2023_postBPix",
#             "AK8PFPuppi_JES_TotalDown",
#             "TT JES down",
#         ),
#     ]
#     outputfile = str(tmp_path / "shapes_comparison")
#     fig = plot_shapes_comparison(
#         df, var, shapes, title="Comparison", outputfile=outputfile
#     )


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


class TestBuildCmsLabelKwargs:
    """Test build_cms_label_kwargs function."""

    def _create_label(self, kwargs: dict):
        fig, ax = plt.subplots()
        try:
            hep.cms.label(ax=ax, **kwargs)
            assert len(ax.texts) >= 1
        finally:
            plt.close(fig)

    def test_default_cms_label_kwargs(self, default_plotting_parameters: dict):
        """Default parameters in PocketCoffea should be valid for mplhep.cms.label."""
        style = Style(default_plotting_parameters)
        # we need a year that exists in the plotting config
        year = next(iter(style.cms_label.com.keys()))
        # test is_mc_only=True
        kwargs = build_cms_label_kwargs(
            cfg=style.cms_label, is_mc_only=True, year=year, fontsize=style.fontsize
        )
        # pass to mplhep.cms.label
        self._create_label(kwargs)

        # test is_mc_only=False
        kwargs = build_cms_label_kwargs(
            cfg=style.cms_label, is_mc_only=False, year=year, fontsize=style.fontsize
        )
        # pass to mplhep.cms.label
        self._create_label(kwargs)

    def test_year_missing_in_lumi_and_com_warning(
        self, default_plotting_parameters: dict
    ):
        style = Style(default_plotting_parameters)
        style.cms_label.lumi.show = True
        year = "1642"
        # this year does not exist in the lumi dict
        # so a warning should be printed
        with pytest.warns() as record:
            kwargs = build_cms_label_kwargs(
                style.cms_label, is_mc_only=True, year=year, fontsize=style.fontsize
            )

        assert len(record) == 2
        assert "not in the 'value' dict of cms_label.lumi" in str(record[0].message)
        assert "not in 'com' dict of cms_label" in str(record[1].message)

        # check that plotting succeeds
        self._create_label(kwargs)

    def test_llabel_rlabel(self, default_plotting_parameters: dict):
        style = Style(default_plotting_parameters)
        style.cms_label.llabel = "Left Label"
        style.cms_label.rlabel = "Right Label"
        year = next(iter(style.cms_label.com.keys()))
        kwargs = build_cms_label_kwargs(
            style.cms_label,
            is_mc_only=True,
            year=year,
            fontsize=style.fontsize,
        )
        self._create_label(kwargs)

    def test_custom_cms_text(self, default_plotting_parameters: dict):
        style = Style(default_plotting_parameters)
        style.cms_label.text = "Custom CMS Text"
        year = next(iter(style.cms_label.com.keys()))
        kwargs = build_cms_label_kwargs(
            style.cms_label, is_mc_only=True, year=year, fontsize=style.fontsize
        )
        self._create_label(kwargs)

    def test_supp_kwarg(self, default_plotting_parameters: dict):
        style = Style(default_plotting_parameters)
        style.cms_label.text = "Custom CMS Text"
        year = next(iter(style.cms_label.com.keys()))
        kwargs = build_cms_label_kwargs(
            style.cms_label, is_mc_only=True, year=year, fontsize=style.fontsize
        )
        self._create_label(kwargs)


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


class TestCutflowPlotting:
    """Test cutflow plotting utilities in cutflow_utils."""

    @pytest.mark.parametrize(
        "log_y,with_ratio", [(False, False), (False, True), (True, False), (True, True)]
    )
    def test_plot_sample_cutflow(
        self,
        tmp_path: Path,
        log_y: bool,
        with_ratio: bool,
    ):
        """Test plot_sample_cutflow for all log_y/with_ratio combinations."""
        sample = "TTTo2L2Nu"
        sample_data = {"initial": 300, "skim": 200, "presel": 190, "baseline": 180}
        categories = list(sample_data.keys())
        year = "2023_postBPix"
        datasets_metadata = {
            f"{sample}_{year}": {
                "sample": sample,
                "isMC": True,
            }
        }

        output_dir = tmp_path / "sample_cutflow"
        output_dir.mkdir()

        saved_files = plot_sample_cutflow(
            sample=sample,
            sample_data=sample_data,
            year=year,
            categories=categories,
            plot_type="Cutflow",
            ylabel="Number of Events",
            log_y=log_y,
            with_ratio=with_ratio,
            output_dir=str(output_dir),
            output_format="png",
            datasets_metadata=datasets_metadata,
        )

        if with_ratio:
            assert len(saved_files) == 2
        else:
            assert len(saved_files) == 1

        for name in saved_files:
            path = output_dir / name
            assert path.exists(), f"expected plot file {path} was not created"

    # def test_plot_cutflow_from_output(self, tmp_path: Path, coffea_output: dict):
    #     """Test plot_cutflow_from_output on the full coffea output."""
    #     output_dir = tmp_path / "cutflow"
    #     output_dir.mkdir()

    #     saved_files = plot_cutflow_from_output(
    #         output=coffea_output,
    #         output_dir=str(output_dir),
    #     )

    #     for plot_type, filepaths in saved_files.items():
    #         assert filepaths, f"no {plot_type} plots were created"
    #         for path in filepaths:
    #             assert Path(path).exists(), f"plot file {path} was not created"

    def test_plot_sample_cutflow_raises_on_missing_categories(self, tmp_path: Path):
        """Test that a ValueError is raised when sample_data shares no category."""
        output_dir = tmp_path / "sample_cutflow"
        output_dir.mkdir()

        with pytest.raises(ValueError, match="No data found for sample"):
            plot_sample_cutflow(
                sample="TTTo2L2Nu",
                sample_data={"some_other_category": 10},
                year="2023_postBPix",
                categories=["initial", "skim", "presel"],
                plot_type="Cutflow",
                ylabel="Number of Events",
                output_dir=str(output_dir),
                output_format="png",
            )
