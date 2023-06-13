# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

from __future__ import annotations

# Warning: do not change the path here. To use autodoc, you need to install the
# package first.
import os
import shutil
import sys
from pathlib import Path


DIR = Path(__file__).parent.resolve()
BASEDIR = DIR.parent

sys.path.append(str(BASEDIR / "pocket_coffea"))


# -- Project information -----------------------------------------------------

project = "PocketCoffea"
copyright = "2022 "
author = "Davide Valsecchi, Matteo Marchegiani"


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.mathjax",
    "sphinx.ext.napoleon",
    "sphinx_copybutton",
]

myst_enable_extensions = [
    "amsmath",
    "attrs_inline",
    "colon_fence",
    "deflist",
    "dollarmath",
    "fieldlist",
    "html_admonition",
    "html_image",
#    "linkify",
    "replacements",
    "smartquotes",
    "strikethrough",
    "substitution",
    "tasklist",
]

# Add any paths that contain templates here, relative to this directory.
templates_path = []

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "**.ipynb_checkpoints", "Thumbs.db", ".DS_Store", ".env"]


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "sphinx_book_theme"

html_title = f"{project}"

html_baseurl = "https://PocketCoffea.readthedocs.io/en/latest/"

html_theme_options = {
    "home_page_in_toc": True,
    "repository_url": "https://github.com/PocketCoffea/PocketCoffea",
    "use_repository_button": True,
    "use_issues_button": True,
    "use_edit_page_button": True,
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path: list[str] = []


def prepare(app):
    outer = BASEDIR / ".github"
    inner = DIR
    contributing = "CONTRIBUTING.md"
    shutil.copy(outer / contributing, inner / "contributing.md")


def clean_up(app, exception):
    inner = DIR
    os.unlink(inner / "contributing.md")


def setup(app):

    # Copy the file in
    app.connect("builder-inited", prepare)

    # Clean up the generated file
    app.connect("build-finished", clean_up)
