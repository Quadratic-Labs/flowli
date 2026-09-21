"""Sphinx configuration for the Flowlet documentation.

Build it with:

    sphinx-build -b html -W --keep-going docs docs/_build/html

The pages are Markdown, read by MyST. The API reference is generated from the
docstrings by autodoc, so the package must be importable: the path below adds
``src/`` for a checkout with no editable install.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

# region ----- Project -----

project = "Flowlet"
author = "Quadratic Labs"
copyright = f"{datetime.now(UTC):%Y}, Quadratic Labs"  # noqa: A001

try:
    from flowlet import __version__ as release
except ImportError:  # the package is not importable yet
    release = "0.1.0"
version = release

# endregion

# region ----- Extensions -----

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
    "sphinx_design",
    "sphinx_copybutton",
    "sphinxcontrib.mermaid",
]

exclude_patterns = ["_build", ".DS_Store", "Thumbs.db"]
root_doc = "index"
language = "en"

# endregion

# region ----- MyST -----

myst_enable_extensions = [
    "attrs_block",
    "attrs_inline",
    "colon_fence",
    "deflist",
    "fieldlist",
    "smartquotes",
    "substitution",
    "tasklist",
]
# A ```mermaid fence becomes the mermaid directive, so the source of a diagram
# stays a plain fenced block.
myst_fence_as_directive = ["mermaid"]
myst_heading_anchors = 3

# endregion

# region ----- autodoc -----

autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "show-inheritance": True,
    "member-order": "bysource",
}
autodoc_member_order = "bysource"
autodoc_preserve_defaults = True
autodoc_inherit_docstrings = False
autodoc_typehints = "signature"
autodoc_typehints_format = "short"
python_use_unqualified_type_names = True

napoleon_google_docstring = True
napoleon_numpy_docstring = False
napoleon_use_admonition_for_notes = True

# A Protocol has no docstring on each method by design, and a dataclass field
# inherits none either. Neither is a defect of the documentation. The
# specifications show pseudo-payloads with placeholders such as `{...}`, which
# no JSON or HTTP lexer accepts; Pygments falls back and says so.
suppress_warnings = ["misc.highlighting_failure"]

intersphinx_mapping = {"python": ("https://docs.python.org/3", None)}

# endregion

# region ----- HTML -----

html_theme = "furo"
html_title = "Flowlet"
html_static_path = ["_static"]
html_copy_source = False
html_show_sphinx = False
html_last_updated_fmt = "%Y-%m-%d"

_GITHUB = "https://github.com/Quadratic-Labs/flowlet"

html_theme_options = {
    "source_repository": f"{_GITHUB}/",
    "source_branch": "main",
    "source_directory": "docs/",
    "light_css_variables": {
        "color-brand-primary": "#00695c",
        "color-brand-content": "#00695c",
        "font-stack--monospace": "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace",
    },
    "dark_css_variables": {
        "color-brand-primary": "#4db6ac",
        "color-brand-content": "#4db6ac",
    },
    "footer_icons": [
        {
            "name": "GitHub",
            "url": _GITHUB,
            "class": "",
            "html": (
                '<svg stroke="currentColor" fill="currentColor" stroke-width="0" '
                'viewBox="0 0 16 16"><path fill-rule="evenodd" d="M8 0C3.58 0 0 3.58 0 '
                "8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 "
                "0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01"
                "1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95"
                "0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27"
                ".68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15"
                "0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38"
                'A8.013 8.013 0 0 0 16 8c0-4.42-3.58-8-8-8z"></path></svg>'
            ),
        },
    ],
}

# endregion
