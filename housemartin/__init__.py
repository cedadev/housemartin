# -*- coding: utf-8 -*-

"""Top-level package for housemartin."""

from .__version__ import __author__, __email__, __version__  # noqa: F401

from roocs_utils.config import get_config
import housemartin

CONFIG = get_config(housemartin)

from .wsgi import application  # noqa: F401
