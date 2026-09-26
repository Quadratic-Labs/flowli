"""The HTTP service. See specs/09-http-api.md.

Install with the `api` extra. The control plane and the catalog are here;
the worker plane (section 9) and evidence (section 10) are not built yet.
"""

from .app import create_app
from .auth import (
    CachingAuthenticator,
    OIDCAuthenticator,
    OIDCConfig,
    Principal,
    StaticAuthenticator,
)
from .catalog import Catalog
from .deps import ApiConfig

__all__ = [
    "ApiConfig",
    "CachingAuthenticator",
    "Catalog",
    "OIDCAuthenticator",
    "OIDCConfig",
    "Principal",
    "StaticAuthenticator",
    "create_app",
]
