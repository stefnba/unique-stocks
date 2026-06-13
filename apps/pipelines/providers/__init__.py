"""Concrete provider implementation exports.

Provider identity keys live in ``config.providers`` so implementation packages
do not own app composition. This package exports provider client classes only;
callers that need provider identities should import ``config.providers``.
"""

from providers.eodhd import EODHDClient
from providers.iso10383 import ISO10383Client

__all__ = ["EODHDClient", "ISO10383Client"]
