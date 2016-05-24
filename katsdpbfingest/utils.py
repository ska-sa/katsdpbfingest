"""Miscellaneous ingest utilities"""

import logging
import katsdptelstate


_logger = logging.getLogger(__name__)


def set_telstate_entry(telstate, name, value, prefix=None, attribute=True):
    if telstate is not None:
        if prefix is not None:
            name = '{0}_{1}'.format(prefix, name)
        try:
            telstate.add(name, value, immutable=attribute)
        except katsdptelstate.ImmutableKeyError:
            old = telstate.get(name)
            _logger.warning('Attribute %s could not be set to %s because it is already set to %s',
                            name, value, old)


__all__ = ['set_telstate_entry']
