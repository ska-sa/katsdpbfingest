"""Miscellaneous ingest utilities"""

import logging
import katsdptelstate


_logger = logging.getLogger(__name__)


def cbf_telstate_prefixes(telstate, stream_name):
    """Find all prefixes that should be consulted in telstate to learn
    information about a stream. It supports only baseline-correlation-products
    and tied-array-channelised-voltage streams.

    It supports older versions of cam2telstate that strips the stream
    names out of most sensors as a fallback.

    Returns
    -------
    prefixes : list of str
        Prefixes to prepend to telstate lookups (without a trailing
        underscore). They should be consulted in order, using the first one
        that matches.
    """
    prefixes = []
    if stream_name is not None:
        stream_name = stream_name.replace('.', '_').replace('-', '_')
        prefixes.append('cbf_' + stream_name)
        # Generate a list of places to look for attributes:
        # - the stream itself
        # - if known, the upstream antenna-channelised-voltage stream, and its instrument
        # - if all else fails, the base cbf_ prefix (for backwards compatibility)
        try:
            # antenna-channelised-voltage stream
            src = telstate['cbf_' + stream_name + '_src_streams'][0]
            prefixes.append('cbf_' + src)
            instrument = telstate['cbf_' + src + '_instrument_dev_name']
            prefixes.append('cbf_' + instrument)
        except KeyError, IndexError:
            _logger.warning('Could not find upstream sources of %s', stream_name)
            pass
    prefixes.append('cbf')
    return prefixes


def get_telstate_entry(telstate, prefixes, key):
    """Fetch an attribute from telstate.

    Multiple version of the name are consulted, and the first one found wins.

    Raises
    ------
    KeyError
        if the key is not found with any of the prefixes
    """
    for prefix in prefixes:
        try:
            telstate_name = '{}_{}'.format(prefix, key)
            return telstate[telstate_name]
        except KeyError:
            pass
    # We didn't find a match on any prefix
    raise KeyError('attribute {} not found in telstate with any prefix'.format(key))


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


class Range(object):
    """Representation of a range of values, as specified by a first and a
    past-the-end value. This can be seen as an extended form of `xrange` or
    `slice` (although without support for a non-unit step), where it is easy to
    query the start and stop values, along with other convenience methods.

    Ranges can be empty, in which case they still have a `start` and `stop`
    value that are equal, but the value itself is irrelevant.
    """
    def __init__(self, start, stop):
        if start > stop:
            raise ValueError('start must be <= stop')
        self.start = start
        self.stop = stop

    @classmethod
    def parse(cls, value):
        """Convert a string of the form 'A:B' to a :class:`~katsdpingest.utils.Range`,
        where A and B are integers.

        This is suitable as an argparse type converter.
        """
        fields = value.split(':', 1)
        if len(fields) != 2:
            raise ValueError('Invalid range format {}'.format(value))
        else:
            return Range(int(fields[0]), int(fields[1]))

    def __str__(self):
        return '{}:{}'.format(self.start, self.stop)

    def __repr__(self):
        return 'Range({}, {})'.format(self.start, self.stop)

    def __len__(self):
        return self.stop - self.start

    def __contains__(self, value):
        return self.start <= value < self.stop

    def __eq__(self, other):
        if not isinstance(other, Range):
            return False
        if not self:
            return not other
        else:
            return self.start == other.start and self.stop == other.stop

    def __ne__(self, other):
        return not (self == other)

    # Can't prevent object from being mutated, but __eq__ is defined, so not
    # suitable for hashing.
    __hash__ = None

    def issubset(self, other):
        return self.start == self.stop or (other.start <= self.start and self.stop <= other.stop)

    def issuperset(self, other):
        return other.issubset(self)

    def isaligned(self, alignment):
        """Whether the start and end of this interval are aligned to multiples
        of `alignment`.
        """
        return not self or (self.start % alignment == 0 and self.stop % alignment == 0)

    def alignto(self, alignment):
        """Return the smallest range containing self for which
        ``r.isaligned()`` is true.
        """
        if not self:
            return self
        else:
            return Range(self.start // alignment * alignment,
                         (self.stop + alignment - 1) // alignment * alignment)

    def intersection(self, other):
        start = max(self.start, other.start)
        stop = min(self.stop, other.stop)
        if start > stop:
            return Range(0, 0)
        else:
            return Range(start, stop)

    def union(self, other):
        """Return the smallest range containing both ranges."""
        if not self:
            return other
        if not other:
            return self
        return Range(min(self.start, other.start), max(self.stop, other.stop))

    def __iter__(self):
        return iter(xrange(self.start, self.stop))

    def relative_to(self, other):
        """Return a new range that represents `self` as a range relative to
        `other` (i.e. where the start element of `other` is numbered 0). If
        `self` is an empty range, an undefined empty range is returned.

        Raises
        ------
        ValueError
            if `self` is not a subset of `other`
        """
        if not self.issubset(other):
            raise ValueError('self is not a subset of other')
        return Range(self.start - other.start, self.stop - other.start)

    def asslice(self):
        """Return a slice object representing the same range"""
        return slice(self.start, self.stop)

    def astuple(self):
        """Return a tuple containing the start and end values"""
        return (self.start, self.stop)

    def split(self, chunks, chunk_id):
        """Return the `chunk_id`-th of `chunks` equally-sized pieces.

        Raises
        ------
        ValueError
            if chunk_id is not in the range [0, chunks) or the range does not
            divide evenly.
        """
        if not 0 <= chunk_id < chunks:
            raise ValueError('chunk_id is out of range')
        if len(self) % chunks != 0:
            raise ValueError('range {} does not divide into {} chunks'.format(self, chunks))
        chunk_size = len(self) // chunks
        return Range(self.start + chunk_id * chunk_size,
                     self.start + (chunk_id + 1) * chunk_size)


__all__ = ['set_telstate_entry', 'Range']
