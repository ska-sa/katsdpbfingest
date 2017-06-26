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


__all__ = ['set_telstate_entry', 'Range']
