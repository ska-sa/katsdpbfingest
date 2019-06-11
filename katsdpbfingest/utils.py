"""Miscellaneous ingest utilities"""

import logging
from typing import Iterator, Tuple

import katsdptelstate


_logger = logging.getLogger(__name__)


def cbf_telstate_view(telstate: katsdptelstate.TelescopeState,
                      stream_name: str) -> katsdptelstate.TelescopeState:
    """Create a telstate view that allows querying properties from a stream.
    It supports only baseline-correlation-products and
    tied-array-channelised-voltage streams. Properties that don't exist on the
    stream are searched on the upstream antenna-channelised-voltage stream, and
    then the instrument of that stream.

    Returns
    -------
    view
        Telstate view that allows stream properties to be searched
    """
    prefixes = []
    stream_name = stream_name.replace('.', '_').replace('-', '_')
    prefixes.append(stream_name)
    # Generate a list of places to look for attributes:
    # - the stream itself
    # - the upstream antenna-channelised-voltage stream, and its instrument
    src = telstate.view(stream_name, exclusive=True)['src_streams'][0]
    prefixes.append(src)
    instrument = telstate.view(src, exclusive=True)['instrument_dev_name']
    prefixes.append(instrument)
    # Create a telstate view that has exactly the given prefixes (and no root prefix).
    for i, prefix in enumerate(reversed(prefixes)):
        telstate = telstate.view(prefix, exclusive=(i == 0))
    return telstate


class Range:
    """Representation of a range of values, as specified by a first and a
    past-the-end value. This can be seen as an extended form of `xrange` or
    `slice` (although without support for a non-unit step), where it is easy to
    query the start and stop values, along with other convenience methods.

    Ranges can be empty, in which case they still have a `start` and `stop`
    value that are equal, but the value itself is irrelevant.
    """
    def __init__(self, start: int, stop: int) -> None:
        if start > stop:
            raise ValueError('start must be <= stop')
        self.start = start
        self.stop = stop

    @classmethod
    def parse(cls, value: str) -> 'Range':
        """Convert a string of the form 'A:B' to a :class:`~katsdpingest.utils.Range`,
        where A and B are integers.

        This is suitable as an argparse type converter.
        """
        fields = value.split(':', 1)
        if len(fields) != 2:
            raise ValueError('Invalid range format {}'.format(value))
        else:
            return Range(int(fields[0]), int(fields[1]))

    def __str__(self) -> str:
        return '{}:{}'.format(self.start, self.stop)

    def __repr__(self) -> str:
        return 'Range({}, {})'.format(self.start, self.stop)

    def __len__(self) -> int:
        return self.stop - self.start

    def __contains__(self, value: int) -> bool:
        return self.start <= value < self.stop

    def __eq__(self, other):
        if not isinstance(other, Range):
            return NotImplemented
        if not self:
            return not other
        else:
            return self.start == other.start and self.stop == other.stop

    def __ne__(self, other):
        if not isinstance(other, Range):
            return NotImplemented
        else:
            return not (self == other)

    # Can't prevent object from being mutated, but __eq__ is defined, so not
    # suitable for hashing.
    __hash__ = None        # type: ignore  # workaround for python/mypy#4266

    def issubset(self, other: 'Range') -> bool:
        return self.start == self.stop or (other.start <= self.start and self.stop <= other.stop)

    def issuperset(self, other: 'Range') -> bool:
        return other.issubset(self)

    def isaligned(self, alignment: int) -> bool:
        """Whether the start and end of this interval are aligned to multiples
        of `alignment`.
        """
        return not self or (self.start % alignment == 0 and self.stop % alignment == 0)

    def alignto(self, alignment: int) -> 'Range':
        """Return the smallest range containing self for which
        ``r.isaligned()`` is true.
        """
        if not self:
            return self
        else:
            return Range(self.start // alignment * alignment,
                         (self.stop + alignment - 1) // alignment * alignment)

    def __floordiv__(self, alignment: int) -> 'Range':
        """Divide start and end by `alignment`.

        If they are not multiples, it is as if :meth:`alignto` was used first.
        """
        if not self:
            return self
        else:
            return Range(self.start // alignment,
                         (self.stop + alignment - 1) // alignment)

    def intersection(self, other: 'Range') -> 'Range':
        start = max(self.start, other.start)
        stop = min(self.stop, other.stop)
        if start > stop:
            return Range(0, 0)
        else:
            return Range(start, stop)

    def union(self, other: 'Range') -> 'Range':
        """Return the smallest range containing both ranges."""
        if not self:
            return other
        if not other:
            return self
        return Range(min(self.start, other.start), max(self.stop, other.stop))

    def __iter__(self) -> Iterator[int]:
        return iter(range(self.start, self.stop))

    def relative_to(self, other: 'Range') -> 'Range':
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

    def asslice(self) -> slice:
        """Return a slice object representing the same range"""
        return slice(self.start, self.stop)

    def astuple(self) -> Tuple[int, int]:
        """Return a tuple containing the start and end values"""
        return (self.start, self.stop)

    def split(self, chunks: int, chunk_id: int) -> 'Range':
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


__all__ = ['cbf_telstate_view', 'Range']
