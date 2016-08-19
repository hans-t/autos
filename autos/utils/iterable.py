__all__ = ['dedupe', 'partition', 'chunk_iterable']

import itertools


def dedupe(items, key=None):
    """Remove duplicates in an iterable of items based on key function.

    :type items: iterable
    :param items: Iterable of items.

    :type key: function
    :param key: A key function that takes an item as argument.

    :rtype: iterator
    :returns: Deduplicated, order-preserving iterable of items.
    """

    seen = set()
    for item in items:
        val = key(item) if key is not None else item
        if val not in seen:
            yield item
            seen.add(val)


def partition(iterable, k):
    """
    Partition finite iterable into k almost-equal partitions in round-robin manner.
    :type iterable: iterable
    :param iterable: A finite iterable.

    :type k: int
    :param k: Number of partitions.

    :rtype: list
    :returns: List of lists of partitioned iterable.
    """

    partitioned = []
    for idx, item in zip(itertools.cycle(range(k)), iterable):
        try:
            partitioned[idx].append(item)
        except IndexError:
            partitioned.append([item])
    return partitioned


def chunk_iterable(iterable, size):
    """Chunk iterable to approximately the same size.

    :type iterable: iterable
    :param iterable: Iterable to be chunked.

    :type size: int
    :param size: Number of elements per chunk.

    :rtype: iterator
    :returns: A list of chunks tuple.
    """

    it = iter(iterable)
    chunk = tuple(itertools.islice(it, size))
    while chunk:
        yield chunk
        chunk = tuple(itertools.islice(it, size))

