__all__ = ['dedupe_by_key', 'partition', 'chunk_iterable']

import itertools


def dedupe_by_key(items, key):
    """Remove duplicates in an iterable of items based on key function.

    :type items: iterable
    :param items: Iterable of items.

    :type key: function
    :param key: A key function that takes an item as argument.
    """

    seen = set()
    for item in items:
        val = key(item)
        if val not in seen:
            yield item
            seen.add(val)


def partition(iterable, k):
    """
    Partition finite iterable into k almost-equal partitions
    Args:
        iterable: a finite iterable
        k: an integer that specifies the number of partitions
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
    :param size: Positive integer that specifies how many elements per chunk.
    """

    it = iter(iterable)
    chunk = tuple(itertools.islice(it, size))
    while chunk:
        yield chunk
        chunk = tuple(itertools.islice(it, size))

