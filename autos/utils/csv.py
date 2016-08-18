__all__ = ['iter_csv']


import csv
import collections


def itertuples(fp, **kwargs):
    """Iterate over CSV rows and yields namedtuple for each row.
    Fieldnames need to be valid Python identifier except for names starting with underscore.
    Read more: https://docs.python.org/3/library/collections.html#collections.namedtuple

    :type fp: file object
    :param fp: File object that points to a CSV file.

    :type kwargs: keyword arguments
    :param kwargs: extra arguments to be passed to csv.reader.

    :rtype: namedtuple
    :returns: Yields namedtuple for each row.
    """

    reader = csv.reader(fp, **kwargs)
    header = next(reader)
    Row = collections.namedtuple('Row', header)
    for row in reader:
        yield Row(*row)
