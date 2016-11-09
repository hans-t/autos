__all__ = ['fread_csv', 'read_csv', 'fwrite_csv', 'write_csv']


import csv
import collections

import autos.constants as constants


def iterlists(fp, **kwargs):
    yield from csv.reader(fp, **kwargs)


def itertuples(fp, **kwargs):
    '''Iterate over CSV rows and yields namedtuple for each row.
    Fieldnames need to be valid Python identifier except for names starting with underscore.
    Read more: https://docs.python.org/3/library/collections.html#collections.namedtuple

    :type fp: file object
    :param fp: File object that points to a CSV file.

    :type kwargs: keyword arguments
    :param kwargs: extra arguments to be passed to csv.reader.

    :rtype: iterator
    :returns: Namedtuple rows.
    '''

    reader = iterlists(fp, **kwargs)
    header = next(reader)
    Row = collections.namedtuple('Row', header)
    for row in reader:
        yield Row(*row)


def iterdicts(fp, **kwargs):
    yield from csv.DictReader(fp, **kwargs)


def fread_csv(fp, as_namedtuple=False, as_dict=False, **kwargs):
    delimiter = kwargs.pop('delimiter', constants.DEFAULT_DELIMITER)
    if as_namedtuple:
        rows = itertuples(fp, delimiter=delimiter, **kwargs)
    elif as_dict:
        rows = iterdicts(fp, delimiter=delimiter, **kwargs)
    else:
        rows = iterlists(fp, delimiter=delimiter, **kwargs)
    yield from rows


def read_csv(path, as_namedtuple=False, as_dict=False, **kwargs):
    encoding = kwargs.pop('encoding', constants.DEFAULT_ENCODING)
    newline = kwargs.pop('newline', constants.DEFAULT_NEWLINE)
    fp = open(path, encoding=encoding, newline=newline)
    yield from fread_csv(
        fp,
        as_namedtuple=as_namedtuple,
        as_dict=as_dict,
        **kwargs
    )


def fwrite_csv(fp, rows, from_dict=False, header=True, **kwargs):
    '''Write rows to file-object.

    :type fp: file object
    :param fp: File object that points to a CSV file.

    :type rows: iterable
    :param rows: Iterable of iterables or dicts.

    :type from_dict: bool
    :param from_dict: If from_dict is True, use csv.DictWriter.

    :type header: bool
    :param header: If header is true, write header. Only applicable if from_dict is true.

    :type kwargs: keyword arguments
    :param kwargs: extra arguments to be passed to csv.writer() or csv.DictWriter().
    '''

    delimiter = kwargs.pop('delimiter', constants.DEFAULT_DELIMITER)
    with fp:
        if from_dict:
            writer = csv.DictWriter(fp, delimiter=delimiter, **kwargs)
            if header:
                writer.writeheader()
        else:
            writer = csv.writer(fp, delimiter=delimiter, **kwargs)
        writer.writerows(rows)


def write_csv(path, rows, from_dict=False, header=True, **kwargs):
    '''Write rows to path.

    :type path: str
    :param path: Destination CSV file path.

    :type kwargs: keyword arguments
    :param kwargs: extra arguments to be passed to open() and csv.writer().
    '''

    encoding = kwargs.pop('encoding', constants.DEFAULT_ENCODING)
    newline = kwargs.pop('newline', constants.DEFAULT_NEWLINE)
    fp = open(path, mode='w', encoding=encoding, newline=newline)
    fwrite_csv(fp, rows, from_dict=from_dict, header=header, **kwargs)
