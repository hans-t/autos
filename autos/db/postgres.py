import os
import csv
import logging
import tempfile
import itertools
from random import choice
from string import ascii_lowercase
from collections import namedtuple


DELIMITER = '\t'
ENCODING = 'utf-8'

logger = logging.getLogger(__name__)


def get_random_cursor_name(length=7):
    return ''.join(choice(ascii_lowercase) for i in range(length))


def extract_table(conn, query, itersize=-1, use_named_cursor=False, with_header=False):
    """
    :type conn: psycopg2.connection
    :param conn: connection object returned by psycopg2.connect.

    :type query: string
    :param query: SQL query string.

    :type itersize: int
    :param itersize: If itersize == -1, then fetchall, else fetchmany(itersize)

    :type use_named_cursor: boolean
    :param use_named_cursor: If true, then use server side cursor, else client side cursor.
    """

    cursor_name = get_random_cursor_name() if use_named_cursor else None
    with conn.cursor(cursor_name) as cursor:
        cursor.execute(query)
        if cursor_name:
            cursor.itersize = 10000 if itersize == -1 else itersize
            rows = itertools.chain([next(cursor)], cursor)
        else:
            if itersize == -1:
                rows = cursor.fetchall()
            else:
                cursor.arraysize = itersize
                rows = itertools.chain.from_iterable(iter(cursor.fetchmany, []))

        header = [desc[0] for desc in cursor.description]
        if with_header:
            yield header

        Row = namedtuple('Row', header)
        for row in rows:
            yield Row(*row)


def write_csv(filename, rows, delimiter=DELIMITER, encoding=ENCODING):
    """
    :type filename: string
    :param filename: CSV filename.

    :type rows: list
    :param rows: A list of rows to be written to the csv file.

    :type delimiter: string
    :param delimiter: CSV delimiter.

    :param encoding: string
    :param encoding: CSV file encoding.
    """
    logger.info("Writing data to {}".format(filename))
    with open(filename, 'w', newline='', encoding=encoding) as fp:
        writer = csv.writer(fp, delimiter=delimiter)
        writer.writerows(rows)


def load_table(conn, table_name, filename, columns=None,
               delimiter=DELIMITER, encoding=ENCODING, size=8192,
               truncate_table=True, with_header=False):
    """
    :type conn: psycopg2.connection
    :param conn: connection object returned by psycopg2.connect.

    :type table_name: string
    :param table_name: Destination table name.

    :type filename: string
    :param filename: Name of the source file to be loaded to the table

    :type columns: list
    :param columns: A list of column names. Provide this if you do not want to copy all columns.
                    Else, None will copy all columns.

    :type delimiter: string
    :param delimiter: The delimiter of source CSV file.

    :type encoding: string
    :param encoding: The encoding of source CSV file.

    :type size: integer
    :param size: Chunk size of copy_from method.

    :type truncate_table: boolean
    :param truncate_table: If true, then the destination table will be truncated before copy.
    """

    with conn, conn.cursor() as cursor:
        if truncate_table:
            logger.info("Truncating {} table".format(table_name))
            cursor.execute("TRUNCATE TABLE {}".format(table_name))

        logger.info("Copying from file {} to {} table".format(filename, table_name))
        with open(filename, 'r', newline='', encoding=encoding) as fp:
            if with_header:
                next(fp)
            cursor.copy_from(file=fp, table=table_name, sep=delimiter,
                             columns=columns, size=size, null='')


def copy_table(query,
               src_conn,
               dst_conn,
               dst_table_name,
               use_named_cursor=False,
               truncate_table=False,
               itersize=-1):
    """
    Run query and import result to another database.

    We use null="" in copy_from, because csv library dumps None as "" (empty string).

    :type query: string
    :param query: Query to be executed on source database.

    :type src_conn: psycopg2.connection
    :param src_conn: Source database connection.

    :type dst_conn: psycopg2.connection
    :param dst_conn: Destination database connection.

    :type dst_table_name: string
    :param dst_table_name: Table name in destination database.

    :type truncate_table: boolean
    :param truncate_table: If true, destination table will be truncated before loading the data.
    """
    rows = extract_table(src_conn, query, use_named_cursor=use_named_cursor, itersize=itersize)
    with tempfile.NamedTemporaryFile('w+t', encoding=ENCODING, newline='') as fp:
        logger.info("Writing result to a temporary file.")
        csv.writer(fp, delimiter=DELIMITER).writerows(rows)
        fp.flush()
        fp.seek(0)

        with dst_conn, dst_conn.cursor() as cursor:
            if truncate_table:
                logger.info("Truncating {}.".format(dst_table_name))
                cursor.execute('TRUNCATE {};'.format(dst_table_name))
            logger.info("Copying result to {}".format(dst_table_name))
            cursor.copy_from(file=fp, table=dst_table_name, sep=DELIMITER, null='')


def dump(conn, query, filename, with_header=False):
    """Execute query and dump data to filename.

    :type conn: `psycopg2.connection`
    :param conn: Database connection instance.

    :type query: string
    :param query: SQL SELECT query to be executed.

    :type filename: string
    :param filename: Output filename.
    """
    rows = extract_table(conn, query, with_header=with_header)
    write_csv(filename, rows)


def write_csv2(rows, fieldnames=None, file=None, filename=None):
    if filename is not None:
        file = open(filename, 'w', encoding=ENCODING, newline='')

    if file is None:
        raise ValueError('Please specify either filename or file.')

    with file:
        if fieldnames is None:
            writer = csv.writer(file, delimiter=DELIMITER)
        else:
            writer = csv.DictWriter(
                file,
                fieldnames=fieldnames,
                delimiter=DELIMITER,
                extrasaction='ignore',
            )
        writer.writerows(rows)


def write_temp_csv(rows, fieldnames=None, prefix='', dir=None):
    logger.info('Writing to a temporary CSV file.')
    file = tempfile.NamedTemporaryFile(
        mode='w+t',
        encoding=ENCODING,
        newline='',
        prefix=prefix,
        dir=dir,
        suffix='.csv',
        delete=False,
    )
    with file:
        write_csv2(rows=rows, fieldnames=fieldnames, file=file)
    return file.name


def load_table2(conn, table_name, fields, filename=None, file=None):
    if filename is not None:
        file = open(filename, encoding=ENCODING, newline='')

    if file is None:
        raise ValueError('Please specify either filename or file.')

    copy_sql = "COPY {table_name} ({columns}) FROM STDIN "\
               "WITH CSV " \
               "NULL '' " \
               "DELIMITER '{delimiter}'".format(
        table_name=table_name,
        columns=','.join(fields),
        delimiter=DELIMITER,
    )

    logger.info('Loading data to table {}.'.format(table_name))
    with file, conn, conn.cursor() as cursor:
        cursor.copy_expert(sql=copy_sql, file=file)


def remove_file(filename):
    logger.info('Deleting file {}.'.format(filename))
    try:
        os.remove(filename)
    except FileNotFoundError:
        pass


def load_rows(conn, *, rows, table_name, prefix='', fields=None, dir=None):
    if rows:
        logger.info('Loading rows.')
        filename = write_temp_csv(rows, fieldnames=fields, prefix=prefix, dir=dir)
        load_table2(conn, table_name=table_name, fields=fields, filename=filename)
        remove_file(filename)
