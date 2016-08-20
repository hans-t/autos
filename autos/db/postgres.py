import csv
import logging
import tempfile
import itertools
import collections
from random import choice
from string import ascii_lowercase

import psycopg2

import autos.utils.file as file


logger = logging.getLogger(__name__)


class Postgres:
    def __init__(self):
        self.encoding = 'utf-8'
        self.delimiter = '\t'
        self._conn = None

    @property
    def conn(self):
        if self._conn is not None:
            return self._conn
        else:
            raise RuntimeError('Connection has not been initialized.')

    def get_encoding(self, encoding):
        """Get encoding, use default if None.

        :type encoding: str
        :param encoding: File encoding.
        """

        if encoding is None:
            return self.encoding
        else:
            return encoding

    def get_delimiter(self, delimiter):
        """Get delimiter, use default if None.

        :type delimiter: str
        :param delimiter: File delimiter.
        """

        if delimiter is None:
            return self.delimiter
        else:
            return delimiter

    def open_csv(self, filename, mode='r', encoding=None):
        """Open a CSV file."""

        encoding = self.get_encoding(encoding)
        return open(filename, mode=mode, encoding=encoding, newline='')

    def connect(self, *args, **kwargs):
        """Establish connection to database."""

        self._conn = psycopg2.connect(*args, **kwargs)

    def execute(self, query):
        """Execute an SQL statement.

        :type query: string
        :param query: SQL query string.
        """

        with self.conn, self.conn.cursor() as cursor:
            cursor.execute(query)

    def select(self, query, arraysize=-1):
        """Execute a SELECT statement.

        :type query: string
        :param query: SQL query string.

        :type arraysize: int
        :param arraysize: If arraysize == -1, then fetchall will be used,
                          else fetchmany(arraysize).

        :rtype: iterator
        :returns: Iterator of result rows.
        """

        with self.conn, self.conn.cursor() as cursor:
            cursor.execute(query)
            if arraysize == -1:
                rows = cursor.fetchall()
            else:
                cursor.arraysize = arraysize
                rows = itertools.chain.from_iterable(iter(cursor.fetchmany, []))

            header = [desc[0] for desc in cursor.description]
            Row = collections.namedtuple('Row', header)
            yield header
            yield from map(Row._make, rows)

    def to_file(self, rows, file, delimiter=None):
        """Write rows to a file-object.

        :type rows: iterable
        :param rows: Iterable of rows.

        :type file: file object
        :param file: Destination file object.
        """

        delimiter = self.get_delimiter(delimiter)
        with file:
            writer = csv.writer(file, delimiter=delimiter)
            writer.writerows(rows)

    def to_filename(self, rows, filename, encoding=None, delimiter=None):
        """Write rows to a filename.

        :type rows: iterable
        :param rows: Iterable of rows.

        :type filename: str
        :param filename: Destination file name.
        """

        file = self.open_csv(filename, mode='w', encoding=encoding)
        self.to_file(rows, file, delimiter=delimiter)

    def to_temp_file(self, rows):
        """Write rows to a temporary file.

        :type rows: iterable
        :param rows: Iterable of rows.
        """

        file = tempfile.NamedTemporaryFile(
            mode='w+t',
            encoding=self.encoding,
            newline='',
            suffix='.csv',
            delete=False,
        )
        self.to_file(rows, file)
        return file.name

    def extract(self, filename, query, delimiter=None):
        """Extract the result of a SELECT query into a CSV file.

        :type filename: str
        :param filename: Query result CSV file name.

        :type query: str
        :param query: SELECT query to be executed.

        :type delimiter: str
        :param delimiter: CSV delimiter.
        """

        delimiter = self.get_delimiter(delimiter)
        copy_sql = "COPY ({query}) TO STDOUT WITH CSV HEADER NULL '' DELIMITER '{delimiter}'"
        sql = copy_sql.format(query=query, delimiter=delimiter)
        file = self.open_csv(filename, mode='w')
        with file, self.conn, self.conn.cursor() as cursor:
            cursor.copy_expert(sql, file)

    def dump(self, filename, table_name, columns=None, delimiter=None):
        """Dump a table into a file.

        :type filename: str
        :param filename: Query result CSV file name.

        :type table_name: str
        :param table_name: Table name to be dumped.

        :type columns: list or None
        :param columns: List of columns of the table to be dumped.

        :type delimiter: str
        :param delimiter: CSV delimiter.
        """

        delimiter = self.get_delimiter(delimiter)
        if columns is None:
            columns = ''
        else:
            columns = '({})'.format(','.join(columns))

        copy_sql = "COPY {table_name} {columns} TO STDOUT WITH CSV HEADER NULL '' DELIMITER '{delimiter}'"
        sql = copy_sql.format(table_name=table_name, columns=columns, delimiter=delimiter)

        file = self.open_csv(filename, mode='w')
        with file, self.conn, self.conn.cursor() as cursor:
            cursor.copy_expert(sql, file)

    def load_from_file(
        self,
        file,
        table_name,
        delimiter=None,
        columns=None,
        truncate_table=False,
        with_header=True
    ):
        """Load data from a file into a table.

        :type file: file-object
        :param file: Source file object.

        :type table_name: str
        :param table_name: Destination table name.

        :type delimiter: str or None
        :param delimiter: CSV delimiter. If None, the default will be used.

        :type columns: list or None
        :param columns: List of columns of the table to be dumped.

        :type truncate_table: bool
        :param truncate_table: If true, the table will be truncated before loading the data.

        :type with_header: bool
        :param with_header: If true, the header will be skipped before loading the data.
        """

        delimiter = self.get_delimiter(delimiter)
        with file, self.conn, self.conn.cursor() as cursor:
            if truncate_table:
                sql = 'TRUNCATE TABLE {}'.format(table_name)
                cursor.execute(sql)
            if with_header:
                next(file)
            cursor.copy_from(file, table=table_name, sep=delimiter, null='', columns=columns)

    def load_from_filename(self, filename, table_name, encoding=None, *args, **kwargs):
        """Load data from a file `filename` into a table.

        :type table_name: str
        :param table_name: Destination table name.

        :type encoding: str or None
        :param encoding: File encoding. If None, the default will be used.
        """

        file = self.open_csv(filename, encoding=encoding)
        self.load_from_file(file, table_name=table_name, *args, **kwargs)

    def load_rows(self, rows, table_name, columns=None, truncate_table=False):
        """Load rows into a table.

        :type rows: iterable
        :param rows: Iterable of rows.

        :type table_name: str
        :param table_name: Destination table name.

        :type columns: list or None
        :param columns: List of columns of the table to be dumped.

        :type truncate_table: bool
        :param truncate_table: If true, then the table will be truncated before loading the data.
        """

        filename = self.to_temp_file(rows)
        self.load_from_filename(
            filename,
            table_name=table_name,
            columns=columns,
            truncate_table=truncate_table,
        )
        file.remove_file(filename)

    def truncate(self, table_name):
        """Truncate a table `table_name`.

        :type table_name: str
        :param table_name: Name of table to be truncated.
        """

        sql = 'TRUNCATE TABLE {}'.format(table_name)
        with self.conn, self.conn.cursor() as cursor:
            cursor.execute(sql)
