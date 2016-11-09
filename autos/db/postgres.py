import csv
import glob
import logging
import tempfile
import itertools
import collections
from random import choice
from string import ascii_lowercase

import psycopg2

import autos.utils.file as file
import autos.constants as constants


logger = logging.getLogger(__name__)


class Postgres:
    @classmethod
    def connect(cls, *args, **kwargs):
        """Establish connection to database."""

        return cls(conn=psycopg2.connect(*args, **kwargs))

    def __init__(self, conn):
        self.encoding = constants.DEFAULT_ENCODING
        self.delimiter = constants.DEFAULT_DELIMITER
        self.conn = conn
        self._search_path = None

    @property
    def search_path(self):
        return self._search_path

    @search_path.setter
    def search_path(self, value):
        default = ('"$user"', 'public')
        if value:
            search_path = (value,) + default
        else:
            search_path = tuple(default)
            value = None

        param = ','.join(search_path)
        sql = 'SET search_path TO {};'.format(param)
        self.execute(sql)
        self._search_path = value

    def get_encoding(self, encoding):
        """Returns encoding argument, use default if None.

        :type encoding: str
        :param encoding: encoding argument.
        """

        if encoding is None:
            return self.encoding
        else:
            return encoding

    def get_delimiter(self, delimiter):
        """Returns delimiter argument, use default if None.

        :type delimiter: str
        :param delimiter: delimiter argument.
        """

        if delimiter is None:
            return self.delimiter
        else:
            return delimiter

    def get_columns(self, columns):
        """Returns columns argument, use default if None.

        :type columns: str
        :param columns: columns argument.
        """

        if columns is None:
            return ''
        else:
            return '({})'.format(','.join(columns))

    def get_header(self, header):
        """Returns header argument, use default if None.

        :type header: str
        :param header: header argument.
        """

        if header:
            return 'TRUE'
        else:
            return 'FALSE'

    def get_null(self, null):
        """Returns null argument, use default if None.

        :type null: str
        :param null: null argument.
        """

        if null is None:
            return "''"
        else:
            return null

    def open_csv(self, filename, mode='r', encoding=None):
        """Open a CSV file."""

        encoding = self.get_encoding(encoding)
        return open(filename, mode=mode, encoding=encoding, newline='')

    def execute(self, query, parameters=()):
        """Execute an SQL statement.

        :type query: string
        :param query: SQL query string.
        """

        with self.conn, self.conn.cursor() as cursor:
            cursor.execute(query, parameters)

    def select(self, query, parameters=(), arraysize=-1, with_header=True):
        """Execute a SELECT statement.

        Always use %% for % literal. See:
        http://stackoverflow.com/questions/14054920/psycopg2-indexerror-tuple-index-out-of-range-error-when-using-like-operat

        :type query: string
        :param query: SQL query string.

        :type arraysize: int
        :param arraysize: If arraysize == -1, then fetchall will be used,
                          else fetchmany(arraysize).

        :rtype: iterator
        :returns: Iterator of result rows.
        """

        with self.conn, self.conn.cursor() as cursor:
            cursor.execute(query, parameters)
            if arraysize == -1:
                rows = cursor.fetchall()
            else:
                cursor.arraysize = arraysize
                rows = itertools.chain.from_iterable(iter(cursor.fetchmany, []))

            header = tuple(desc[0] for desc in cursor.description)
            Row = collections.namedtuple('Row', header)
            if with_header:
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

    def extract(
        self,
        filename,
        query,
        delimiter=None,
        encoding=None,
        header=True,
        null=None,
        use_copy=True,
    ):
        """Extract the result of a SELECT query into a CSV file.

        :type filename: str
        :param filename: Query result CSV file path.

        :type query: str
        :param query: SELECT query to be executed.

        :type delimiter: str
        :param delimiter: CSV delimiter.
        """

        delimiter = self.get_delimiter(delimiter)
        header = self.get_header(header)
        file = self.open_csv(filename, mode='w', encoding=encoding)
        if use_copy:
            null = self.get_null(null)
            copy_sql_template = "COPY ({query}) " \
                                "TO STDOUT WITH (" \
                                "FORMAT CSV," \
                                "HEADER {header}," \
                                "NULL {null}," \
                                "DELIMITER '{delimiter}'," \
                                "ENCODING '{encoding}'" \
                                ")"
            copy_sql = copy_sql_template.format(
                query=query,
                header=header,
                null=null,
                delimiter=delimiter,
                encoding=file.encoding,
            )
            with file, self.conn, self.conn.cursor() as cursor:
                cursor.copy_expert(copy_sql, file)
        else:
            rows = self.select(query=query, with_header=header)
            self.to_file(rows, file=file, delimiter=delimiter)

    def dump(
        self,
        filename,
        table_name,
        columns=None,
        delimiter=None,
        encoding=None,
        header=True,
        null=None,
    ):
        """Dump a table into a file.

        :type filename: str
        :param filename: Query result CSV file path.

        :type table_name: str
        :param table_name: Table name to be dumped.

        :type columns: list or None
        :param columns: List of columns of the table to be dumped.

        :type delimiter: str
        :param delimiter: CSV delimiter.
        """

        file = self.open_csv(filename, mode='w', encoding=encoding)
        copy_sql_template = "COPY {table_name} {columns} " \
                            "TO STDOUT WITH (" \
                            "FORMAT CSV," \
                            "HEADER {header}," \
                            "NULL {null}," \
                            "DELIMITER '{delimiter}'," \
                            "ENCODING '{encoding}'" \
                            ")"

        delimiter = self.get_delimiter(delimiter)
        columns = self.get_columns(columns)
        header = self.get_header(header)
        null = self.get_null(null)
        copy_sql = copy_sql_template.format(
            table_name=table_name,
            columns=columns,
            header=header,
            null=null,
            delimiter=delimiter,
            encoding=file.encoding,
        )
        with file, self.conn, self.conn.cursor() as cursor:
            cursor.copy_expert(copy_sql, file)

    def load_from_file(
        self,
        file,
        table_name,
        columns=None,
        header=True,
        null=None,
        delimiter=None,
        truncate_table=False,
    ):
        """Load data from a file into a table using copy_expert(). See:
        https://www.postgresql.org/docs/current/static/sql-copy.html

        :type file: file-object
        :param file: Source file object.

        :type table_name: str
        :param table_name: Destination table name.

        :type columns: list or None
        :param columns: List of columns of the table to be dumped.

        :type header: str
        :param header: If true, the header will be skipped before loading the data.

        :type null: str
        :param null: Specifies the string that represents a null value.

        :type delimiter: str or None
        :param delimiter: CSV delimiter. If None, the default will be used.

        :type truncate_table: bool
        :param truncate_table: If true, the table will be truncated before loading the data.
        """

        delimiter = self.get_delimiter(delimiter)
        columns = self.get_columns(columns)
        header = self.get_header(header)
        null = self.get_null(null)
        copy_sql_template = "COPY {table_name} {columns} " \
                            "FROM STDIN WITH (" \
                            "FORMAT CSV," \
                            "HEADER {header}," \
                            "NULL {null}," \
                            "DELIMITER '{delimiter}'," \
                            "ENCODING '{encoding}'" \
                            ")"

        copy_sql = copy_sql_template.format(
            table_name=table_name,
            columns=columns,
            header=header,
            null=null,
            delimiter=delimiter,
            encoding=file.encoding,
        )
        with file, self.conn, self.conn.cursor() as cursor:
            if truncate_table:
                cursor.execute('TRUNCATE TABLE {}'.format(table_name))
            cursor.copy_expert(copy_sql, file)

    def load_from_filename(self, filename, table_name, encoding=None, **load_kwargs):
        """Load data from a file `filename` into a table `table_name`.

        :type table_name: str
        :param table_name: Destination table name.

        :type encoding: str or None
        :param encoding: File encoding. If None, the default will be used.
        """

        file = self.open_csv(filename, encoding=encoding)
        self.load_from_file(file, table_name=table_name, **load_kwargs)

    def load_from_filenames(self, table_name, glob_pattern='', paths=(), encoding=None, **load_kwargs):
        """Load files from a list of paths or pathnames that match glob pattern.

        :type table_name: str
        :param table_name: Destination table name.

        :type glob_pattern: str
        :param glob_pattern: Pattern to match pathnames.
                             This argument will be considered first before `paths`.

        :type paths: iterable
        :param paths: An iterable of paths.

        :type encoding: str or None
        :param encoding: File encoding. If None, the default will be used.
        """

        if not glob_pattern and not paths:
            raise ValueError("glob_pattern or paths must be provided.")

        if glob_pattern:
            paths = glob.iglob(glob_pattern)

        for path in paths:
            self.load_from_filename(
                filename=path,
                table_name=table_name,
                encoding=encoding,
                **load_kwargs,
            )

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
        self.execute(sql)

    def dblink_bq(self, bqio, dest, query='', source='', dir=None, **export_kwargs):
        if dir is None:
            dir = tempfile.gettempdir()

        if query:
            paths = bqio.copy_csv_to(query, dir, **export_kwargs)
        elif source:
            paths = bqio.dump(source, dir, **export_kwargs)
        else:
            raise ValueError('Either query or source must be supplied.')
        self.load_from_filenames(table_name=dest, paths=paths)
