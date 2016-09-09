import io
import unittest
import unittest.mock as mock

import autos.db.postgres as postgres


class TestPostgres(unittest.TestCase):
    def setUp(self):
        patcher = mock.patch.object(postgres, 'psycopg2', autospec=True)
        self.addCleanup(patcher.stop)
        self.mock_psycopg2 = patcher.start()

        patcher = mock.patch('builtins.open', autospec=True)
        self.addCleanup(patcher.stop)
        self.mock_open = patcher.start()

        self.postgres = postgres.Postgres.connect(dsn='host=127.0.0.1 user=ubuntu')
        self.mock_cursor = self.postgres \
                               .conn \
                               .cursor \
                               .return_value \
                               .__enter__ \
                               .return_value

    def test_connect(self):
        self.mock_psycopg2.connect.return_value = 'connection'
        pg = postgres.Postgres.connect(dsn='host=127.0.0.1 user=ubuntu')
        self.assertEqual(pg.conn, 'connection')

    def test_default_encoding(self):
        self.postgres.encoding = 'utf-8'

    def test_default_delimiter(self):
        self.postgres.delimiter = '\t'

    def test_get_encoding(self):
        actual = self.postgres.get_encoding(None)
        expected = self.postgres.encoding

        actual = self.postgres.get_encoding('utf-16')
        expected = 'utf-16'
        self.assertEqual(actual, expected)

    def test_get_delimiter(self):
        actual = self.postgres.get_delimiter(None)
        expected = self.postgres.delimiter

        actual = self.postgres.get_delimiter(';')
        expected = ';'
        self.assertEqual(actual, expected)

    def test_open_csv(self):
        self.postgres.open_csv(filename='test.csv', encoding='utf-32')
        self.mock_open(filename='test.csv', mode='r', encoding='utf-32', newline='')

        self.postgres.open_csv(filename='test.csv', mode='w', encoding='utf-32')
        self.mock_open(filename='test.csv', mode='w', encoding='utf-32', newline='')

    def test_execute(self):
        query = 'SELECT * FROM public.test LIMIT 1'
        self.postgres.execute(query)
        self.postgres.conn.__enter__.assert_called_once_with()
        self.mock_cursor.execute \
            .assert_called_once_with(query)

    def test_select(self):
        self.mock_cursor.description = [['id']]

        self.mock_cursor.fetchall.return_value = [[1], [2]]
        rows = list(self.postgres.select('SELECT * FROM public.test LIMIT 2', arraysize=-1))
        self.mock_cursor.fetchall.assert_called_once_with()
        self.assertEqual(rows[0], ['id'])
        self.assertEqual(rows[1].id, 1)
        self.assertEqual(rows[2].id, 2)

        self.mock_cursor.fetchmany.side_effect = [[[1], [2]], [[3]], []]
        rows = list(self.postgres.select('SELECT * FROM public.test LIMIT 3', arraysize=2))
        self.mock_cursor.fetchall.assert_called_once_with()
        self.assertEqual(rows[0], ['id'])
        self.assertEqual(rows[1].id, 1)
        self.assertEqual(rows[2].id, 2)
        self.assertEqual(rows[3].id, 3)

    @mock.patch.object(postgres.Postgres, 'open_csv', autospec=True)
    def test_extract(self, mock_open_csv):
        self.postgres.extract(
            filename='foo.csv',
            query='SELECT * FROM public.test LIMIT 3',
            delimiter=';',
            encoding='utf-16'
        )
        mock_open_csv.assert_called_once_with(self.postgres, filename='foo.csv', mode='w')

        copy_query = "COPY (SELECT * FROM public.test LIMIT 3) TO STDOUT WITH CSV HEADER NULL '' DELIMITER ';' ENCODING 'utf-16'"
        self.mock_cursor.copy_expert.assert_called_once_with(copy_query, mock_open_csv.return_value)


    @mock.patch.object(postgres.Postgres, 'open_csv', autospec=True)
    def test_dump_with_columns_none(self, mock_open_csv):
        self.postgres.dump(
            filename='foo.csv',
            table_name='public.test',
            delimiter='|',
            encoding='utf-32',
        )
        mock_open_csv.assert_called_once_with(self.postgres, filename='foo.csv', mode='w')

        copy_query = "COPY public.test  TO STDOUT WITH CSV HEADER NULL '' DELIMITER '|' ENCODING 'utf-32'"
        self.mock_cursor.copy_expert.assert_called_once_with(copy_query, mock_open_csv.return_value)

    @mock.patch.object(postgres.Postgres, 'open_csv', autospec=True)
    def test_dump_with_columns(self, mock_open_csv):
        self.postgres.dump(
            filename='foo.csv',
            table_name='public.test',
            columns=['id1', 'id2'],
            delimiter='|',
            encoding='utf-32',
        )
        mock_open_csv.assert_called_once_with(self.postgres, filename='foo.csv', mode='w')

        copy_query = "COPY public.test (id1,id2) TO STDOUT WITH CSV HEADER NULL '' DELIMITER '|' ENCODING 'utf-32'"
        self.mock_cursor.copy_expert.assert_called_once_with(copy_query, mock_open_csv.return_value)

    @mock.patch.object(postgres.Postgres, 'execute', autospec=True)
    def test_truncate(self, mock_execute):
        expected_sql = 'TRUNCATE TABLE public.foo'
        self.postgres.truncate('public.foo')
        mock_execute.assert_called_once_with(self.postgres, expected_sql)

    @mock.patch.multiple(postgres.Postgres, open_csv=mock.DEFAULT, load_from_file=mock.DEFAULT, autospec=True)
    def test_load_from_filename(self, **mocks):
        self.postgres.load_from_filename(
            'foo.csv',
            'public.test',
            encoding='utf-8',
            truncate_table=True,
        )
        mock_open_csv = mocks['open_csv']
        mock_open_csv.assert_called_once_with(self.postgres, 'foo.csv', encoding='utf-8')
        mocks['load_from_file'].assert_called_once_with(
            self.postgres,
            mock_open_csv.return_value,
            table_name='public.test',
            **{'truncate_table': True}
        )
