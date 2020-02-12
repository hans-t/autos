import io
import unittest
import unittest.mock as mock

import credentials
import autos.db.postgres as postgres




class TestPostgresIntegrated(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.postgres = postgres.Postgres.connect(**credentials.POSTGRES)

    def setUp(self):
        self.conn = self.postgres.conn
        create_table_sql = '''
        CREATE TEMP TABLE IF NOT EXISTS test029301 (
            id integer,
            name text
        )
        '''

        insert_values_sql = '''
        INSERT INTO test029301 (id, name) VALUES
            (928, 'john')
            ,(382, 'ray')
            ,(842, 'anne')
            ,(574, 'murray')
            ,(839, 'mary')
        '''
        with self.conn, self.conn.cursor() as cursor:
            cursor.execute(create_table_sql)
            cursor.execute(insert_values_sql)

    def tearDown(self):
        drop_table_sql = '''DROP TABLE IF EXISTS test029301;'''
        with self.conn, self.conn.cursor() as cursor:
            cursor.execute(drop_table_sql)

    def test_execute(self):
        sql = 'DELETE FROM test029301 WHERE id = %s'
        parameters = (928,)
        self.postgres.execute(sql, parameters)

    def test_select(self):
        sql = 'SELECT * FROM test029301 WHERE id IN (928, 382)'
        rows = tuple(self.postgres.select(sql))
        self.assertEqual(len(rows), 3)

        sql = 'SELECT * FROM test029301 WHERE id IN (%s, %s)'
        parameters = (928, 382)
        rows = tuple(self.postgres.select(sql, parameters=parameters))
        self.assertEqual(len(rows), 3)




