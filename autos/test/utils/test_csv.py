import io
import unittest
import collections

import autos.utils.csv as csv


class TestItertuples(unittest.TestCase):
    def test_returns_namedtuples(self):
        csv_file = io.StringIO(
            'col1,col2,col3\n'
            '1,2,3\n' \
            '4,5,6'
        )
        Row = collections.namedtuple('Row', ['col1', 'col2', 'col3'])
        expected = [
            Row('1', '2', '3'),
            Row('4', '5', '6'),
        ]
        actual = list(csv.itertuples(csv_file))
        self.assertEqual(actual, expected)
        self.assertEqual(expected[0].col1, '1')
        self.assertEqual(expected[1].col3, '6')
