import decimal
import unittest

import autos.utils.number as number


class TestDround(unittest.TestCase):
    def test_returns_correct_rounded_decimal(self):
        actual = number.dround(decimal.Decimal('3.1457'), 2)
        expected = decimal.Decimal('3.15')
        self.assertEqual(actual, expected)
