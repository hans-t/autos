import unittest

import autos.utils.postgres as postgres


class TestValuePlaceholder(unittest.TestCase):
    def test_returns_3_values_placeholder(self):
        actual = postgres.value_placeholder(3)
        expected = "('{}','{}','{}')"
        self.assertEqual(actual, expected)
