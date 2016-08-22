import unittest

import autos.utils.iterable as iterable


class TestDedupe(unittest.TestCase):
    def test_deduplicates_list_of_strings_based_on_length(self):
        strings = ['three', 'four', 'apple', 'moon']
        expected = ['three', 'four']
        actual = list(iterable.dedupe(strings, key=lambda s: len(s)))
        self.assertEqual(actual, expected)

    def test_deduplicates_iterable_of_strings_based_on_length(self):
        strings = (x for x in ['three', 'four', 'apple', 'moon'])
        expected = ['three', 'four']
        actual = list(iterable.dedupe(strings, key=lambda s: len(s)))
        self.assertEqual(actual, expected)

    def test_key_defaults_to_none(self):
        strings = ['three', 'four', 'three', 'moon']
        expected = ['three', 'four', 'moon']
        actual = list(iterable.dedupe(strings))
        self.assertEqual(actual, expected)


class TestPartition(unittest.TestCase):
    def test_partition_4_into_2(self):
        strings = ['three', 'four', 'three', 'moon']
        actual = list(iterable.partition(strings, 2))
        expected = [['three', 'three'], ['four', 'moon']]
        self.assertEqual(actual, expected)

    def test_partition_7_into_3(self):
        strings = ['three', 'four', 'three', 'moon', 'apple', 'gas', 'rose']
        actual = list(iterable.partition(strings, 3))
        expected = [['three', 'moon', 'rose'], ['four', 'apple'], ['three', 'gas']]
        self.assertEqual(actual, expected)


class TestChunkIterable(unittest.TestCase):
    def test_chunk_even_length_iterable_with_odd_size(self):
        strings = ['three', 'four', 'three', 'moon', 'rose']
        actual = list(iterable.chunk_iterable(strings, 3))
        expected = [('three', 'four', 'three'), ('moon', 'rose')]
        self.assertEqual(actual, expected)


