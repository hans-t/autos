import unittest

import autos.utils.string as string


class TestTitleCase(unittest.TestCase):
    def test_returns_correct_titlecased_strings(self):
        actual = [
            string.titlecase('FOX'),
            string.titlecase('fox'),
            string.titlecase('Fox'),
            string.titlecase('foX'),
            string.titlecase('FoX'),
            string.titlecase('a Fox jump over The SHEEP'),
        ]

        expected = [
            'Fox',
            'Fox',
            'Fox',
            'Fox',
            'Fox',
            'A Fox Jump Over The Sheep',
        ]
        self.assertEqual(actual, expected)
