import datetime
import unittest

import autos.utils.date as date


class TestDateRange(unittest.TestCase):
    def test_returns_today_date_as_default(self):
        actual = list(date.date_range())
        expected = [datetime.date.today()]
        self.assertEqual(actual, expected)

    def test_returns_correct_range(self):
        actual = list(date.date_range(
            since=(datetime.date.today() - datetime.timedelta(days=3)),
            until=(datetime.date.today() - datetime.timedelta(days=1)),
        ))

        expected = [
            (datetime.date.today() - datetime.timedelta(days=1)),
            (datetime.date.today() - datetime.timedelta(days=2)),
            (datetime.date.today() - datetime.timedelta(days=3)),
        ]

        self.assertEqual(actual, expected)


class TestGetPastDate(unittest.TestCase):
    def test_returns_today_date_by_default(self):
        actual = date.get_past_date()
        expected = (datetime.date.today() - datetime.timedelta(days=0))
        self.assertEqual(actual, expected)

    def test_returns_past_3_days_ago_date(self):
        actual = date.get_past_date(days=3)
        expected = datetime.date.today() - datetime.timedelta(days=3)
        self.assertEqual(actual, expected)

    def test_returns_past_5_weeks_ago_date(self):
        actual = date.get_past_date(weeks=5)
        expected = datetime.date.today() - datetime.timedelta(weeks=5)
        self.assertEqual(actual, expected)

    def test_returns_past_3_days_and_2_weeks_ago_date(self):
        actual = date.get_past_date(days=3, weeks=2)
        expected = datetime.date.today() - datetime.timedelta(days=3, weeks=2)
        self.assertEqual(actual, expected)

    def test_returns_future_date_on_negative_input(self):
        actual = date.get_past_date(days=-3, weeks=-2)
        expected = datetime.date.today() + datetime.timedelta(days=3, weeks=2)
        self.assertEqual(actual, expected)

