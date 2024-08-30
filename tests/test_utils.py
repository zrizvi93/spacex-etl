import unittest
from datetime import datetime, timedelta
import pytz
from src.utils import parse_iso_format

class TestParseISOFormat(unittest.TestCase):
    def test_valid_date_with_positive_offset(self):
        date_str = '2024-08-05T12:30:00+02:00'
        expected = datetime(2024, 8, 5, 10, 30, 0, tzinfo=pytz.utc)
        result = parse_iso_format(date_str)
        self.assertEqual(result, expected)

    def test_valid_date_with_negative_offset(self):
        date_str = '2024-08-05T12:30:00-03:00'
        expected = datetime(2024, 8, 5, 15, 30, 0, tzinfo=pytz.utc)
        result = parse_iso_format(date_str)
        self.assertEqual(result, expected)

    def test_valid_date_without_offset(self):
        date_str = '2024-08-05T12:30:00'
        expected = datetime(2024, 8, 5, 12, 30, 0, tzinfo=pytz.utc)
        result = parse_iso_format(date_str)
        self.assertEqual(result, expected)

    def test_invalid_date_format(self):
        date_str = '2024/08/05 12:30:00'
        with self.assertRaises(ValueError):
            parse_iso_format(date_str)