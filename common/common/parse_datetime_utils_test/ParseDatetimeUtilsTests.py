from parse_datetime_utils import ParseDatetimeUtils
import unittest
from datetime import datetime


class ParseDatetimeUtilsTests(unittest.TestCase):
    def test_parse(self):
        filename   = 'V4NA03_PM25_NA_20040115-RH35.nc'
        expression = '%Y%m%d'
        sub_str_start, sub_str_end = (15, 23)
        parser     = ParseDatetimeUtils(expression, sub_str_start=sub_str_start,
                                        sub_str_end=sub_str_end)
        start_datetime, end_datetime = parser.parse(filename)

        self.assertEqual(start_datetime, end_datetime)
        self.assertEqual(end_datetime, datetime(2004, 1, 15))

    def test_parse_range_with_separator(self):
        filename   = 'V5GL01.HybridPM25.NorthAmerica.200706-200908.nc'
        expression = '%Y%m'
        sub_str_start, sub_str_end = (31, 44)
        parser     = ParseDatetimeUtils(expression, sub_str_start=sub_str_start,
                                        sub_str_end=sub_str_end, is_range=True)
        start_datetime, end_datetime = parser.parse(filename)

        self.assertEqual(start_datetime, datetime(2007, 6, 1))
        self.assertEqual(end_datetime, datetime(2009, 8, 1))

    def test_parse_range_without_separator(self):
        filename   = 'V5GL01.HybridPM25.NorthAmerica.200706200908.nc'
        expression = '%Y%m'
        sub_str_start, sub_str_end = (31, 43)
        parser     = ParseDatetimeUtils(expression, sub_str_start=sub_str_start,
                                        sub_str_end=sub_str_end, is_range=True)
        start_datetime, end_datetime = parser.parse(filename)

        self.assertEqual(start_datetime, datetime(2007, 6, 1))
        self.assertEqual(end_datetime, datetime(2009, 8, 1))


if __name__ == '__main__':
    unittest.main()
