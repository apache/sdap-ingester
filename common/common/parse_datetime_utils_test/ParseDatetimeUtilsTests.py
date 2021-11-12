from parse_datetime_utils import ParseDatetimeUtils, DayMode
import unittest
from datetime import datetime


class ParseDatetimeUtilsTests(unittest.TestCase):
    def test_month_mode(self):
        day_mode      = DayMode.MONTH_MODE
        start_date_cg = (1, 2)
        end_date_cg   = (3, 4)
        filename      = 'V5GL01.HybridPM25.NorthAmerica.200706-200908.nc'
        pattern       = '((?:19|20)[0-9]{2})([0-9]{2})-((?:19|20)[0-9]{2})([0-9]{2})'
        #                 -----------------  --------   -----------------  --------
        #                          1            2               3             4

        parser        = ParseDatetimeUtils(pattern, start_date_cg, end_date_cg,
                                           day_mode=day_mode)
        start_datetime, end_datetime = parser.parse(filename)

        self.assertEqual(start_datetime, datetime(2007, 6, 1))
        self.assertEqual(end_datetime, datetime(2009, 8, 1))

    def test_day_of_year(self):
        day_mode      = DayMode.DAY_OF_YEAR_MODE
        start_date_cg = (1, 2)
        filename      = 'A2017005.L3m_CHL_chlor_a_4km.nc'
        pattern       = '((?:19|20)[0-9]{2})([0-9]{3})'
        #                 -----------------  --------
        #                          1            2

        parser        = ParseDatetimeUtils(pattern, start_date_cg, day_mode=day_mode)
        start_datetime, end_datetime = parser.parse(filename)

        self.assertEqual(start_datetime, end_datetime)
        self.assertEqual(start_datetime, datetime(2017, 1, 5))

    def test_month_and_day_mode(self):
        day_mode      = DayMode.MONTH_AND_DAY_MODE
        start_date_cg = (1, 2, 3)
        filename      = 'V4NA03_PM25_NA_20040115-RH35.nc'
        pattern       = '((?:19|20)[0-9]{2})([0-9]{2})([0-9]{2})'
        #                 -----------------  --------  --------
        #                          1            2          3

        parser        = ParseDatetimeUtils(pattern, start_date_cg, day_mode=day_mode)
        start_datetime, end_datetime = parser.parse(filename)

        self.assertEqual(start_datetime, end_datetime)
        self.assertEqual(start_datetime, datetime(2004, 1, 15))


if __name__ == '__main__':
    unittest.main()
