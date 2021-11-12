from enum import Enum
import re
from functools import reduce
from datetime import datetime, timedelta
from typing import Tuple


class DayMode(Enum):
    DAY_OF_YEAR_MODE   = 1
    MONTH_AND_DAY_MODE = 2
    MONTH_MODE         = 3


class ParseDatetimeUtils:
    def __init__(self, pattern: str, start_date_cg: tuple,
                 end_date_cg: tuple =(None, None, None),
                 day_mode: DayMode =DayMode(2)):
        self.regex    = re.compile(pattern)
        self.day_mode = day_mode

        # figure out whether end_date_cg is from input or default values
        end_date_cg_is_all_none = reduce(lambda i,j: i and j == None, end_date_cg, True)

        # end_date_cg to should equal to start_date_cg if not set
        end_date_cg = start_date_cg if end_date_cg_is_all_none else end_date_cg
        if self.day_mode == DayMode.DAY_OF_YEAR_MODE:
            if len(start_date_cg) == 2 and len(end_date_cg) == 2:
                self.start_year_cg, self.start_day_cg = start_date_cg
                self.end_year_cg, self.end_day_cg = end_date_cg
            else:
                raise ValueError(f'Invalid arguments for {self.day_mode} mode')

        elif self.day_mode == DayMode.MONTH_AND_DAY_MODE:
            if len(start_date_cg) == 3 and len(end_date_cg) == 3:
                self.start_year_cg, self.start_month_cg, self.start_day_cg = start_date_cg
                self.end_year_cg, self.end_month_cg, self.end_day_cg = end_date_cg
            else:
                raise ValueError(f'Invalid arguments for {self.day_mode} mode')
        
        elif self.day_mode == DayMode.MONTH_MODE:
            if len(start_date_cg) == 2 and len(end_date_cg) == 2:
                self.start_year_cg, self.start_month_cg = start_date_cg
                self.end_year_cg, self.end_month_cg = end_date_cg
            else:
                raise ValueError(f'Invalid arguments for {self.day_mode} mode')
        
        # calculate number of capture groups
        self.number_of_groups = reduce(lambda i,j: i if j == None else i + 1,
            start_date_cg if end_date_cg_is_all_none else start_date_cg + \
            end_date_cg, 0)
        
    def parse(self, inputStr: str) -> Tuple[datetime, datetime]:
        search_res = self.regex.search(inputStr)

        if not search_res or len(search_res.groups()) != self.number_of_groups:
            return (None, None)
        
        if self.day_mode == DayMode.DAY_OF_YEAR_MODE:
            start_year  = int(search_res.group(self.start_year_cg))
            start_day   = int(search_res.group(self.start_day_cg))
            end_year    = int(search_res.group(self.end_year_cg))
            end_day     = int(search_res.group(self.end_day_cg))
            return ((datetime(start_year, 1, 1) + timedelta(start_day-1)),
                    (datetime(end_year, 1, 1) + timedelta(end_day-1)))

        if self.day_mode == DayMode.MONTH_AND_DAY_MODE:
            start_year  = int(search_res.group(self.start_year_cg))
            start_month = int(search_res.group(self.start_month_cg))
            start_day   = int(search_res.group(self.end_day_cg))
            end_year    = int(search_res.group(self.end_year_cg))
            end_month   = int(search_res.group(self.end_month_cg))
            end_day     = int(search_res.group(self.end_day_cg))
            return (datetime(start_year, start_month, start_day),
                    datetime(end_year, end_month, end_day))
        
        if self.day_mode == DayMode.MONTH_MODE:
            start_year  = int(search_res.group(self.start_year_cg))
            start_month = int(search_res.group(self.start_month_cg))
            end_year    = int(search_res.group(self.end_year_cg))
            end_month   = int(search_res.group(self.end_month_cg))
            return (datetime(start_year, start_month, 1),
                    datetime(end_year, end_month, 1))

        return (None, None)
