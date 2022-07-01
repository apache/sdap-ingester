from enum import Enum
import re
from functools import reduce
from datetime import datetime, timedelta
from typing import Tuple


class ParseDatetimeUtils:
    def __init__(self, expression: str, sub_str_start: int =None, sub_str_end: int =None, is_range: bool =False):
        self.expression = expression

        self.sub_str_start = sub_str_start
        self.sub_str_end = sub_str_end
        self.is_sub_str = self.sub_str_start is not None and self.sub_str_end is not None

        self.is_range = is_range
        
    def parse(self, input_str: str) -> Tuple[datetime, datetime]:
        input_str = input_str[self.sub_str_start:self.sub_str_end] \
                    if self.is_sub_str else input_str
        
        start_dt, end_dt = (None, None)
        if self.is_range:
            input_str_len = len(input_str)
            input_str_mid = int(input_str_len / 2)
            start_dt      = datetime.strptime(input_str[0:input_str_mid], self.expression)
            end_dt        = datetime.strptime(input_str[input_str_mid+(input_str_len%2):], self.expression)
        else:
            start_dt = datetime.strptime(input_str, self.expression)
            end_dt   = start_dt

        return (start_dt, end_dt)
