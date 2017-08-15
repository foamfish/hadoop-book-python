#!/usr/bin/env python
# -*- coding: utf-8 -*-

from mrjob.job import MRJob
import re
import sys
from functools import reduce

class MRMaxTemperature(MRJob):

    def mapper(self, _, line):
        val = line.strip()
        (year, temp, q) = (val[15:19], val[87:92], val[92:93])
        if (temp != "+9999" and re.match("[01459]", q)):
            yield (int(year), int(temp))
    
    def combiner(self, year, temps):
        yield (year, max(temps))

    def reducer(self, year, temps):
        yield (year, max(temps))

if __name__ == '__main__':
    MRMaxTemperature.run()

