#! /usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
from avro import schema
from avro.io import DatumReader
from avro.datafile import DataFileReader

if __name__ == '__main__':

    if len(sys.argv) != 2:
        sys.exit('Usage: %s <data_file>' % sys.argv[0])
   
    avro_file = sys.argv[1]
    reader = DataFileReader(open(avro_file,'rb'),DatumReader())
    for pair in reader:
        sys.stdout.write("left: %s, right: %s. \n" % (pair.get('left'),pair.get('right')))
    reader.close()
