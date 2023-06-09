#! /usr/local/bin/python3

""" Add "Except" phrase to MinGrade rules for Spring 2020.
    Not adopted in this form by CUNY, but retained for historical record.
"""
import re
import sys
import csv
import argparse
from pathlib import Path
from datetime import datetime, timezone
from collections import namedtuple
import codecs

import pgconnection

parser = argparse.ArgumentParser()
parser.add_argument('-d', '--debug', action='store_true', default=False)
parser.add_argument('-v', '--verbose', action='store_true', default=False)
parser.add_argument('-de', '--delimiter', default=',')
parser.add_argument('-q', '--quotechar', default='"')
args = parser.parse_args()

csv.field_size_limit(sys.maxsize)

# Get latest dap_req_block.csv from OIRA
that = None
# Get latest  QNS_GENED csv from downloads and say that that's that.
them = Path().glob('./archives/dap_req_block*.csv')
for this in them:
  if that is None or this.stat().st_mtime > that.stat().st_mtime:
    that = this
if that is None:
  sys.exit('No dap_req_block.csv file found')

# Force the input file to be valid utf-8 text.
with codecs.open(that, 'r', encoding='utf-8', errors='replace') as infile:
  reader = csv.reader(infile, delimiter=args.delimiter, quotechar=args.quotechar)
  with open('pnc.csv', 'w') as outfile:
    writer = csv.writer(outfile)
    for row in reader:
      new_text = []
      old_text = row[22]
      lines = old_text.split('\n')
      for line in lines:
        new_text.append(re.sub(r'^\s*([Mm][Ii][Nn][Gg][Rr][Aa][Dd][Ee]\s+\d.\d)',
                               r' \1 Except @ @ (With DWTERM = 1202U)',
                               line))
      row[22] = '\n'.join(new_text)
      writer.writerow(row)
