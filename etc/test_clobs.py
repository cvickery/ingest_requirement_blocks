#! /usr/local/bin/python3
""" Tell the sizes of CLOBS and whether or not they have END. lines
    Used for integrity checks back in the days when the export from SQL Developer was truncating
    them. Now it could be used as a general integrity check. But it isn't.
"""


import csv
import sys
from collections import namedtuple

csv.field_size_limit(sys.maxsize)

if len(sys.argv) != 2:
  sys.exit('Usage: test_clobs file.csv')

cols = None
with open(sys.argv[1]) as csvfile:
  reader = csv.reader(csvfile)
  for line in reader:
    if cols is None:
      cols = [col.replace(' ', '_').lower() for col in line]
      Row = namedtuple('Row', cols)
    else:
      if len(line) > 29:
        row = Row._make(line)
        if r'END.' not in row.requirement_text.upper():
          no_end = 'NO END'
        else:
          no_end = 'END OK'
        print(f'{row.institution} {row.period_stop:>10} {row.requirement_id} '
              f'{row.block_type:6} {row.block_value:12} {len(row.requirement_text):6,} {no_end}')
      else:
        print(f'IGNORE |{line}|', file=sys.stderr)
