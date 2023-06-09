#! /usr/local/bin/python3
""" Analylze dgw_ir_active_requirements.csv
    No real surprises here. Blocks that were active previously don't always stay around.
"""
import csv
import psycopg
import sys

from collections import namedtuple, defaultdict
from psycopg.rows import namedtuple_row

active_blocks = dict()
with psycopg.connect('dbname=cuny_curriculum') as conn:
  with conn.cursor(row_factory=namedtuple_row) as cursor:
    cursor.execute("""
    select institution, requirement_id, block_type, block_value, period_stop
      from requirement_blocks
    """)
    for row in cursor.fetchall():
      active_blocks[(row.institution, row.requirement_id)] = (row.block_type,
                                                              row.block_value,
                                                              row.period_stop)

block_types = defaultdict(int)
period_stops = defaultdict(int)
active_terms = defaultdict(int)

num_found = 0
not_found = 0
no_enrollment = 0
with open('archives/dgw_ir_active_requirements.csv') as csv_file:
  reader = csv.reader(csv_file, delimiter='|')
  for line in reader:
    if reader.line_num == 1:
      Row = namedtuple('Row', [col.lower().replace(' ', '_') for col in line])
    else:
      row = Row._make(line)
      block_key = (row.institution, row.dap_req_id)
      try:
        block_type, block_value, period_stop = active_blocks[block_key]
        if int(row.distinct_students) < 1:
          no_enrollment += 1
          continue
        # print(f'{row.institution} {row.dap_req_id} {block_type} {block_value} {period_stop}')
        num_found += 1
        block_types[block_type] += 1
        period_stops[period_stop] += 1
        active_terms[row.dap_active_term] += 1
      except KeyError as err:
        not_found += 1

print(f'Found:         {num_found:7,}')
print(f'Not Found:     {not_found:7,}')
print(f'No Enrollment: {no_enrollment:7,}')

print('\nBLOCK TYPES')
for key, value in block_types.items():
  print(f'  {key:<10} {value:6,}')

print('\nPERIOD STOP VALUES')
for key, value in period_stops.items():
  print(f'{key:<12} {value:6,}')

print('\nACTIVE TERMS')
for key, value in active_terms.items():
  print(f'{key:<12} {value:6,}')
