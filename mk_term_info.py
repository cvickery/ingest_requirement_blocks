#! /usr/local/bin/python3
"""Fill requirement_blocks.term_info column using latest dgw_ir_active_requirements.csv."""

import csv
import json
import psycopg
import re

from collections import defaultdict, namedtuple
from datetime import date
from pathlib import Path

# __main__
# -------------------------------------------------------------------------------------------------
if __name__ == '__main__':

  # Verify directory locations
  home_dir = Path.home()
  latest_dir = Path(home_dir, 'Projects/ingest_requirement_blocks/latest_queries/')
  logs_dir = Path(home_dir, 'Projects/ingest_requirement_blocks/Logs/')
  assert home_dir.is_dir(), 'Home dir not recognized'
  assert latest_dir.is_dir(), 'Latest queries dir not found'
  assert logs_dir.is_dir(), 'Logs dir not found'

  # Get the current query from the latest_queries dir
  latest_query = Path(latest_dir, 'dgw_ir_active_requirements.csv')
  if not (latest_query and latest_query.is_file()):
    # Fatal
    exit('No dgw_ir_active_requirements file available.')

  print(f'DGW_IR_ACTIVE_REQUIREMENTS: {latest_query.name}')
  csv_reader = csv.reader(latest_query.open('r', newline=''), delimiter='|')

  # The OAREDA list includes the enrollment for each requirement block for each active term,
  # where an active term is one in which current student(s) at the institution are actually
  # enrolled in a program. Here, that is converted into a timeline of term-enrollment pairs.
  active_blocks = defaultdict(list)
  irdw_load_date = None
  for line in csv_reader:
    if csv_reader.line_num == 1:
      Row = namedtuple('Row', ' '.join(col.lower().replace(' ', '_') for col in line))
    else:
      row = Row._make(line)
      if irdw_load_date is None:
        irdw_load_date = row.irdw_load_date
        print(f'IRDW_LOAD_DATE: {irdw_load_date}')
      else:
        assert irdw_load_date == irdw_load_date

      if re.findall(r'RA\d{6}', row.dap_req_id):
        term_info = {'active_term': int(row.dap_active_term.strip('U')),
                     'distinct_students': int(row.distinct_students)}
        active_blocks[(row.institution, row.dap_req_id)].append(term_info)

  log_pathname = Path(logs_dir, f'mk_term_info_{date.today()}.log')
  with log_pathname.open('w') as log_file:
    with psycopg.connect('dbname=cuny_curriculum') as conn:
      with conn.cursor() as cursor:
        # Clear all values from the term_info column of the requirement_blocks table.
        cursor.execute("""
        update requirement_blocks set term_info = Null;
        """)

        # Add the term_info list for each active block
        num_set = 0
        for key, value in active_blocks.items():
          # Sort by active_term so most-recent is last term in the list
          value = sorted(value, key=lambda d: d['active_term'])
          institution, requirement_id = key
          cursor.execute("""
          update requirement_blocks set term_info = %s
           where institution = %s
             and requirement_id = %s
          """, (json.dumps(value), institution, requirement_id))

          if cursor.rowcount != 1:
            # Print the last active term for missing rows
            print(f'{institution} {requirement_id} {value[-1]["active_term"]}', file=log_file)
          else:
            num_set += 1

    print(f'{len(active_blocks):9,} active blocks')
    print(f'{num_set:9,} matching blocks found')
    if num_set < len(active_blocks):
      print(f'{len(active_blocks) - num_set:9,} missing blocks logged to Logs/{log_pathname.name}')
