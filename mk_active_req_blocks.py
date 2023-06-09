#! /usr/local/bin/python3
"""Create the active_req_blocks table.

Terminology:
  A dap_req_block is a Degree Works "Degree Audit Process REQuirement BLOCK" which includes the
  Scribe code that specifies a set of requirements, along with attributes (metadata) to identify
  the block (requirement_id, block_type, block_value and range of academic years, for example)

  Current blocks are in dap_req_block (local table name is requirement_blocks) with a
  period_stop attribute that starts with '9', meaning they are in effect for the current
  academic year.

  Active blocks are dap_req_blocks for degrees, majors, minors, concentrations, and some other
  types of block (other and libl) for which students who are currently in attendance at the
  college and are registered for the program. OAREDA provides an daily update of these blocks,
  including their enrollment sizes, for each term the block was active.

  Programs includes both :
    - academic plans (dap_req_block type is MAJOR or MINOR)
    - academic subplans (dap_req_block type is CONC)
"""

import csv
import datetime
import json
import os
import psycopg
import sys
import time

from checksize import check_size
from collections import namedtuple, defaultdict
from pathlib import Path
from psycopg.rows import namedtuple_row
from quarantine_manager import QuarantineManager

quarantined_dict = QuarantineManager()

BlockInfo = namedtuple('BlockInfo', 'block_type block_value block_title major1 '
                       'period_start period_stop')

if __name__ == '__main__':

  archives_dir = Path('archives')

  start = time.time()
  # Get the latest-available CSV of active program requirement_blocks from OAREDA
  """Find the latest archived version for size-consistency check and for use if there’s nothing
     available in downloads.
  """
  latest_active = None
  for active in archives_dir.glob('*active*'):
    if latest_active is None or active.stat().st_mtime > latest_active.stat().st_mtime:
      latest_active = active

  # Check downloads folder
  downloaded = Path('downloads/dgw_ir_active_requirements.csv')
  if downloaded.is_file():

    file_date = datetime.date.fromtimestamp(downloaded.stat().st_mtime)
    new_name = downloaded.name.replace('.csv', f'_{file_date}.csv')

    # Check new file's size is sane
    if latest_active:
      size_ok = check_size(latest_active.stat().st_size, downloaded.stat().st_size, 0.10)
      if not size_ok:
        print(f'{downloaded} size is not within 10% of {latest_active} (ignored)')
    else:
      size_ok = True

    if size_ok:
      # download is within tolerance
      latest_active = downloaded.rename(Path(archives_dir, new_name))

  if latest_active is None:
    # Fatal
    exit('Make active_req_blocks: no dgw_ir_active_requirements file')
  else:
    print(f'Make active_req_blocks: Using {latest_active.parent}/{latest_active.name}')

  # Create the table of active requirement blocks.
  # Include dap_req_block metadata as well as active enrollment data by term
  with psycopg.connect('dbname=cuny_curriculum') as conn:
    with conn.cursor(row_factory=namedtuple_row) as cursor:

      cursor.execute("""
      drop table if exists active_req_blocks cascade;

      create table active_req_blocks (
      institution text,
      requirement_id text,
      block_type text,
      block_value text,
      block_title text,
      major1 text,
      period_start text,
      period_stop text,
      term_info jsonb,
      foreign key (institution, requirement_id) references requirement_blocks,
      primary key (institution, requirement_id),
      constraint for_plans unique (institution, requirement_id, block_value));
      """)

      # Initialize the is_active column of the requirement_blocks table
      cursor.execute("""
      alter table requirement_blocks drop column if exists term_info cascade;
      alter table requirement_blocks add column term_info jsonb default null;
      DROP VIEW IF EXISTS view_blocks;
      CREATE VIEW view_blocks AS (
      SELECT institution,
             requirement_id,
             block_type,
             block_value,
             title,
             major1,
             period_stop,
             term_info is not null as is_active
        FROM requirement_blocks);
      """)

      # Create dict of metadata for "current" blocks in the dap_req_block (requirement_blocks) table
      cursor.execute(r"""
      select institution, requirement_id, block_type, block_value, title as block_title, major1,
             period_start, period_stop
        from requirement_blocks
       where period_stop ~* '^9'
         and block_value !~* '^\d+$' -- Skip numeric block values
         and block_value !~* '^mhc'  -- Skip Macaulay blocks
       """)
      num_current = 0
      current_blocks = dict()
      # Filter out quarantined blocks.
      for row in cursor.fetchall():
        if quarantined_dict.is_quarantined((row.institution, row.requirement_id)):
          continue
        num_current += 1
        current_blocks[(row.institution, row.requirement_id)] = BlockInfo._make([row.block_type,
                                                                                 row.block_value,
                                                                                 row.block_title,
                                                                                 row.major1,
                                                                                 row.period_start,
                                                                                 row.period_stop])
      print(f'{num_current:,} current blocks')

      # The OAREDA list includes the enrollment for each requirement block for each active term,
      # where an active term is one in which current student(s) at the institution are actually
      # enrolled in a program. Here, that is converted into a timeline of term-enrollment pairs.
      active_blocks = defaultdict(list)
      with open(latest_active, newline='') as csv_file:
        reader = csv.reader(csv_file, delimiter='|')
        for line in reader:
          if reader.line_num == 1:
            Row = namedtuple('Row', ' '.join(col.lower().replace(' ', '_') for col in line))
          else:
            row = Row._make(line)
            try:
              current_block = current_blocks[(row.institution, row.dap_req_id)]
            except KeyError:
              # print(f'{row.institution} {row.dap_req_id} No current block for active block')
              continue
            if (row.institution, row.dap_req_id) not in active_blocks.keys():
              block_dict = current_block._asdict()
              block_dict['terms'] = []
              active_blocks[(row.institution, row.dap_req_id)] = block_dict
            term_info = {'active_term': int(row.dap_active_term.strip('U')),
                         'distinct_students': int(row.distinct_students)}
            active_blocks[(row.institution, row.dap_req_id)]['terms'].append(term_info)

      print(f'{len(active_blocks):,} current blocks that are active')

      # Populate the active_req_blocks table
      # NOTE: Whether a block is currently active or not depends on the date of the most recent
      # active_term and when in the academic year you look. These were all "active blocks" at some
      # point in the past, but not necessarily "now."
      # The activeblocks module will pick out blocks that have been active within the past calendar
      # year. [see activeblocks.py for the exact time interval]
      for key, active_block in active_blocks.items():
        term_info_list = sorted(active_block['terms'], key=lambda x: x['active_term'])

        cursor.execute("""
        insert into active_req_blocks values(%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, [key[0],
              key[1],
              active_block['block_type'],
              active_block['block_value'],
              active_block['block_title'],
              active_block['major1'],
              active_block['period_start'],
              active_block['period_stop'],
              json.dumps(term_info_list, ensure_ascii=False)
              ])

        # Update the is_active column of the block
        cursor.execute("""
        update requirement_blocks set term_info = %s
        where institution = %s
          and requirement_id = %s
        """, (json.dumps(term_info_list, ensure_ascii=False), key[0], key[1]))

seconds = int(round(time.time() - start))
mins, secs = divmod(seconds, 60)
print(f'Make active_req_blocks took {mins} min {secs} sec')
