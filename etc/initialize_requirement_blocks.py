#! /usr/local/bin/python3

import csv
import re
import sys

from collections import namedtuple
from hashlib import md5
from pathlib import Path

from pgconnection import PgConnection

""" Create the requirement blocks table with latest dap_req_block from OAREDA.
    Once this is done, cuny_requirement_blocks will update just process new, updated, and deleted
    blocks.
    Parsing will be done separately, but requirement_html and hexindex are intitialized here.
"""
if __name__ == '__main__':
  latest = Path('/Users/vickery/Projects/CUNY_Programs/dgw_info/downloads/dap_req_block.csv')
  if not latest.exists():
    latest = None
    archives_dir = Path('/Users/vickery/Projects/CUNY_Programs/dgw_info/archives')
    archives = archives_dir.glob('dgw_dap_req_block*.csv')
    for archive in archives:
      if latest is None or archive.stat().st_mtime > latest.stat().st_mtime:
        latest = archive
    if latest is None:
      sys.exit(f'{file} does not exist, and no archive found.')
  print(f'Using {latest}')
  print('Clearing program requirements table (may take a while) ...', end='')

  # Create the table
  conn = PgConnection()
  cursor = conn.cursor()
  cursor.execute("""
  drop table if exists requirement_blocks cascade;
  create table requirement_blocks (
    institution       text   not null,
    requirement_id    text   not null,
    block_type        text,
    block_value       text,
    title             text,
    period_start      text,
    period_stop       text,
    school            text,
    degree            text,
    college           text,
    major1            text,
    major2            text,
    concentration     text,
    minor             text,
    liberal_learning  text,
    specialization    text,
    program           text,
    parse_status      text,
    parse_date        date,
    parse_who         text,
    parse_what        text,
    lock_version      text,
    requirement_text  text,
    -- Added Values
    requirement_html  text,
    parse_tree        jsonb default '{}'::jsonb,
    hexdigest         text,
    PRIMARY KEY (institution, requirement_id));
  delete from program_requirements;
  """)
  conn.commit()

  print('done \nStart CSV file')
  fields = ("""institution, requirement_id, block_type, block_value, title, period_start,
  period_stop, school, degree, college, major1, major2, concentration, minor, liberal_learning,
  specialization, program, parse_status, parse_date, parse_who, parse_what, lock_version,
  requirement_text, requirement_html, parse_tree, hexdigest""")
  csv.field_size_limit(sys.maxsize)
  with open(latest, newline='', errors='replace') as csvfile:
    csv_reader = csv.reader(csvfile)
    for line in csv_reader:
      if csv_reader.line_num == 1:
        cols = [col.lower().replace(' ', '_') for col in line]
        Row = namedtuple('Row', cols)
      else:
        row = Row._make(line)
        program_title = row.title.replace('\'', '’')
        # Replace tabs with spaces and, for db storage, primes with apostrophes.
        requirement_text = row.requirement_text.replace('\t', ' ').replace("'", '’')
        # Remove all text following “END.” that needs/wants never to be seen, and which messes up
        # parsing anyway.
        requirement_text = re.sub(r'[Ee][Nn][Dd]\.(.|\n)*', 'END.\n', requirement_text)
        requirement_html = (f'<details><summary>Scribe Block</summary><pre>{requirement_text}'
                            f'</pre></details>')
        hexdigest = md5(requirement_text.encode('utf-8'))
        values = (f"""
                  '{row.institution}', '{row.requirement_id}', '{row.block_type}',
                  '{row.block_value}', '{program_title}', '{row.period_start}', '{row.period_stop}',
                  '{row.school}', '{row.degree}', '{row.college}', '{row.major1}', '{row.major2}',
                  '{row.concentration}', '{row.minor}', '{row.liberal_learning}',
                  '{row.specialization}', '{row.program}', '{row.parse_status}', '{row.parse_date}',
                  '{row.parse_who}', '{row.parse_what}', '{row.lock_version}', '{requirement_text}',
                  '{requirement_html}', default, '{hexdigest}'""")
        cursor.execute(f"""
        insert into requirement_blocks ({fields}) values({values})
        """)
conn.commit()
