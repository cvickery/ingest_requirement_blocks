#! /usr/local/bin/python3

""" Update requirement block information.
"""

import argparse
from pathlib import Path
import csv
import psycopg2
from psycopg2.extras import NamedTupleCursor
from datetime import datetime, timezone
from collections import namedtuple

# Not in DegreeWorks
# GRD01 | The Graduate Center
# LAW01 | CUNY School of Law
# MED01 | CUNY School of Medicine
# SOJ01 | Graduate School of Journalism
# SPH01 | School of Public Health

# Map DGW college codes to CF college codes
# BB BAR01 | Baruch College
# BC BKL01 | Brooklyn College
# BM BMC01 | Borough of Manhattan CC
# BX BCC01 | Bronx Community College
# CC CTY01 | City College
# HC HTR01 | Hunter College
# HO HOS01 | Hostos Community College
# JJ JJC01 | John Jay College
# KB KCC01 | Kingsborough Community College
# LC LEH01 | Lehman College
# LG LAG01 | LaGuardia Community College
# LU SLU01 | School of Labor & Urban Studies
# ME MEC01 | Medgar Evers College
# NC NCC01 | Guttman Community College
# NY NYT01 | NYC College of Technology
# QB QCC01 | Queensborough Community College
# QC QNS01 | Queens College
# SI CSI01 | College of Staten Island
# SP SPS01 | School of Professional Studies
# YC YRK01 | York College

parser = argparse.ArgumentParser()
parser.add_argument('-d', '--debug', action='store_true', default=False)
args = parser.parse_args()

institution_mappings = {'bb': 'bar',
                        'bc': 'bkl',
                        'bm': 'bmc',
                        'bx': 'bcc',
                        'cc': 'cty',
                        'hc': 'htr',
                        'ho': 'hos',
                        'jj': 'jjc',
                        'kb': 'kcc',
                        'lc': 'leh',
                        'lg': 'lag',
                        'lu': 'slu',
                        'me': 'mec',
                        'nc': 'ncc',
                        'ny': 'nyt',
                        'qb': 'qcc',
                        'qc': 'qns',
                        'si': 'csi',
                        'sp': 'sps',
                        'yc': 'yrk'}
db_cols = ['institution',
           'requirement_id',
           'block_type',
           'block_value',
           'title',
           'period_start',
           'period_stop',
           'major1',
           'major2',
           'concentration',
           'minor',
           'requirement_text']
vals = '%s, ' * len(db_cols)
vals = '(' + vals.strip(', ') + ')'

DB_Record = namedtuple('DB_Record', db_cols)

db = psycopg2.connect('dbname=cuny_programs')
cursor = db.cursor(cursor_factory=NamedTupleCursor)

# Get list of query files
queries = Path('./queries')
for query in queries.iterdir():
  if query.name.endswith('.csv'):
    if query.name[0:2] in institution_mappings.keys():
      institution = institution_mappings[query.name[0:2]]
      file_time = datetime.fromtimestamp(query.stat().st_mtime, tz=timezone.utc)
      cursor.execute("select last_update from updates where institution = %s", (institution, ))
      assert cursor.rowcount < 2, f'Error: multiple updates for {query.name}'
      if cursor.rowcount == 1:
        last_update = cursor.fetchone().last_update
      if cursor.rowcount == 0 or last_update < file_time:
        cols = None
        db_records = []
        with open(query) as csvfile:
          reader = csv.reader(csvfile)
          for row in reader:
              if cols is None:
                headings = [val.lower().replace(' ', '_').replace('/', '_') for val in row]
                cols = {headings[i]: i for i in range(len(headings))}
                if args.debug:
                  print(cols.keys())
                  for key in cols.keys():
                    if key in db_cols:
                      print(f'{key} ({cols[key]}) IS IN db_cols:')
                    else:
                      print(f'{key} ({cols[key]}) NOT IN db_cols')
              else:
                values = [row[cols[key]] for key in cols.keys() if key in db_cols]
                assert len(values) + 1 == len(db_cols), f'{len(values)+1} is not {len(db_cols)}'
                db_records.append(DB_Record._make([institution] + values))

        cursor.execute(f"delete from requirement_blocks where institution = '{institution}'")
        if args.debug:
          print('replacing {} requirement blocks with {} blocks for {}'
                .format(cursor.rowcount, len(db_records), institution))
        for db_record in db_records:
          cursor.execute(f"insert into requirement_blocks values {vals}", (db_record))
        cursor.execute("""insert into updates values (%s, %s)
                              on conflict (institution)
                              do update set last_update = %s
                       """, (institution, file_time, file_time))
db.commit()











