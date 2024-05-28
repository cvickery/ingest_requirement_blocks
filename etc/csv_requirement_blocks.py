#! /usr/local/bin/python3

""" Update requirement block information.

    2019-07-26
    This version works with the CUNY-wide dap_req_block table maintained by OIRA, instead of the
    separate tables used in requirement_blocks.py.

    The last two columns of the csv are the college in QNS01 format and the date in DD-MMM-YY
    format. (DD-MMM-YY ... really?)
    datetime.strptime('25-JUL-19', '%d-%b-%y').strftime('%Y-%m-%d') ==> 2019-07-25
"""

import os
import argparse
from pathlib import Path
import csv
import psycopg2
from psycopg2.extras import NamedTupleCursor
from datetime import datetime, timezone
from collections import namedtuple

# CUNY Institutions Not In DegreeWorks
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
parser.add_argument('-v', '--verbose', action='store_true', default=False)
args = parser.parse_args()

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

# Make dict of CSV rows by institution, and be sure irdw_load_dates are all the same
institutions = {}
Institution = namedtuple('Institution', 'load_date rows')
cols = None
with open('queries/DAP_REQ_BLOCK.csv', 'r') as query_file:
  reader = csv.reader(query_file)
  for line in reader:
    if cols is None:
      cols = [col.lower().replace(' ', '_') for col in line]
      Row = namedtuple('Row', cols)
      if args.debug:
        print(cols)
    else:
      row = Row._make(line)
      institution = row.institution.lower().strip('10')
      load_date = datetime.strptime(row.irdw_load_date, '%d-%b-%y').strftime('%Y-%m-%d')
      if institution not in institutions.keys():
        institutions[institution] = Institution._make([load_date, []])
      assert load_date == institutions[institution].load_date, \
          f'{load_date} is not {institutions[institution].load_date} for {institution}'
      institutions[institution].rows.append(row)

# Process the rows for each institution
for institution in institutions.keys():
  load_date = institutions[institution].load_date
  if args.verbose:
    print(f'Found {len(institutions[institution].rows):8,}'
          f' records dated {load_date} for {institution}')
  cursor.execute("select * from updates where institution = %s", (institution, ))
  institution_rowcount = cursor.rowcount
  assert institution_rowcount < 2, f'Multiple rows for {institution} in updates table'
  if institution_rowcount == 1:
    last_update = str(cursor.fetchone().last_update)
  else:
    last_update = ''
  if institution_rowcount == 0 or last_update != load_date:
    cursor.execute(f"delete from requirement_blocks where institution = '{institution}'")
    if args.verbose:
      suffix = 's'
      if cursor.rowcount == 1:
        suffix = ''
      print(f'Replace {cursor.rowcount:6,} requirement block{suffix} '
            f'dated {last_update} for {institution}')
    cursor.execute("""insert into updates values (%s, %s)
                          on conflict (institution)
                          do update set last_update = %s
                   """, (institution, load_date, load_date))
    for row in institutions[institution].rows:
      db_record = DB_Record._make([institution,
                                   row.requirement_id,
                                   row.block_type,
                                   row.block_value,
                                   row.title,
                                   row.period_start,
                                   row.period_stop,
                                   row.major1,
                                   row.major2,
                                   row.concentration,
                                   row.minor,
                                   row.requirement_text])
      cursor.execute(f"insert into requirement_blocks values {vals}", (db_record))

db.commit()
db.close()

# Archive the csv file
os.rename('./queries/DAP_REQ_BLOCK.csv', f'./query_archive/DAP_REQ_BLOCK_{load_date}.csv')

