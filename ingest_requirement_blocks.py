#! /usr/local/bin/python3
"""Update the cuny_programs.requirement_blocks table from a cuny-wide extract.

Preconditions
  A separate process has obtained the latest CSV files from OAREDA, checked that their sizes are
  consistent with the most-recent versions, and moved them into the downloads directory of this
  project. At Lehman, this is done by the check_new_queries cron job. Elsewhere, the files are
  (manually) pulled from Lehman.

Check the downloads directory. If it doesn’t have both dap_req_block and active_requirements CSVs,
there is nothing to do. (If there is just one, alert sysop.)
  Archive both, and use them to replace whatever is in the latest_queries directory.

Ingest the dgw_dap_req block.csv file
  If a row is new, an entire new row is added to requirement_blocks.
  Otherwise, the dap_req_block row is checked for metadata and/or requirement_text changes; log
  changes to the history directory.
  For each new block and each block where the requirement_text field changed:
    Set the dgw_parse_tree, dgw_seconds, dgw_timestamp, and requirement_html values to Null.
    Re-/parsing can take a long time to run, so doing that is deferred to a separate job.
      It may be better to include that in this job ... but not implemented yet.

Ingest the dgw_id_active_requirements.csv file
  Invoke mk_term_info.py to replace (or initialize) the term_info dict for all current blocks.
  Log the latest active term for missing blocks.

Invoke regenerate_html.py to generate missing requirement_html fields for all current blocks.

---------------------------------------------------------------------------------------------------
It took some doing to get the dap_req_block files to transfer to the development system
successfully, so some of the code in this module reflects developmental steps that may no longer
actually be necessary.

Other Notes:

  CUNY Institutions Not In DegreeWorks
  GRD01 | The Graduate Center
  LAW01 | CUNY School of Law
  MED01 | CUNY School of Medicine
  SOJ01 | Graduate School of Journalism
  SPH01 | School of Public Health

  Map of OIRA DGW college codes to PeopleSoft college codes
  BB BAR01 | Baruch College
  BC BKL01 | Brooklyn College
  BM BMC01 | Borough of Manhattan CC
  BX BCC01 | Bronx Community College
  CC CTY01 | City College
  HC HTR01 | Hunter College
  HO HOS01 | Hostos Community College
  JJ JJC01 | John Jay College
  KB KCC01 | Kingsborough Community College
  LC LEH01 | Lehman College
  LG LAG01 | LaGuardia Community College
  LU SLU01 | School of Labor & Urban Studies
  ME MEC01 | Medgar Evers College
  NC NCC01 | Guttman Community College
  NY NYT01 | NYC College of Technology
  QB QCC01 | Queensborough Community College
  QC QNS01 | Queens College
  SI CSI01 | College of Staten Island
  SP SPS01 | School of Professional Studies
  YC YRK01 | York College
"""

import argparse
import csv
import datetime
import difflib
import json
import os
import psycopg
import re
import shutil
import sys
import time

from collections import namedtuple
from pathlib import Path
from psycopg.rows import namedtuple_row
from psycopg.types.json import Json
from sendemail import send_message
from subprocess import run

from scribe_to_html import to_html

# Deal with large CLOBS
csv.field_size_limit(sys.maxsize)

# Deal with incoming data-encoding issues
trans_dict = dict()
for c in range(14, 31):
  trans_dict[c] = None
cruft_table = str.maketrans(trans_dict)


class Action:
  """Container for insert/update bools."""

  def __init__(self):
    """Initialize bools."""
    self.do_insert = False
    self.do_update = False


# decruft()
# -------------------------------------------------------------------------------------------------
def decruft(block):
  """Remove chars in the range 0x0e through 0x1f and return the block otherwise unchanged.

  This is the same thing strip_file does, which has to be run before this program for xml files. But
  for csv files where strip_files wasn’t run, this makes the text cleaner, avoiding possible parsing
  problems.
  """
  return_block = block.translate(cruft_table)

  # Replace tabs with spaces, and primes with u2018.
  return_block = return_block.replace('\t', ' ').replace("'", '’')

  # Remove all text following END. that needs/wants never to be seen, and which messes up parsing
  # anyway. (The preprocessor does this again. No harm done.)
  return_block = re.sub(r'[Ee][Nn][Dd]\.(.|\n)*', 'END.\n', return_block)

  return return_block


# csv_generator()
# -------------------------------------------------------------------------------------------------
def csv_generator(file):
  """Generate rows from a csv export of OIRA’s DAP_REQ_BLOCK table."""
  cols = None
  with open(file, newline='') as query_file:
    reader = csv.reader(query_file,
                        delimiter=args.delimiter,
                        quotechar=args.quotechar)
    for line in reader:
      if cols is None:
        cols = [col.lower().replace(' ', '_') for col in line]
        Row = namedtuple('Row', cols)
      else:
        try:
          # Trim trailing whitespace from lines in the Scribe text; they were messing up checking
          # for changes to the blocks at one point.
          row = Row._make(line)._asdict()
          requirement_text = row['requirement_text']
          row['requirement_text'] = '\n'.join([scribe_line.rstrip()
                                               for scribe_line in requirement_text.split('\n')])
          row = Row._make(row.values())
          yield row
        except TypeError as type_error:
          sys.exit(f'{type_error}: |{line}|')


# __main__()
# -------------------------------------------------------------------------------------------------
if __name__ == '__main__':

  parser = argparse.ArgumentParser()
  parser.add_argument('-p', '--progress', action='store_true')
  parser.add_argument('--log_unchanged', action='store_true')
  parser.add_argument('--testing', action='store_true')
  parser.add_argument('--timing', action='store_true')
  parser.add_argument('--delimiter', default=',')
  parser.add_argument('--quotechar', default='"')
  parser.set_defaults(parse=True)
  args = parser.parse_args()

  hostname = os.uname().nodename

  # Set up email params
  sysops = [{'name': 'Christopher Vickery', 'email': 'Christopher.Vickery@qc.cuny.edu'}]
  subject = f'Requirement block ingestion report from {hostname}'
  sender = {'name': 'T-Rex Labs', 'email': 'christopher.vickery@qc.cuny.edu'}

  # Directories
  home_dir = Path.home()
  downloads_dir = Path(home_dir, 'Projects/ingest_requirement_blocks/downloads')
  archives_dir = Path(home_dir, 'Projects/ingest_requirement_blocks/archives')
  latest_dir = Path(home_dir, 'Projects/ingest_requirement_blocks/latest_queries')
  assert downloads_dir.is_dir() and archives_dir.is_dir() and latest_dir.is_dir()

  # What, where, and when
  www = f'This is {Path(sys.argv[0]).name} at {hostname} on {datetime.date.today()}'

  if args.progress:
    print(www)

  # front_matter is text that will go at the beginning of email reports.
  front_matter = f'<p><strong>{www}</strong></p>'

  # Move queries from downloads/ to archive, and newest pair from archives/ to
  # latest/

  # Check both files are present. The two could be processed independently, but something’s
  # not normal if one is missing.
  download_dapreq = None
  download_active = None
  for file in downloads_dir.glob('*'):
    if file.is_file():

      if args.progress:
        print(f'downloads/{file.name}')

      if file.stem.lower() == 'dgw_dap_req_block':
        if download_dapreq:
          # Should not occur: report to sysop for now
          if args.progress:
            print('Multiple dgw_dap_req_blocks. Keeping only most-recent')
          front_matter += '<p>Multiple dgw_dap_req_blocks. Keeping only most-recent</p>'
          if file.stat().st_ctime <= download_dapreq.stat().st_ctime:
            file.unlink()
          else:
            download_dapreq.unlink()
            download_dapreq = file
        else:
          download_dapreq = file

      elif file.stem.lower() ==  'dgw_ir_active_requirements':
        if download_active:
          # Likewise
          if args.progress:
            print('Multiple dgw_ir_active_requirements. Keeping only most-recent')
          front_matter += '<p>Multiple dgw_ir_active_requirements. Keeping only most-recent</p>'
          if file.stat().st_ctime <= download_active.stat().st_ctime:
            file.unlink()
          else:
            download_active.unlink()
            download_active = file
        else:
          download_active = file

      else:
        if args.progress:
          print(f'Deleted stray download: {file.name}')
        front_matter += f'<p><strong>Deleted stray download: {file.name}</strong></p>'
        file.unlink()

  # Continue?
  if not (download_dapreq and download_active):
    if args.progress:
      print('Empty downloads directory. Nothing to do.')
    front_matter += '<p>Empty downloads directory. Nothing to do.</p>'
    send_message(sysops, sender, subject, front_matter)
    exit()

  # Delete whatever is currently in latest/
  for cruft_file in latest_dir.glob('*'):
    cruft_file.unlink()

  # Copy the new downloads to latest_queries/
  shutil.copy2(download_dapreq, latest_dir)
  shutil.copy2(download_active, latest_dir)

  # Date-stamp downloaded files and move from downloads/ to archives/
  for file in [download_dapreq, download_active]:
    # Get the file's creation (download) date for archival purposes.
    creation_datetime = datetime.datetime.fromtimestamp(file.stat().st_ctime)
    creation_date = creation_datetime.strftime('%Y-%m-%d')
    archives_name = f'{file.stem.lower()}_{creation_date}.csv'
    shutil.move(str(file), archives_dir / archives_name)
    if args.progress:
      print(f'Moved, downloads/{file.name} to archives/')
    front_matter += f'<p>Moved, downloads/{file.name} to archives/</p>'

  # Sanity Checks
  requirement_block = Path(latest_dir, 'dgw_dap_req_block.csv')
  assert requirement_block.is_file()
  actives_block = Path(latest_dir, 'dgw_ir_active_requirements.csv')
  assert actives_block.is_file()

  # Now update the requirement_blocks table from the latest requirements block

  # These are dap_req_block columns with OAREDA additions, plus requirement_html that gets added
  # here, but not dgw_parse_tree and dgw_seconds, which will be set by the parser.
  db_cols = ['institution', 'requirement_id', 'block_type', 'block_value', 'title', 'period_start',
             'period_stop', 'school', 'degree', 'college', 'major1', 'major2', 'concentration',
             'minor', 'liberal_learning', 'specialization', 'program', 'parse_status', 'parse_date',
             'parse_who', 'parse_what', 'lock_version', 'requirement_text', 'requirement_html',
             'irdw_load_date']
  vals = '%s, ' * len(db_cols)
  vals = '(' + vals.strip(', ') + ')'

  DB_Record = namedtuple('DB_Record', db_cols)

  # There used to be an XML generator, but it’s no longer used.
  generator = csv_generator

  start_time = int(time.time())

  empty_parse_tree = json.dumps({})
  file_datetime = datetime.datetime.fromtimestamp(requirement_block.stat().st_ctime)
  file_date = file_datetime.strftime('%Y-%m-%d')
  irdw_load_date = None
  num_rows = num_inserted = num_updated = num_parsed = 0

  for row in generator(requirement_block):
    num_rows += 1

  # Here begins the actual update process
  # -----------------------------------------------------------------------------------------------

  # Process the dgw_dap_req_block file
  with psycopg.connect('dbname=cuny_curriculum') as conn:
    with conn.cursor(row_factory=namedtuple_row) as cursor:
      row_num = 0
      for new_row in generator(requirement_block):

        # Integrity check: all rows must have the same irdw load date.
        # Desired date format: YYYY-MM-DD
        load_date = new_row.irdw_load_date[0:10]
        if re.match(r'^\d{4}-\d{2}-\d{2}$', load_date):
          load_date = datetime.date.fromisoformat(load_date)
        # Alternate format: DD-MMM-YY
        elif re.match(r'\d{2}-[a-z]{3}-\d{2}', load_date, re.I):
          dt = datetime.strptime(load_date, '%d-%b-%y').strftime('%Y-%m-%d')
          load_date = datetime.date(dt.year, dt.month, dt.day)
        else:
          sys.exit(f'Unrecognized load date format: {load_date}')
        if irdw_load_date is None:
          irdw_load_date = load_date
          log_file = open(f'./Logs/update_requirement_blocks_{irdw_load_date}.log', 'w')
          print(f'Using {requirement_block.name} with irdw_load_date {irdw_load_date}')

        if irdw_load_date != load_date:
          sys.exit(f'dap_req_block irdw_load_date ({load_date}) is not “{irdw_load_date}”'
                   f'for {row.institution} {row.requirement_id}')

        row_num += 1
        if args.progress:
          print(f'\r{row_num:,}/{num_rows:,}', end='')

        """ Determine the action to take.
              If this is a new block, do insert
              If this is an existing block and it has changed, do update (Check both
              requirement_text and key metadata for changes)
        """
        action = Action()

        requirement_text = decruft(new_row.requirement_text)
        requirement_html = to_html(row.institution, row.requirement_id, requirement_text)
        text_is_changed = False  # Don’t know yet
        current_parse_tree = None

        # When did the institution last parse the block?
        parse_date = datetime.date.fromisoformat(new_row.parse_date)

        # Check for changes in the data and metadata items that we use.
        changes_str = ''
        cursor.execute(f"""
        select block_type, block_value, period_start, period_stop, major1,
               parse_date, requirement_text, requirement_html, dgw_parse_tree, dgw_seconds
          from requirement_blocks
         where institution = '{new_row.institution}'
           and requirement_id = '{new_row.requirement_id}'
        """)
        if cursor.rowcount == 0:
          action.do_insert = True

        else:
          assert cursor.rowcount == 1, (f'Error: {cursor.rowcount} rows for {new_row.institution} '
                                        f'{new_row.requirement_id}')
          db_row = cursor.fetchone()
          current_dgw_parse_tree = db_row.dgw_parse_tree  # Re-use these if text hasn’t changed
          if current_dgw_parse_tree == 'null':  # Quirk
            current_dgw_parse_tree = None
          current_dgw_parse_date = db_row.parse_date
          current_dgw_parse_secs = db_row.dgw_seconds
          current_dgw_html = db_row.requirement_html

          # Record history of changes to the Scribe block itself
          days_ago = f'{(parse_date - db_row.parse_date).days}'.zfill(3)
          s = '' if days_ago == 1 else 's'
          diff_msg = f'{days_ago} day{s} since previous parse date'

          if text_is_changed := db_row.requirement_text != requirement_text:
            current_dgw_parse_tree = None
            current_dgw_parse_date = None
            current_dgw_parse_secs = None
            current_dgw_html = None
            db_lines = db_row.requirement_text.split('\n')
            new_lines = requirement_text.split('\n')
            prev_len = len(db_lines)
            new_len = len(new_lines)
            if prev_len < new_len:
              changes_str = f'{new_len - prev_len} lines longer.'
            elif (new_len < prev_len):
              changes_str = f'{prev_len - new_len} lines shorter.'
            else:
              changes_str = f'{prev_len:,} lines.'
            action.do_update = True
            with open(f'history/{new_row.institution}_{new_row.requirement_id}_{parse_date}_'
                      f'{days_ago}', 'w') as _diff_file:
              diff_lines = difflib.context_diff([f'{line}\n' for line in db_lines],
                                                [f'{line}\n' for line in new_lines],
                                                fromfile='previous', tofile='changed', n=0)
              _diff_file.writelines(diff_lines)

          # Check for changes to key metadata fields: log any changes and trigger block update
          for item in ['block_type', 'block_value',
                       'major1', 'period_start', 'period_stop']:
            old_value = getattr(db_row, item)
            new_value = getattr(new_row, item)
            if old_value != new_value:
              action.do_update = True
              print(f'{new_row.institution} {new_row.requirement_id} {item}: {old_value} ==> '
                    f'{new_value}', file=log_file)

        # Insert or update the requirement_block as the case may be
        if action.do_insert:
          db_record = DB_Record._make([new_row.institution,
                                       new_row.requirement_id,
                                       new_row.block_type,
                                       new_row.block_value,
                                       decruft(new_row.title),
                                       new_row.period_start,
                                       new_row.period_stop,
                                       new_row.school,
                                       new_row.degree,
                                       new_row.college,
                                       new_row.major1,
                                       new_row.major2,
                                       new_row.concentration,
                                       new_row.minor,
                                       new_row.liberal_learning,
                                       new_row.specialization,
                                       new_row.program,
                                       new_row.parse_status,
                                       parse_date,
                                       new_row.parse_who,
                                       new_row.parse_what,
                                       new_row.lock_version,
                                       requirement_text,
                                       requirement_html,
                                       irdw_load_date
                                       ])

          vals = ', '.join([f"'{val}'" for val in db_record])
          cursor.execute(f'insert into requirement_blocks ({",".join(db_cols)}) values ({vals})')
          assert cursor.rowcount == 1, (f'Inserted {cursor.rowcount} rows\n{cursor.query}')
          print(f'Inserted  {new_row.institution} {new_row.requirement_id} {new_row.block_type} '
                f'{new_row.block_value} {new_row.period_stop}.', file=log_file)
          conn.commit()
          num_inserted += 1

        elif action.do_update:
          # Things that might have changed
          update_dict = {'block_type': new_row.block_type,
                         'block_value': new_row.block_value,
                         'title': decruft(new_row.title),
                         'period_start': new_row.period_start,
                         'period_stop': new_row.period_stop,
                         'parse_status': new_row.parse_status,
                         'parse_date': parse_date,
                         'parse_who': new_row.parse_who,
                         'parse_what': new_row.parse_what,
                         'lock_version': new_row.lock_version,
                         'requirement_text': requirement_text,
                         'requirement_html': requirement_html,
                         'dgw_parse_tree': Json(current_dgw_parse_tree),
                         'dgw_parse_date': current_dgw_parse_date,
                         'dgw_seconds': current_dgw_parse_secs,
                         'irdw_load_date': irdw_load_date,
                         }
          set_args = ','.join([f'{key}=%s' for key in update_dict.keys()])
          cursor.execute(f"""
          update requirement_blocks set {set_args}
           where institution = %s and requirement_id = %s
          """, ([v for v in update_dict.values()] + [new_row.institution, new_row.requirement_id]))
          assert cursor.rowcount == 1, (f'Updated {cursor.rowcount} rows\n{cursor.query}')
          print(f'Updated   {new_row.institution} {new_row.requirement_id} {changes_str}.',
                file=log_file)
          conn.commit()
          num_updated += 1

        else:
          if args.log_unchanged:
            print(f'No change {new_row.institution} {new_row.requirement_id} {new_row.block_type} '
                  f'{new_row.block_value}.', file=log_file)

      cursor.execute(f"""update updates
                            set update_date = '{load_date}', file_name = '{requirement_block.name}'
                          where table_name = 'requirement_blocks'""")

  # Summarize DAP_REQ_BLOCK processing.
  front_matter += f"""
        <div class="hr">
          <p class="label">DAP_REQ_BLOCK</p>
          <p><span class="label">File Date:</span> {file_date}</p>
          <p><span class="label">IRDW_LOAD_DATE:</span> {irdw_load_date}</p>
        </div>"""

  if none_changed := (num_updated + num_inserted == 0):
    # Make this easy to see in the email report to me
    print('\nNO NEW OR UPDATED BLOCKS FOUND\n')
    # and in the email report to Lehman
    front_matter += '<p><strong>No new or updated requirement blocks</strong></p>'
  else:
    s = '' if num_inserted == 1 else 's'
    msg = f'{num_inserted:6,} Requirement Block{s} INSERTED'
    print(msg)
    front_matter += f'<p">{msg}</p>'

    s = '' if num_updated == 1 else 's'
    msg = f'{num_updated:6,} Requirement Block{s} UPDATED'
    print(msg)
    front_matter += f'<p>{msg}</p>'

  if args.timing:
    m, s = divmod(int(round(time.time())) - start_time, 60)
    h, m = divmod(m, 60)
    print(f'  {int(h):02}:{int(m):02}:{round(s):02}')

  # (Re-)generate the requirement_html column of requirement_blocks table if there were any changes
  if not none_changed:
    print('Generate new/changed requirement_blocks.requirement_html')
    substep_start = time.time()
    run_regen = ['./mk_html.py']
    if args.progress:
      run_regen.append('--progress')
    run(run_regen, stdout=sys.stdout, stderr=sys.stdout)
    if args.timing:
      m, s = divmod(int(round(time.time() - substep_start)), 60)
      h, m = divmod(m, 60)
      print(f'  {int(h):02}:{int(m):02}:{round(s):02}')

  print('Populate requirement_blocks.term_info')

  # Start report
  parse_report = """
  <style>
  * {
    font-family: sans-serif;
    }
  .label {
    display: inline-block;
    width: 20em;
    font-weight: bold;
  }
  table {
    border-collapse: collapse;
  }
  td, th {
    border: 1px solid;
    padding: 0.5em;
  }
  th {
    background-color: #eee;
  }
  td:nth-child(2) {
    text-align: right;
  }
  .warning {
    font-weight: bold;
    background-color: #600;
    color: #fff;
  }
  .warning p {
    padding-left: 1em;
  }
  .hr {
    border-top: 2px solid black;
    padding-top: 0.5em;
    max-width: 45em;
  }
  .mono {
    white-space: pre;
    font-family: monospace;
  }
  </style>
  """ + front_matter

  # mk_term_info ingests OAREDA’s dgw_ir_active_requirements.csv files
  result = run(['./mk_term_info.py'], capture_output=True)
  if result.returncode != 0:
    print('\nmk_term_info FAILED!')
    parse_report += f"""
    <div class="warning">
      <p>mk_term_info.py FAILED!</p>
      <p>{result.stderr}</p>
    </div>
    <p><strong>No Term_Info Report</strong></p>
    """
  else:
    paragraphs = result.stdout.split(b'\n')
    term_report = []
    for paragraph in paragraphs:
      if paragraph := str(paragraph, encoding='utf-8'):
        try:
          name, date = paragraph.split(':')
          term_report.append(f'<p><span class="label">{name.strip()}</span>{date.strip()}</p> ')
        except ValueError:
          # No filename:date -- just give the other info
          term_report.append(f'<p class="mono">{paragraph}</p>')

    term_report = '\n'.join(term_report)
    parse_report += f'<div class="hr"><p class="label">MK_TERM_INFO</p>{term_report}</div>'

    # Generate table of un-parsed current blocks, giving most-recent active term.
    # Alert (bool) currently-active un-parsed blocks
    today = datetime.date.today()
    reports_dir = Path('./ingestion_reports')
    if not reports_dir.is_dir():
      reports_dir.mkdir()
    report_file = Path(reports_dir, f'{today}.csv').open('w')
    writer = csv.writer(report_file)
    writer.writerow(['Institution', 'Requirement ID', 'Latest Term', 'This Year'])
    with psycopg.connect('dbname=cuny_curriculum') as conn:
      with conn.cursor(row_factory=namedtuple_row) as cursor:
        cursor.execute("""
        select institution, requirement_id, term_info
          from requirement_blocks
         where dgw_parse_tree is null
           and term_info is not null
           and period_stop ~* '^9'
        order by institution, requirement_id
        """)
        this_year = (today.year - 1900) * 10
        num_warnings = 0
        num_rows = cursor.rowcount
        table_body = ''
        for row in cursor:
          value = row.term_info
          value = sorted(value, key=lambda d: d['active_term'])
          latest_term = value[-1]['active_term']
          class_attribute = ''
          alert = latest_term >= this_year
          if alert:
            num_warnings += 1
          writer.writerow([row.institution, row.requirement_id, latest_term, alert])
    report_file.close()

    s = '' if num_warnings == 1 else 's'
    parse_report += (f'<p class="hr"><strong>{num_rows} Unparsed-block IDs written to '
                     f'{report_file.name}</strong></p>')
    if num_warnings:
      parse_report += f'<div class="warning"><p>{num_warnings} “this year” Alert{s}</p></div>'

  print('Email mapping files status report')
  send_message(sysops, sender, subject, parse_report)

  if args.timing:
    m, s = divmod(int(round(time.time() - substep_start)), 60)
    h, m = divmod(m, 60)
    print(f'  {int(h):02}:{int(m):02}:{round(s):02}')

  m, s = divmod(time.time() - start_time, 60)
  h, m = divmod(m, 60)
  print(f'Total time: {int(h):02}:{int(m):02}:{round(s):02}\n')
