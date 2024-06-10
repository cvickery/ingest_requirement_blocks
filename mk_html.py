#! /usr/local/bin/python3
"""Replace null requirement_html fields in the requirement_blocks table."""

import psycopg
import time

from argparse import ArgumentParser
from psycopg.rows import namedtuple_row

from scribe_to_html import to_html

if __name__ == '__main__':
  """Generate requirement_html for all requirement_blocks where it’s missing."""
  start = time.time()
  argparser = ArgumentParser('Generate missing requirement_html for requirement_blocks')
  argparser.add_argument('-p', '--progress', action='store_true',
                         help='enable progress messages')
  args = argparser.parse_args()

  with psycopg.connect('dbname=cuny_curriculum') as conn:
    with conn.cursor(row_factory=namedtuple_row) as fetch_cursor:
      with conn.cursor() as update_cursor:
        fetch_cursor.execute("""select institution, requirement_id, requirement_text
                                  from requirement_blocks
                                  where requirement_html is null
                             """)
        num_blocks = fetch_cursor.rowcount
        counter = 0
        for row in fetch_cursor:
          counter += 1
          if args.progress:
            print(f'\r{counter:,}/{num_blocks:,}', end='')
          requirement_html = to_html(row.institution, row. requirement_id, row.requirement_text)
          update_cursor.execute("""update requirement_blocks set requirement_html = %s
                                    where institution = %s and requirement_id = %s
                                """, (requirement_html, row.institution, row.requirement_id))

  print(f'\n{round(time.time() - start)} seconds')
