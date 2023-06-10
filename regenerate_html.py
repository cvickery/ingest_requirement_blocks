#! /usr/local/bin/python3
"""Replace the requirement_html field for all rows in the requirement_blocks table."""

import psycopg
import sys
import time

from psycopg.rows import namedtuple_row

from scribe_to_html import to_html

if __name__ == '__main__':
  """Fetch requirement_text for all rows; update requirement_html for each."""
  start = time.time()
  with psycopg.connect('dbname=cuny_curriculum') as conn:
    with conn.cursor(row_factory=namedtuple_row) as fetch_cursor:
      with conn.cursor() as update_cursor:
        fetch_cursor.execute("""select institution, requirement_id, requirement_text
                                  from requirement_blocks
                             """)
        num_blocks = fetch_cursor.rowcount
        counter = 0
        for row in fetch_cursor:
          counter += 1
          print(f'\r{counter:,}/{num_blocks:,}', end='')
          requirement_html = to_html(row.requirement_text)
          update_cursor.execute("""update requirement_blocks set requirement_html = %s
                                    where institution = %s and requirement_id = %s
                                """, (requirement_html, row.institution, row.requirement_id))

  print(f'\n{round(time.time() - start)} seconds')
