#! /usr/local/bin/python3
"""Function to generate HTML details element from requirement_text."""

import psycopg
import sys

from argparse import ArgumentParser
from dgw_preprocessor import dgw_filter
from psycopg.rows import dict_row


# to_html()
# -------------------------------------------------------------------------------------------------
def to_html(requirement_text):
  """Generate a HTML details element for the code of a Scribe Block."""
  # catalog_type, first_year, last_year, catalog_years_text = catalog_years(row.period_start,
  #                                                                         row.period_stop)
  # institution_name = institution_names[row.institution]
  filtered_text = dgw_filter(requirement_text, remove_comments=False, remove_hidden=False)
  html = f"""
<details>
  <summary><strong>Degree Works Code</strong> (<em>Scribe Block</em>)</summary>
  <hr>
  <pre>{filtered_text.replace('<', '&lt;')}</pre>
</details>
"""

  return html.replace('\t', ' ').replace("'", '’')


if __name__ == '__main__':
  """For development, give an institution/requirement_id and get back the html text."""
  argument_parser = ArgumentParser('Test html generator')
  argument_parser.add_argument('-i', '--institution')
  argument_parser.add_argument('-r', '--requirement_id')
  args = argument_parser.parse_args()
  if args.institution and args.requirement_id:
    institution = f'{args.institution.upper()[0:3]}01'
    requirement_id = f"RA{int(args.requirement_id.upper().strip('RA')):06}"
  else:
    argument_parser.print_usage()
    exit()
  print(f'{institution} {requirement_id}')
  with psycopg.connect('dbname=cuny_curriculum') as conn:
    with conn.cursor(row_factory=dict_row) as cursor:
      cursor.execute("""
      select block_type, block_value, title, period_start, period_stop, requirement_text
        from requirement_blocks
       where institution = %s
         and requirement_id = %s
      """, (institution, requirement_id))
      if cursor.rowcount != 1:
        exit('Not Found')
      else:
        row = cursor.fetchone()
        block_type = row['block_type']
        block_value = row['block_value']
        block_title = row['title']
        period_start = row['period_start']
        period_stop = row['period_stop']
        details = to_html(row['requirement_text'])
  with open(f'{institution}_{requirement_id}.html', 'w') as html_file:
    print(f"""<!DOCTYPE html>
<html>
  <head>
    <title>{institution[0:3]} {requirement_id}</title>
  </head>
  <body>
    <h1>{institution} {requirement_id} {block_type} {block_value} {block_title}
        {period_start}–{period_stop}</h1>
    {details}
  </body>
</html>
""", file=html_file)
  print(f'See {institution}_{requirement_id}.html')
