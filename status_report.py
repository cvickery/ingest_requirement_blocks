#! /usr/local/bin/python3
"""Generate status report for email to stakeholders."""

import csv
import datetime
import sys
from pathlib import Path

csv.field_size_limit(1024 * 1024 * 1024)


# status_report()
# -------------------------------------------------------------------------------------------------
def status_report(dap_req_block_date: str, irdw_load_date: str, front_matter: str) -> str:
  """Generate HTML report."""
  return_str = """
  <style>
  * {
    font-family: sans-serif;
    }
  .label {
    display: inline-block;
    width: 15em;
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
  </style>

  """

  return_str += f"""
  <div>
    <p><span class="label">DAP_REQ_BLOCK File Date:</span> {dap_req_block_date}</p>
    <p><span class="label">IRDW_LOAD_DATE:</span> {irdw_load_date}</p>
  </div>

  {front_matter}

  <table><tr><th>File</th><th>File Size</th><th>File Date</th><th>Generate Date</th></tr>
        """

  units = ['B', 'KB', 'MB', 'GB', 'TB']
  home_dir = Path.home()
  files = Path(home_dir, 'Projects/transfer_app/static/csv').glob('dgw_*')

  for file in files:
    name = file.name.replace('course_mapper.', '')
    size = file.stat().st_size
    index = 0
    while size > 1024:
      size /= 1024
      index += 1
      if index > len(units):
        break
    size_str = f'{size:,.1f} {units[index]}'
    mtime = file.stat().st_mtime
    file_date = datetime.datetime.fromtimestamp(mtime)
    file_date_str = str(file_date)[0:19]
    reader = csv.reader(open(file, newline=''))
    for line in reader:
      if reader.line_num == 2:
        generate_date = line[-1]
        return_str += f"""
  <tr><td>{name}</td><td>{size_str}</td><td>{file_date_str}</td><td>{generate_date}</td>
        """
        continue
  return_str += '</table>'

  return return_str


if __name__ == '__main__':
  if len(sys.argv) != 3:
    exit(f'Usage: {sys.argv[0]} dap_req_block_date irdw_load_date')
  print(status_report(sys.argv[1], sys.argv[2], '<p>This is front matter.</p>'))
