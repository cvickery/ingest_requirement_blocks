#! /usr/local/bin/python3
"""Copy the latest-avalable versions of the two query files to latest_queries/ .

Run this on babbage (or trexlabs) for use as a data source on a development system.
"""

from pathlib import Path
from shutil import copy

if __name__ == '__main__':
  latest_dir = Path('./latest_queries')
  if not latest_dir.is_dir():
    latest_dir.mkdir()

  archive_dir = Path('./archives')
  assert archive_dir.is_dir()

  req_blocks = archive_dir.glob('dgw_dap_req_block*')
  active_blocks = archive_dir.glob('dgw_ir_active_requirements*')

  print(list(req_blocks)[-1])
  print(list(active_blocks)[-1])
