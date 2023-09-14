#! /usr/local/bin/python3
"""Copy the latest-avalable versions of the two query files to latest_queries/ .

Run this on babbage (or trexlabs) for use as a data source to a development system.
Use ./sync to update the archives folder on the development system with the contents
of latest_queries/ where this script was run.
"""

import shutil
from pathlib import Path

if __name__ == '__main__':
  latest_dir = Path('./latest_queries')
  if not latest_dir.is_dir():
    latest_dir.mkdir()

  archive_dir = Path('./archives')
  assert archive_dir.is_dir()

  req_blocks = archive_dir.glob('dgw_dap_req_block*')
  active_blocks = archive_dir.glob('dgw_ir_active_requirements*')

  latest_req_block = sorted(list(req_blocks))[-1]
  latest_active_block = sorted(list(active_blocks))[-1]

  for cruft_file in latest_dir.glob('*'):
    cruft_file.unlink()

  shutil.copy2(latest_req_block, latest_dir)
  shutil.copy2(latest_active_block, latest_dir)
