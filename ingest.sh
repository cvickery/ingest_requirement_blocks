#! /usr/local/bin/bash

# Ingest new/changed requirement blocks; Parse unparsed blocks; Build requirement mapping files.

# All output is written to a file named ./html, which is emailed when the script terminates.
#  - 2026-03-30 Leave ./html in place for diagnostic purposes rather than deleting it.

(
  # Be sure we’re in the right place
  cd "$HOME"/Projects/ingest_requirement_blocks || ( echo "Unable to cd to ingest project"; exit 1 )

  now=$(date "+%Y-%m-%d %H:%M")
  # Initialize the ./html file
  echo "<h1> Ingest.sh on $(hostname) at $now</h1>" > html

  # Make sure there is a complete set of dap_req_blocks
  if [[ -f downloads/dgw_dap_req_block.csv ]]
  then
    num_colleges=$("$HOME"/bin/count_blocks ./downloads/dgw_dap_req_block.csv | wc -l)
    if (( num_colleges < 21 ))
    then
      cat << EOD > html 2>&1
  <h3>Incomplete dgw_dap_req_block.csv at $now</h3>
  <p>$num_colleges colleges</p>"
EOD
      sendemail -s "ingest.sh failure on $(hostname)" -h html christopher.vickery@qc.cuny.edu
      exit 1
    fi
  else
    cat << EOD > html 2>&1
  <h3>Missing dgw_dap_req_block.csv at $now</h3>
EOD
    sendemail -s "ingest.sh failure on $(hostname)" -h html christopher.vickery@qc.cuny.edu
    exit 1
  fi

  # Ingest new blocks
  {
    echo "<h3>Ingest Requirement Blocks at $now</h3><pre>"
    ./ingest_requirement_blocks.py
  } > html 2>&1

  # Email Ingest Report
  sendemail -s "Ingest on $(hostname)" -h html christopher.vickery@qc.cuny.edu


  # PUNT: Parse UNparsed and Timeouts with short, then long, timelimits
  dgws="$HOME"/Projects/dgw_processor
  {
    echo "<h3>PUNT Report $now</h3><pre>"
    echo -e "\nUnparsed:"
    "$dgws"/parse_active.py -u
    echo -e "\nTimeouts:"
    "$dgws"/parse_active.py -tl 3600
    echo "</pre>"
  } > html 2>&1
  # Email PUNT report
  sendemail -s "PUNT on $(hostname)" -h html christopher.vickery@qc.cuny.edu

  # Copy the newly-archived files back to ./downloads
  if [[ $# -gt 0 ]]
  then "$HOME"/bin/check_oareda_files.py --extract
  fi

  # Run requirements mapper
  now=$(date "+%Y-%m-%d %H:%M")
  {
    echo "<h3>SMAPREP Report $now</h3><pre>"
    "$HOME"/Projects/requirement_mapper/smaprep.sh
  } > html 2>&1
  # Email Mapper Report
  sendemail -s "SMAPREP on $(hostname)" -h html christopher.vickery@qc.cuny.edu > html

)
