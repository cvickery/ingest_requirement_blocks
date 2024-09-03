#! /usr/local/bin/bash

now=$(date "+%Y-%m-%d %H:%M")

# Make sure there is a complete set of dap_req_blocks
if [[ -f downloads/dgw_dap_req_block.csv ]]
then
  num_colleges=$("$HOME"/bin/count_blocks downloads/dgw_dap_req_block.csv | wc -l)
  if (( num_colleges < 21 ))
  then
    cat << EOD > html
<h3>Incomplete dgw_dap_req_block.csv at $now</h3>
<p>$num_colleges colleges</p>"
EOD
    sendemail -s "ingest.sh failure on $(hostname)" -h html christopher.vickery@qc.cuny.edu
    rm html
    exit 1
  fi
else
  cat << EOD > html
<h3>Missing dgw_dap_req_block.csv at $now</h3>
EOD
  sendemail -s "ingest.sh failure on $(hostname)" -h html christopher.vickery@qc.cuny.edu
  rm html
  exit 1
fi

# Ingest new blocks
{
  echo "<h3>Ingest Requirement Blocks at $now</h3><pre>"
  ./ingest_requirement_blocks.py
} > html
# Email Ingest Report
sendemail -s "Ingest on $(hostname)" -h html christopher.vickery@qc.cuny.edu
rm -f html

# PUNT: Parse UNparsed and Timeouts with short, then long, timelimits
dgws="$HOME"/Projects/dgw_processor
{
  echo "<h3>PUNT Report $now</h3><pre>"
  echo -e "\nUnparsed:"
  "$dgws"/parse_active.py -u
  echo -e "\nTimeouts:"
  "$dgws"/parse_active.py -tl 3600
  echo "</pre>"
} >> html
# Email PUNT report
sendemail -s "PUNT on $(hostname)" -h html christopher.vickery@qc.cuny.edu
rm -f html

# If there are any command line arguments, it means to copy the newly-archived files back to
# ./downloads
if [[ $# -gt 0 ]]
then "$HOME"/bin/check_oareda_files.py --extract
fi

# Run requirements mapper
now=$(date "+%Y-%m-%d %H:%M")
{
  echo "<h3>SMAPREP Report $now</h3><pre>"
  "$HOME"/Projects/requirement_mapper/smaprep.sh
} > html
# Email Mapper Report
sendemail -s "SMAPREP on $(hostname)" -h html christopher.vickery@qc.cuny.edu
rm -f html
