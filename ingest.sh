#! /usr/local/bin/bash

# Ingest new blocks
./ingest_requirement_blocks.py -p >> "$$"

# PUNT: Parse UNparsed and Timeouts
now=$(date "+%Y-%m-%d %H:%M")
echo "<h1>PUNT Report $now</h1><pre>" > "$$"

# Parse unparsed active blocks with short, then long, timelimits
echo -e "\nUnparsed:" >> "$$"
"$dgws"/parse_active.py -u >> "$$"
echo -e "\nTimeouts:" >> "$$"
"$dgws"/parse_active.py -tl 3600 >> "$$"
echo "</pre>" >> "$$"
# Email PUNT report
sendemail -s "PUNT from $(hostname)" -h "$$" christopher.vickery@qc.cuny.edu
rm -f "$$"

# If there are any command line arguments, it means to copy the newly-archived files back to
# ./downloads
if [[ $# > 0 ]]
then "$HOME"/bin/check_oareda_files.py --extract
fi

# Run requirements mapper
now=$(date "+%Y-%m-%d %H:%M")
echo "<h1>SMAPREP Report $now</h1><pre>" > "$$"
"$HOME"/Projects/requirement_mapper/smaprep.sh > $$
sendemail -s "SMAPREP from $(hostname)" -h "$$" christopher.vickery@qc.cuny.edu
rm -f "$$"

