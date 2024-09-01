#! /usr/local/bin/bash

# Ingest new blocks
./ingest_requirement_blocks.py -p >> "$$"

# Parse Unparsed aNd Timeouts
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
