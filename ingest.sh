#! /usr/local/bin/bash

./ingest_requirement_blocks.py -p

# Parse unparsed active blocks with short, then long, timelimits
echo -e "\nUnparsed:"
"$dgws"/parse_active.py -u
echo -e "\nTimeouts:"
"$dgws"/parse_active.py -tl 3600
