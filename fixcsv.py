#! /usr/local/bin/python3
""" Filter to put \r\n between lines and just \n within fields of a csv file.
    While at it, filter out ASCII codes in the range 0x0E and 0x1f
"""

import sys
import argparse

CR = chr(0x0D)
LF = chr(0x0A)

blk = '\x1b[30m'
red = '\x1b[31m'
grn = '\x1b[32m'

parser = argparse.ArgumentParser()
parser.add_argument('-d', '--delimiter', default=',')
parser.add_argument('-q', '--quotechar', default='"')
args = parser.parse_args()

delimiter = args.delimiter
quotechar = args.quotechar


in_field = False

with sys.stdin as infile:
  with sys.stdout as outfile:
    while True:
      ch = infile.read(1)
      if not ch:
        exit()
      # Skip bogus control codes
      if ord(ch) < 0x20 and ord(ch) > 0x0d:
        continue
      if ch == quotechar:
        # Patterns to deal with:
        #   ..., "This is ""ok"" because the quotes are doubled", ...
        #   ..., "This is "not ok" because the quotes are not doubled", ...
        #   That is, a quotechar inside a field doesn't necessarily end the field. It depends on
        #   what came before, and what comes next.
        if in_field:
          # The next non-whitespace has to be a delimiter, otherwise, this char has to be escaped
          # (doubled -- there is no escape char in CSV)
          next_char = infile.read(1)
          if next_char == quotechar:
            # This is the normal, ok, case.
            outfile.write(ch)
            outfile.write(next_char)
            continue
          else:
            # This isn't going to work. What if there is an unescaped string at the end of a field?
            #   ..., "This is a known "problem"", ...
            pass  # I give up
        in_field = not in_field
        # if in_field:
        #   outfile.write(grn)
        # else:
        #   outfile.write(blk)
      if ch == LF and last_char != CR and not in_field:
        # print(f' CR', file=sys.stderr, end='')
        outfile.write(CR)
        last_char = CR
      if ch == CR and in_field:
        continue
      outfile.write(ch)
      # print(f' {ch}', file=sys.stderr, end='')
print()
