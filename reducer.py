#!/usr/bin/env python
import sys

total_flight = 0
current_id = ""

for line in sys.stdin:
    key, value = line.strip().split()

    try:
        value = int(value)
    except ValueError:
        continue

    if current_id == key:
        total_flight += value
    
    else:
        if current_id:
            print ('%s\t%d' % (current_id, total_flight))
        total_flight = value
        current_id = key

if current_id == key:
    print ('%s\t%d' % (current_id, total_flight))
