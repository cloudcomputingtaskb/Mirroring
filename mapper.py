#!/usr/bin/env python
import sys

total_flight = 0
max_flight = 0
current_id = ""
highest_id = ""

for line in sys.stdin:
    key, value = line.strip().split('\t')
    value = int(value)

    if current_id == key:
        total_flight += value
        if total_flight > max_flight:
            max_flight = total_flight
            highest_id = current_id
    
    else:
        if current_id:
            print ('%s\t%d' % (current_id, total_flight))
        total_flight = value
        current_id = key

if current_id == key:
    print ('%s\t%d' % (current_id, total_flight))
