#!/usr/bin/env python
import sys

current_id = ""
current_flight = 0

id_max = ""
flight_max = 0

for line in sys.stdin:
    key, value = line.strip().split()

    try:
        value = int(value)
    except ValueError:
        continue

    if current_id == key:
        current_flight += value
        if current_flight > flight_max:
            flight_max = current_flight
            id_max = current_id
    
    else:        
        current_flight = value
        current_id = key

print ('%s\t%d' % (id_max, flight_max))