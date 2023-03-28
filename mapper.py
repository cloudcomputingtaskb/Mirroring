import sys

for line in sys.stdin:
    item = line.strip().split(',')
    passenger_id = item[0]
    print('%s\t%s', passenger_id, 1)