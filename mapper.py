'''
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
            print '%s\t%d' % (current_id, total_flight)
        total_flight = value
        current_id = key

if current_id == key:
    print '%s\t%d' % (current_id, total_flight)
'''
    
import sys
word = ""
current_word = ""
current_count = 0

for line in sys.stdin:
    line = line.strip()
    word, count = line.split('\t', 1)
    try:
        count = int(count)
    except ValueError:
        continue
    if current_word == word:
        current_count += count
    else:
        if current_word:
            print '%s\t%s' % (current_word, current_count)
        current_count = count
        current_word = word
if current_word == word:
    print '%s\t%s' % (current_word, current_count)
