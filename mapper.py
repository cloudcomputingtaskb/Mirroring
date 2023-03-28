'''
import sys
for line in sys.stdin:
    item = line.strip().split(',')
    passenger_id = item[0]
    print '%s\t%s' % (passenger_id, 1)
'''
# 나중에 추가할 것
# passenger id 정규표현식 확인해서 일치하는 경우만 key로 set
# 예외처리

for line in sys.stdin:
    line = line.strip()
    words = line.split()
    for word in words:
        print '%s\t%s' % (word, 1)
