import sys
# 
for line in sys.stdin:
<<<<<<< HEAD
    line = line.strip()
    words = line.split()
    for word in words:
        print(word, 1)
=======
    #
    words = line.strip().split(',')
    #
    for word in words:
        #
        print ('%s\t%s' % (word, 1))
>>>>>>> e277d91 (commit_test)
