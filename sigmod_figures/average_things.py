import sys
#import numpy

f = open(sys.argv[1])
s = sys.argv[2]
# read in all the utilities
l = []
for line in f:
	print line
	if line.startswith(s):
		print s
		parts = line.split(':')
		l.append(float(parts[1]))
print l
print numpy.mean(l), numpy.std(l)