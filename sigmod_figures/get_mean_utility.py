import sys

f = open(sys.argv[1])
# read in all the utilities
l = []
first = True
for line in f:
	if first:
		first = False
		continue
	if line.startswith("Time:"):
		break
	parts = line.split(',')
	l.append(float(parts[1]))

print sum(l)/len(l)