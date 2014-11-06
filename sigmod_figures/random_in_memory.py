import random
import numpy
# select k random items from l without replacement
def selectK(k, l):
	res = []
	for i in range(k):
		tmp = random.choice(l)
		while tmp in res:
			tmp = random.choice(l)
		res.append(tmp)
	return res	


#f = open("/Users/manasi/Public/top_k/diabetic_data/no_pruning_diabetic_Data.csv_60.txt")

f = open("/Users/manasi/Public/top_k/bank_data/no_pruning_bank-additional-full.csv_70.txt")
# read in all the utilities
l = []
for line in f:
	parts = line.split(',')
	l.append(float(parts[1]))

ks = [1,2,3,4,5,6,7,8,9,10,15,20,25]
means = [0.122450415627, 0.109747419563, 0.109984689869, 0.106721229036, 0.104735036755, 0.103274725968, 0.101996237977, 0.100704331811, 0.0991901871533, 0.0955270270046, 0.0804740527745, 0.0720438924524, 0.0655250999186 ]
#means = [0.2809869607475946, 0.278494614002, 0.276207111512, 0.27309072494, 0.269999949403, 0.26744350836, 0.265508926073, 0.263725001892, 0.262254368314, 0.260637448539, 0.236585575141, 0.21462634366, 0.193996454227]
for idx in range(len(ks)):
	k = ks[idx]
	tmp = []
	# take samples 20 times
	for i in range(100):
		# for each set of k items, find their mean and get difference from actual mean
		tmp.append(abs(numpy.mean(selectK(k, l))- means[idx]))
	print numpy.mean(tmp), numpy.std(tmp)