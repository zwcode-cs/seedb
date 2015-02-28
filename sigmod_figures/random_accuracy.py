import random

def find_overlap(expected, actual):
  ctr = 0
  for e in expected:
    for a in actual:
      if (a == e):
        ctr = ctr + 1
        break
#  print len(expected), "::", ctr
  return ctr

# read file
f = open("diabetic_data_all_views.txt", "r")
views = {}
views_list = []
for line in f:
  parts = line.split(",")
  views[parts[0].strip()] =  float(parts[1].strip())
  views_list.append(parts[0].strip())

k_s = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 25]

for k in k_s:
  avg_accuracy = 0
  for i in range(20):
    # select views at random
    selection = []
    for i in range(k):
      selection.append(random.choice(views_list))
    # find accuracy of selection
    avg_accuracy = avg_accuracy + find_overlap(views_list[0:k], selection) * 1.0/k
  print k, ":", avg_accuracy/20


