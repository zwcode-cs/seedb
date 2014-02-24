"""
Use this function only when you have a lot of attributes and you don't to 
specify the number of distinct values for each attribute
"""

import createSingleTable
import sys
import random

if __name__ == '__main__':
  nrows = int(sys.argv[1])
  ndims = int(sys.argv[2])
  nmeasures = int(sys.argv[3])
  min_distinct_values = int(sys.argv[4])
  max_distinct_values = int(sys.argv[5])

  print min_distinct_values, max_distinct_values

  possible_distinct_values = [2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000]
  distinct_values_probability = [1, 1, 1, 2, 2, 2, 3, 3, 3, 2, 2, 2]

  num_distinct_values = []
  for x in range(len(possible_distinct_values)):
    print x, possible_distinct_values[x], min_distinct_values <= possible_distinct_values[x]
    print min_distinct_values, possible_distinct_values[x], possible_distinct_values[x], max_distinct_values
    print (min_distinct_values <= possible_distinct_values[x]),  (possible_distinct_values[x] <= max_distinct_values)
    if (min_distinct_values <= possible_distinct_values[x]) and (possible_distinct_values[x] <= max_distinct_values): 
      print "here2"
      for i in range(distinct_values_probability[x]):
      	num_distinct_values += [possible_distinct_values[x]]
    else:
      print "here"
      continue

  print num_distinct_values
  # select data from distict values
  mult = []
  for x in range(int(ndims)):
  	mult += [random.choice(num_distinct_values)]

  createSingleTable.generate_single_table(nrows, ndims, nmeasures, mult)