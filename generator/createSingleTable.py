import random
import sys
import string
import os

"""
Call function as: python createSingleTable.py nrows ndims nmeasures list of
multiplicities (distinct values) for each dimension column

"""

max_value = 5000
max_dim_value_size = 6
float_format = '{0:.2f}'

def random_string_generator(size=max_dim_value_size, 
  chars=string.ascii_lowercase + string.digits):
  return ''.join(random.choice(chars) for x in range(size))

def generate_single_table(nrows, ndims, nmeasures, mult, join=False, 
  join_range=None, selectivity=1, ntable=1):

  print nrows, ndims, nmeasures, mult, join, join_range, selectivity, ntable
  # figure out filename
  parts = ['table', str(nrows), str(ndims), str(nmeasures)] + \
          [str(ntable)]
  filename = '_'.join(parts)
  fdata = open(filename + '_data.txt', 'w')
  fqueries = open(filename + '_queries.txt', 'w')

  # write queries
  fqueries.write('DROP TABLE IF EXISTS ' + filename + ';\n')
  fqueries.write('CREATE TABLE ' + filename + ' (\n')
  fqueries.write('id INT,\n')
  for i in range(ndims):
    fqueries.write('dim' + str(i+1) +  '_' + str(mult[i]) + ' VARCHAR(' + str(max_dim_value_size) + '),\n')

  for i in range(nmeasures):
    fqueries.write('measure' + str(i+1) + ' DECIMAL(10, 2)')
    if i!= nmeasures-1: 
      fqueries.write(',')
    fqueries.write('\n')

  if join:
    fqueries.write('join_column INT,\n')

  fqueries.write(');\n')
  fqueries.write('COPY ' + filename + ' FROM \'' + os.path.realpath(__file__) + '/' + filename + '_data.txt\'' +  \
   ' DELIMITER \',\' CSV HEADER;')
  fqueries.close()

  # generate lists of possible values for dimensions
  dimvalues = []
  for m in mult:
    values = [random_string_generator() for i in range(m)]
    # generate list of given # of values 
    dimvalues.append(values)

  # write headers
  headers = ['id']
  for i in range(ndims):
    headers.append('dim' + str(i+1) + '_' + str(mult[i]))
  for j in range(nmeasures):
    headers.append('measures' + str(j+1))
  if join:
    headers.append('join_column')

  fdata.write(','.join(headers) + '\n')

  # create the list of join values
  if join:
    join_values = []
    njoin_values = int(join_range * selectivity)
    join_range = [x+1 for x in range(join_range)]
    for i in range(njoin_values):
      tmp = random.randint(0, len(join_range)-1)
      join_values = join_values + [join_range[tmp]]
      join_range = join_range[:i] + join_range[i+1:]

  # create values for each column
  for i in range(nrows):
    rowvals = []
    rowvals.append(i+1)
    for j in range(ndims):
      rowvals.append(dimvalues[j][random.randint(0, mult[j]-1)])
    for j in range(nmeasures):
      rowvals.append(float_format.format(random.random()*max_value))
    if join:
      rowvals.append(random.choice(join_values))
    fdata.write(','.join(str(x) for x in rowvals) + '\n')

  fdata.close()
  fqueries.close()

if __name__ == '__main__':
  nrows = int(sys.argv[1])
  ndims = int(sys.argv[2])
  nmeasures = int(sys.argv[3])

  ctr = 4
  mult = [0] * ndims
  for i in range(ndims):
    mult[i] = int(sys.argv[ctr])
    ctr = ctr + 1

  generate_single_table(nrows, ndims, nmeasures, mult)
  #generate_single_table(nrows, ndims, nmeasures, mult, 
  #      join=True, join_range = 500, selectivity=0.5, 
  #      ntable=2)
