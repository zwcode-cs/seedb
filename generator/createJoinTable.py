import createSingleTable
import sys

if __name__ == '__main__':
  first = True
  # get number of tables and selectivities of sequential joins
  ntables = int(sys.argv[1]) 
  ctr = 2
  selectivities = []
  for i in range(ntables-1):
    selectivities = selectivities + [float(sys.argv[ctr])]
    ctr = ctr + 1

  prev_nrows = None # used in join_range
  for i in range(ntables):
    # read in number of row, num dimensions, measures, multiplicities
    nrows = int(sys.argv[ctr])
    ndims = int(sys.argv[ctr+1])
    nmeasures = int(sys.argv[ctr+2])
    ctr = ctr + 3
    mult = [0] * ndims
    for i in range(ndims):
        mult[i] = int(sys.argv[ctr])
        ctr = ctr + 1

    # create the first table without a foreign key column. All subsequent 
    # tables will have foreign key columns referring to primary key of 
    # previous table
    if first:
      createSingleTable.generate_single_table(nrows, ndims, nmeasures, mult)
    else:
      createSingleTable.generate_single_table(nrows, ndims, nmeasures, mult, 
        join=True, join_range = prev_nrows, selectivity=selectivities[i], 
        ntable=i+1)

    # update loop vars
    prev_nrows = nrows
    first = False