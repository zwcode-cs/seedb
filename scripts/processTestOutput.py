import string
import sys


def processFile(filename):
  f = open(filename)
  d = {}
  ctrs = {}
  for line in f:
    parts = line.split(':')
    key = parts[0]
    val = int(parts[1])
    if key in d:
      d[key] = d[key] + val
      ctrs[key] = ctrs[key] + 1
    else:
      d[key] = val
      ctrs[key] = 1

  strs = ["Initialize", "Optimizer", "DBMS execution target view", "Client-side processing target view", "DBMS execution comparison view", "Client-side processing comparison view", "DBMS execution put into temp tables", "Executor", "nQueries", "Total time"]
  for key in strs:
    if key in d:
      print key, d[key]/(1.0 * ctrs[key])
    else:
      print key, ""


if __name__ == '__main__':
  filename = sys.argv[1]
  processFile(filename)
