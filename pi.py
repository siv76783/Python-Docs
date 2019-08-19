from random import random
from operator import add

from pyspark import SparkContext

sc = SparkContext(appName="PythonPi")

n = 10000000
partitions = 4

def f(_):
    x = random() * 2 -1
    y = random() * 2 -1
    return 1 if x ** 2 + y**2 < 1  else 0

count = sc.parallelize(range(1, n+1), partitions).map(f).reduce(add)

print("count= ", count)
print("Pi is roughly %f" % (4.0 * count / n))
