# test for pulling data from hdfs in pyspark

import os, sys
import numpy
import string
from pyspark import SparkContext
from ast import literal_eval as make_tuple

sc = SparkContext(appName='Read Tuples')

def main(argv):
	data = sc.textFile('hdfs://ambari2.scup.net:8020/h52text/test').map(lambda s: make_tuple(s))
	print(data.take(4))
	### ambari
	#salt on each machine 
	#add slat master to host file
	#control cluster from salt master
	#start ambari from salt master

if __name__ == "__main__":
   main(sys.argv[1:])