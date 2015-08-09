import os, sys
import h5py
import numpy
import string
from pyspark import SparkContext

# to run:
# $SPARK_HOME/bin/spark-submit --master spark://159.8.21.48:7077 h52hdfs.py

sc = SparkContext(appName='H5 2 Tuples')

def h52tuple(path, data_i=[3,4,21,22,23,24,25,26,27,28,29]):
	#3: duration
	#4: end of fade in
	#21: key
	#22: key confidence
	#23: loudness
	#24: mode
	#25: mode_confidence
	#26: start_of_fade_out
	#27: tempo
	#28: time_signature
	#29: time_signature_confidence
	#30: track id
	song_analysis = h5py.File(path, 'r')['analysis']['songs'][0]
	data = [song_analysis[i] for i in data_i]
	track_id = song_analysis[30]
	tags = list(h5py.File(path, 'r')['musicbrainz']['artist_mbtags'][:])
	return (track_id, (tags, data))


def makeDirList(base_dir, depth=1):
	dir_list = []
	alpha = list(string.ascii_uppercase)
	if depth > 0:
		for a in alpha:
			fdir = base_dir + a + '/'
			dir_list.extend(makeDirList(fdir, depth = depth-1))
		return dir_list
	else:
		return [base_dir]

def main(argv):
	for a in list(string.ascii_uppercase):
		dir_list = sc.parallelize(makeDirList('/gpfs/'+a+'/', depth=2))
		#dir_list = sc.parallelize(makeDirList('/gpfs/A/A/', depth=1))

		file_list = dir_list.flatMap(lambda fdir: [fdir + '/' + f for f in os.listdir(fdir)])
		file_list.collect()

		tagdata = file_list.map(lambda fpath: h52tuple(fpath))
		#print(tagdata.count())

		tagdata.saveAsTextFile('hdfs://ambari2.scup.net:8020/h52text/'+a)
		### ambari
		#salt on each machine 
		#add slat master to host file
		#control cluster from salt master
		#start ambari from salt master

if __name__ == "__main__":
   main(sys.argv[1:])

