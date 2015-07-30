import sqlite3
import sys, getopt
import random
import h5py
import math
from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD
from pyspark.mllib.classification import NaiveBayes, LogisticRegressionWithSGD, SVMWithSGD
from pyspark.rddsampler import RDDSamplerBase
from pyspark.mllib.feature import StandardScaler

sc = SparkContext('local', 'One Tag')

def fetchallChunks(cursor, chunksize=10000):
    'uses fetchmany and parallelizes to keep memory usage down'
    fetchrdd = sc.parallelize(cursor.fetchmany(chunksize))
    while True:
        results = cursor.fetchmany(chunksize)
        if not results:
            break
        fetchrdd = fetchrdd.union(sc.parallelize(results))
    return fetchrdd


class RDDRangeSampler(RDDSamplerBase):
	def __init__(self, lowerBound, upperBound, seed=None):
		RDDSamplerBase.__init__(self, False, seed)
		self._lowerBound = lowerBound
		self._upperBound = upperBound
	def func(self, split, iterator):
		self._random = random.Random(self._seed ^ split)
		# mixing because the initial seeds are close to each other
		for _ in range(10):
			self._random.randint(0, 1)
		for obj in iterator:
			if self._lowerBound <= self._random.random() < self._upperBound:
				yield obj

def randomSplit(rdd, weights, seed=None):
	s = float(sum(weights))
	cweights = [0.0]
	for w in weights:
		cweights.append(cweights[-1] + w / s)
	if seed is None:
		seed = random.randint(0, 2 ** 32 - 1)
	return [rdd.mapPartitionsWithIndex(RDDRangeSampler(lb, ub, seed).func, True) for lb, ub in zip(cweights, cweights[1:])]

def getTrackLabels(dbpath, tagstring='rock', verbose=False, usealldata=False, chunksize=10000):

	## Get artist mbtags from subset_artist_term.db (table = artist_mbtag)
	dbname = 'artist_term.db' if usealldata else 'subset_artist_term.db'

	with sqlite3.connect(dbpath+dbname) as conn:
		c = conn.cursor()
		c.execute("SELECT artist_id, mbtag FROM artist_mbtag")
		#artistTags = sc.parallelize(c.fetchall())
		artistTags = fetchallChunks(cursor=c, chunksize=chunksize)

	# group tags by artist
	artistTagList = artistTags.groupByKey()

	if verbose: print(artistTagList.take(3))
	
	# check if rock in group tag or not
	artistRocks = artistTagList.map(lambda (ar, tags): (ar, float(sum([tagstring in tag.lower() for tag in tags]) > 0)))
	
	if verbose: print(artistRocks.take(3))

	# merge song with artist

	## Match artist to track using subset_track_metadata.db (table = songs)

	dbname = 'track_metadata.db' if usealldata else 'subset_track_metadata.db'

	with sqlite3.connect(dbpath+dbname) as conn:
		c = conn.cursor()
		c.execute("SELECT artist_id, track_id FROM songs")
		#trackArtist = sc.parallelize(c.fetchall())
		trackArtist = fetchallChunks(cursor=c, chunksize=chunksize)

	if verbose: print(trackArtist.take(3))

	trackRocks = trackArtist.leftOuterJoin(artistRocks).map(lambda (ar, (tr, rocks)): (tr, rocks))
	
	if verbose: print(trackRocks.take(3))

	return trackRocks.map(lambda (tr, rocks): (tr, 0.0 if rocks is None else rocks))

def getTrackFeatures(dbpath, verbose=False, usealldata=False, chunksize=10000):
	# get song data and merge

	## Get song data from subset_msd_summary_file.h5[analysis][songs]
	file_name = 'msd_summary_file.h5' if usealldata else 'subset_msd_summary_file.h5'
	

	#list(h5py.File(dbpath+file_name, 'r')['analysis']['songs'][:])[0].length()
	#list(h5py.File(dbpath+file_name, 'r')['analysis']['songs'][:])[0][30]
	#songData = sc.parallelize(h5py.File(dbpath+file_name, 'r')['analysis']['songs'][:]).map(lambda x: (x[30], (x[3], x[4], x[21], x[23], x[24], x[27], x[28])))

	nsongs = h5py.File(dbpath+file_name, 'r')['analysis']['songs'].len()
	chunks = int(math.ceil(float(nsongs)/chunksize))

	#songData = sc.parallelize(h5py.File(dbpath+file_name, 'r')['analysis']['songs'][:]).map(lambda x: (x[30], (x[3], x[4], x[21], x[23], x[24], x[27], x[28])))
	
	for i in range(chunks):
		start = i*chunksize
		end = min((i+1)*chunksize, nsongs)
		if i == 0:
			songData = sc.parallelize(h5py.File(dbpath+file_name, 'r')['analysis']['songs'][start:end]).map(lambda x: (x[30], (x[3], x[4], x[21], x[23], x[24], x[27], x[28])))
		else:
			songData = songData.union(sc.parallelize(h5py.File(dbpath+file_name, 'r')['analysis']['songs'][start:end]).map(lambda x: (x[30], (x[3], x[4], x[21], x[23], x[24], x[27], x[28]))))

	if verbose: print(songData.take(3));

	return songData

	# 30 = track_id

	# 3 = duration
	# 4 = danceability
	# 21 = key
	# 23 = loudness
	# 24 = mode
	# 27 = tempo
	# 28 = time_signature

def getLabelsAndFeatures(dbpath, tagstring='rock', verbose=False, usealldata=False):
	# Get list of songs with mbtags, artist, and independent vars
	songData = getTrackFeatures(dbpath, verbose=verbose, usealldata=usealldata)
	trackRocks = getTrackLabels(dbpath, tagstring=tagstring, verbose=verbose, usealldata=usealldata)
	allData = trackRocks.join(songData)
	if verbose: allData.take(3)

	# label data
	labels = allData.map(lambda (tr, (rocks, data)): rocks)
	features = allData.map(lambda (tr, (rocks, data)): data)

	return (labels, features)

def rebalanceSample(labeledData, verbose=False):
	# make sample sizes equal
	labeled1 = labeledData.filter(lambda p: p.label == 1.0)
	labeled1.count()
	labeled1.map(lambda p: p.features[0]).mean()
	n1 = labeled1.count()

	labeled0 = labeledData.filter(lambda p: p.label != 1.0)
	labeled0.map(lambda p: p.features[0]).mean()
	n0= labeled0.count()

	cutoff = float(n1) / (n1 + n0)

	if verbose:
		print('Tags Match : ' + str(n1))
		print('Tags Miss  : ' + str(n0))
		print('Percent    : ' + str(cutoff))

	# recombine
	return labeled1.union(labeled0.filter(lambda p: random.random() < cutoff))


def evaluateModel(model, testData):

	labelsAndPreds = testData.map(lambda p: (p.label, model.predict(p.features)))

	accuracy = labelsAndPreds.filter(lambda (v, p): v == p).count() / float(testData.count())

	guess1 = labelsAndPreds.filter(lambda (v, p): p == 1)
	precision1 = guess1.filter(lambda (v, p): v == p).count() / float(guess1.count())

	act1 = labelsAndPreds.filter(lambda (v, p): v == 1)
	n1 = act1.count()
	recall1 = act1.filter(lambda (v, p): v == p).count() / float(n1)

	guess0 = labelsAndPreds.filter(lambda (v, p): p == 0)
	precision0 = guess0.filter(lambda (v, p): v == p).count() / float(guess0.count())

	act0 = labelsAndPreds.filter(lambda (v, p): v == 0)
	n0 = act0.count()
	recall0 = act0.filter(lambda (v, p): v == p).count() / float(n0)

	trainErr = labelsAndPreds.filter(lambda (v, p): v != p).count() / float(testData.count())
	baselineErr = testData.filter(lambda p: p.label == 1.0).count() / float(testData.count())
	meanLabel = labelsAndPreds.map(lambda (l, p): l).mean()
	meanGuess = labelsAndPreds.map(lambda (l, p): p).mean()

	evalString = "0 | n = " + str(n0) + " | Recall = " + str(recall0) + " | Precision = " + str(precision0) + "\n1 | n = " + str(n1) + " | Recall = " + str(recall1) + " | Precision = " + str(precision1) + "\nTest Error = " + str(trainErr) + '\n' + 'Baseline Error = ' + str(baselineErr) + "\nMean Label = " + str(meanLabel) + "\nMean Prediction = " + str(meanGuess)
	return evalString

def main(argv):

	verbose = False

	dbpath = '/root/data/AdditionalFiles/'
	tagstring = 'rock'
	usealldata = False

	try:
		opts, args = getopt.getopt(argv,"hvd:t:a",["help","verbose","datapath=","tagstring=","alldata"])
	except getopt.GetoptError:
		print 'rockTag.py -d <data path> -t <tag string>'
		sys.exit(2)
	for opt, arg in opts:
		if opt == '-h':
			print 'rockTag.py -d <data path> -t <tag string>'
			sys.exit()
		elif opt in ("-v", "--verbose"):
			verbose = True
		elif opt in ("-d", "--datapath"):
			dbpath = arg
		elif opt in ("-t", "--tagstring"):
			tagstring = str(arg).lower()
		elif opt in ("-a", "--alldata"):
			usealldata = True

	if verbose:
		print('data path: ' + dbpath)
		print('tag string: ' + tagstring)

	labels, features = getLabelsAndFeatures(dbpath, tagstring=tagstring, verbose=verbose, usealldata=usealldata)

	# scale features
	std = StandardScaler(True, True).fit(features)
	scaledFeatures = std.transform(features)

	# make labeled data
	labeledData = labels.zip(scaledFeatures).map(lambda (label, data): LabeledPoint(label, data))
	if verbose: labeledData.take(3)

	# rebalance samples
	equalSampleData = rebalanceSample(labeledData, verbose=verbose)

	# split data
	trainData, testData = randomSplit(equalSampleData, [0.9, 0.1])
	if verbose: trainData.map(lambda p: (p.label, p.features)).take(3)

	# train model
	model = LogisticRegressionWithSGD.train(trainData, intercept = True, iterations=1000)
	#model = LinearRegressionWithSGD.train(trainData, step = 0.1, iterations=1000)
	#model = SVMWithSGD.train(trainData, step=1, iterations=1000, intercept=True)

	evalString = evaluateModel(model, testData)
	print(evalString)

if __name__ == "__main__":
   main(sys.argv[1:])

