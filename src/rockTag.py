import sqlite3
import random
import h5py
from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD
from pyspark.mllib.classification import NaiveBayes, LogisticRegressionWithSGD, SVMWithSGD
from pyspark.rddsampler import RDDSamplerBase
from pyspark.mllib.feature import StandardScaler

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

sc = SparkContext('local', 'Rock Tag')

dbpath = '/root/data/AdditionalFiles/'
# Get list of songs with mbtags, artist, and independent vars

## Get artist mbtags from subset_artist_term.db (table = artist_mbtag)
dbname ='subset_artist_term.db'

with sqlite3.connect(dbpath+dbname) as conn:
	c = conn.cursor()
	c.execute("SELECT artist_id, mbtag FROM artist_mbtag")
	artistTags = sc.parallelize(c.fetchall())

# group tags by artist
artistTagList = artistTags.groupByKey()
print(artistTagList.take(3))


# check if rock in group tag or not
artistRocks = artistTagList.map(lambda (ar, tags): (ar, float(sum(['rock' in tag for tag in tags]) > 0)))
artistRocks.take(3)


# merge song with artist

## Match artist to track using subset_track_metadata.db (table = songs)

dbname ='subset_track_metadata.db'

with sqlite3.connect(dbpath+dbname) as conn:
	c = conn.cursor()
	c.execute("SELECT artist_id, track_id FROM songs")
	trackArtist = sc.parallelize(c.fetchall())

trackArtist.take(3)

trackRocks = trackArtist.leftOuterJoin(artistRocks).map(lambda (ar, (tr, rocks)): (tr, rocks))
trackRocks.take(3)

# get song data and merge

## Get song data from subset_msd_summary_file.h5[analysis][songs]
file_name = 'subset_msd_summary_file.h5'
#songData = sc.parallelize(list(h5py.File(dbpath+file_name, 'r')['analysis']['songs'][:]))
list(h5py.File(dbpath+file_name, 'r')['analysis']['songs'][:])[0].length()
list(h5py.File(dbpath+file_name, 'r')['analysis']['songs'][:])[0][30]
songData = sc.parallelize(h5py.File(dbpath+file_name, 'r')['analysis']['songs'][:]).map(lambda x: (x[30], (x[3], x[4], x[21], x[23], x[24], x[27], x[28])))
songData.take(3)

# 30 = track_id

# 3 = duration
# 4 = danceability
# 21 = key
# 23 = loudness
# 24 = mode
# 27 = tempo
# 28 = time_signature

allData = trackRocks.join(songData).map(lambda (tr, (rocks, data)): (tr, (0.0 if rocks is None else rocks, data)))
allData.take(3)

# label data

# only uses one feature for now
#labeledData = allData.map(lambda (tr, (rocks, data)): LabeledPoint(rocks, [data[6]]))
#labeledData = allData.map(lambda (tr, (rocks, data)): LabeledPoint(rocks, [random.random() + (.5 if rocks == 1 else 0)]))

labels = allData.map(lambda (tr, (rocks, data)): rocks)
features = allData.map(lambda (tr, (rocks, data)): data)
std = StandardScaler(True, True).fit(features)
scaledFeatures = std.transform(features)

labeledData = labels.zip(scaledFeatures).map(lambda (label, data): LabeledPoint(label, data))

# uses all extracted
#labeledData = allData.map(lambda (tr, (rocks, data)): LabeledPoint(rocks, [x for x in data]))

labeledData.take(3)

# make sample sizes equal
labeledRock = labeledData.filter(lambda p: p.label == 1.0)
labeledRock.count()
labeledRock.map(lambda p: p.features[0]).mean()
nrock = labeledRock.count()

labeledNotRock = labeledData.filter(lambda p: p.label != 1.0)
labeledNotRock.map(lambda p: p.features[0]).mean()
nxrock = labeledNotRock.count()

cutoff = float(nrock) / (nrock + nxrock)

# recombine
equalSampleData = labeledRock.union(labeledNotRock)


equalSampleData = labeledData.filter(lambda p: random.random() < cutoff if p.label != 1.0 else True)

# split data
trainData, testData = randomSplit(equalSampleData, [0.9, 0.1])

trainData.map(lambda p: (p.label, p.features)).take(3)

# train model
model = LogisticRegressionWithSGD.train(trainData, intercept = False, iterations=10000)
#model = LinearRegressionWithSGD.train(trainData, step = 0.1, iterations=1000)
#model = SVMWithSGD.train(trainData, step=1, iterations=1000, intercept=True)

# evaluate model
#labelsAndPreds = testData.map(lambda p: (p.label, 1 if model.predict(p.features) > 0.5 else 0))
labelsAndPreds = testData.map(lambda p: (p.label, model.predict(p.features)))

accuracy = labelsAndPreds.filter(lambda (v, p): v == p).count() / float(testData.count())

guess1 = labelsAndPreds.filter(lambda (v, p): p == 1)
precision1 = guess1.filter(lambda (v, p): v == p).count() / float(guess1.count())

act1 = labelsAndPreds.filter(lambda (v, p): v == 1)
recall1 = act1.filter(lambda (v, p): v == p).count() / float(act1.count())

guess0 = labelsAndPreds.filter(lambda (v, p): p == 0)
precision0 = guess0.filter(lambda (v, p): v == p).count() / float(guess0.count())

act0 = labelsAndPreds.filter(lambda (v, p): v == 0)
recall0 = act0.filter(lambda (v, p): v == p).count() / float(act0.count())

trainErr = labelsAndPreds.filter(lambda (v, p): v != p).count() / float(testData.count())
baselineErr = testData.filter(lambda p: p.label == 1.0).count() / float(testData.count())
meanLabel = labelsAndPreds.map(lambda (l, p): l).mean()
meanGuess = labelsAndPreds.map(lambda (l, p): p).mean()

print("0 | Recall = " + str(recall0) + " | Precision = " + str(precision0) + "\n1 | Recall = " + str(recall1) + " | Precision = " + str(precision1) + "\nTest Error = " + str(trainErr) + '\n' + 'Baseline Error = ' + str(baselineErr) + "\nMean Label = " + str(meanLabel) + "\nMean Prediction = " + str(meanGuess))




