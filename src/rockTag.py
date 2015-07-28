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

logFile = '/usr/local/spark/logs/rockTag.txt'
sc = SparkContext('local', 'Rock Tag')
logData = sc.textFile(logFile).cache()



#dbpath = '/vagrant/MillionSongSubset/AdditionalFiles/'
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

