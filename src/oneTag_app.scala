// to run as of Aug 8
// ssh root@198.23.83.41
// ssh ambari2
// /usr/hdp/current/spark-client/bin/spark-submit oneTag_app.scala

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.classification.{LogisticRegressionWithSGD, LogisticRegressionModel}
import org.apache.spark.SparkContext

import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics

object OneTagModel {

	// get data
	def extractData(line:String, tag:String) : (Double, Vector) = {
		val array_string = "\\[.*?\\]".r
		val ars = (array_string findAllIn line).toList
		val hastag = if (ars(0) contains tag) 1.0 else 0.0
		val data = (ars(1) replaceAll ("[\\[\\]\\s]", "")).split(",").map(x => x.toDouble)
		return (hastag, Vectors.dense(data))
	}

	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("OneTagModel")
		val sc = new SparkContext(conf)

		val tag = "rock"
		val verbose = true
		val datapath = "hdfs://ambari2.scup.net:8020/h52text/"
		val dir_start = "A"
		val dir_end = "Z"
		val loadpath = datapath + "[" + dir_start + "-" + dir_end + "]"
		// "svm" or "logistic"
		val model_type = "logistic"
		val model_intercept = true

		// Extract data from text file of the form ('TRACKID', (['tag1', 'tag2', ..., 'tagn'], [data1, data2, ..., datam]))
		val data_unscaled = sc.textFile(loadpath).map(line => extractData(line, tag))
		if (verbose) {
			println("VERBOSE - Unscaled Data:\n".concat(data_unscaled.take(4).deep.mkString("\n")))
		}

		// Scale data to 0 mean and unit variance
		val sclr = new StandardScaler(withMean=true, withStd=true).fit(data_unscaled.map(x => x._2))
		val data_unbalanced = data_unscaled.map(x => (x._1, sclr.transform(x._2)))
		if (verbose) {
			println("VERBOSE - Unbalanced Data:\n".concat(data_unbalanced.take(4).deep.mkString("\n")))
		}

		// See how unbalanced our data set is
		val ntag = data_unbalanced.filter(x => x._1 == 1).count().toDouble
		val n = data_unbalanced.count()
		val fraction_notag = ntag/(n-ntag)
		if (verbose) {
			println("VERBOSE - Percent Have Tags:\n".concat((ntag/n).toString))
		}

		// Rebalance data so we have as many 1s as 0s
		val data_balanced = data_unbalanced.sampleByKey(withReplacement=false, fractions=Map(1.0 -> 1.0, 0.0 -> fraction_notag))
		if (verbose) {
			println("VERBOSE - Balanced Data:\n".concat(data_balanced.take(4).deep.mkString("\n")))
		}

		// Split data in to train and test
		val data_splits = data_balanced.map(x => LabeledPoint(x._1, x._2)).randomSplit(Array(0.7, 0.3))
		val train = data_splits(0).cache()
		val test = data_splits(1)

		// Run logistic regression
		val model = if (model_type.toLowerCase == "svm") {
				new SVMWithSGD().setIntercept(model_intercept).run(train)
			} else {
				new LogisticRegressionWithSGD().setIntercept(model_intercept).run(train)	
			}

		// Compute raw scores on the test set.
		val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
				val prediction = model.predict(features)
				(prediction, label)
			}

		val metrics = new BinaryClassificationMetrics(predictionAndLabels)
		val auroc = metrics.areaUnderROC
		val aupr = metrics.areaUnderPR
		println("Area Under ROC = " + auroc)
		println("Area Under Precision-Recall = " +  aupr)

	}

}