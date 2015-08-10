import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.classification.{LogisticRegressionWithSGD, LogisticRegressionModel}

import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics



// get data
def extractData(line:String, tag:String) : (Double, Vector) = {
    val array_string = "\\[.*?\\]".r
    val ars = (array_string findAllIn line).toList
    val hastag = if (ars(0) contains tag) 1.0 else 0.0
    val data = (ars(1) replaceAll ("[\\[\\]\\s]", "")).split(",").map(x => x.toDouble)
    
    return (hastag, Vectors.dense(data))
}



val tag = "uk"
val verbose = true
val datapath = "hdfs://ambari2.scup.net:8020/h52text/"
val dir_start = "A"
val dir_end = "A"
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



// Run logistic regression
val model_save_location = "saved_model_" + tag
val model = LogisticRegressionModel.load(sc, model_save_location)
model.clearThreshold()


// Make predictions
val predictionAndLabels = data_balanced.map { features =>
    model.predict(features._2)
}

predictionAndLabels.foreach {
    println(_)
}

