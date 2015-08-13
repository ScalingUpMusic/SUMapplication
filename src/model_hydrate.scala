import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.classification.{LogisticRegressionWithSGD, LogisticRegressionModel}

import scala.util.Random


// get data
def extractData(line:String) : Vector = {
    val array_string = "\\[.*?\\]".r
    val ars = (array_string findAllIn line).toList
    val data = (ars(1) replaceAll ("[\\[\\]\\s]", "")).split(",").map(x => x.toDouble)
    
    return Vectors.dense(data)
}



val tag = "uk"

// Get a random data file / song
val dir_letter = (Random.nextInt(26) + 65).toChar
val loadpath = "hdfs://ambari2.scup.net:8020/h52text/[" + dir_letter + "-" + dir_letter + "]"


// Extract data from text file of the form ('TRACKID', (['tag1', 'tag2', ..., 'tagn'], [data1, data2, ..., datam]))
val data_unscaled = sc.textFile(loadpath).take(1)
val data_unscaled_p = sc.parallelize(data_unscaled).map(line => extractData(line))

// Scale data to 0 mean and unit variance
val sclr = new StandardScaler(withMean=true, withStd=true).fit(data_unscaled_p)
val data_unbalanced = data_unscaled_p.map(x => sclr.transform(x))




// Run logistic regression
val model_save_location = "saved_model_" + tag
val model = LogisticRegressionModel.load(sc, model_save_location)
model.clearThreshold()


// Make predictions
val predictionAndLabels = data_unbalanced.map { features =>
    model.predict(features)
}

predictionAndLabels.foreach {
    println(_)
}

