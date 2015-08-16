import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.classification.{LogisticRegressionWithSGD, LogisticRegressionModel}

import scala.util.Random


// Parse string data into audio features
def extractData(line:String) : Vector = {
    val array_string = "\\[.*?\\]".r
    val ars = (array_string findAllIn line).toList
    val data = (ars(1) replaceAll ("[\\[\\]\\s]", "")).split(",").map(x => x.toDouble)
    
    return Vectors.dense(data)
}

def extractTrackId(line:String) : String = {
    val track_id = line.split("'")[1]
    return track_id
}


// Get a random list of songs by picking a random letter
val dir_letter = (Random.nextInt(26) + 65).toChar
val file_path  = "hdfs://ambalt1.sum.net:8020/h52text/[" + dir_letter + "-" + dir_letter + "]"

// Grab a random song from that random list, and get audio features
val one_song_raw = sc.textFile(file_path).sample(false, 0.001).take(1).mkString("")
val one_song_features = extractData(one_song_raw)



/*
The code below COULD be parallelized by simply wrapping the tag List in sc.parallelize()
but sadly, there is some kind of Spark bug that gives this error when you do:

15/08/15 23:55:02 ERROR Executor: Exception in task 1.0 in stage 21.0 (TID 28)
java.lang.NullPointerException
	at org.apache.spark.mllib.util.Loader$.loadMetadata(modelSaveLoad.scala:125)
	at org.apache.spark.mllib.classification.LogisticRegressionModel$.load(LogisticRegression.scala:171)
*/


val tags = List(
    "american",
    "british",
    "classic pop and rock",
    "country",
    "english",
    "hip hop",
    "rock and indie",
    "rock",
    "uk"
)


val tag_predictions = tags.map(tag => {
    val model_save_location = "hdfs://ambalt1.sum.net:8020/tag_models/models/" + tag.replace(" ", "_") + "_model.model"
    val model = LogisticRegressionModel.load(sc, model_save_location)
    model.clearThreshold()
    val prediction = model.predict(one_song_features)
    
    (tag, prediction)
}).sortBy(_._2).reverse


println(extractTrackId(one_song_raw))
tag_predictions.foreach( prediction =>
    println(prediction._1 + ":\t\t" + prediction._2)
)