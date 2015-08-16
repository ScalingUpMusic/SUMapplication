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
    val track_id = line.split("'")(1)
    return track_id
}


val hdfs_path = "hdfs://ambalt1.sum.net:8020/"
val track_info_path = "file:///gpfs/AdditionalFiles/unique_tracks.txt"



// TODO: Need options for selecting the song to be classified

// Get a random list of songs. Pick a letter...
val dir_letter = (Random.nextInt(26) + 65).toChar
val file_path  = hdfs_path + "h52text/[" + dir_letter + "-" + dir_letter + "]"

// Grab a random song from that letter's directory, and get its audio features
val one_song_raw = sc.textFile(file_path).sample(false, 0.001).take(1).mkString("")
val one_song_features = extractData(one_song_raw)
val track_id = extractTrackId(one_song_raw)

// Get additional song data for display
val track_info_file = sc.textFile(track_info_path).filter(line => line.contains(track_id)).collect()
val track_info = track_info_file(0).split("<SEP>")

// Get list of tags
val tag_popularity_cutoff = 30000
val tag_list = sc.textFile(hdfs_path + "tags")
val clean_tag_list = tag_list.map(tag_count => "['()']".r.replaceAllIn(tag_count, ""))
val tags = clean_tag_list.map(tag_count => tag_count.split(",")).filter(tag_count => tag_count(1).toInt >= tag_popularity_cutoff).map(tag_count => tag_count(0))



/*
The code below SHOULD be parallelized by removing the collect() and calling the initial
map() on the tags RDD, but Spark throws this very strange error:

15/08/15 23:55:02 ERROR Executor: Exception in task 1.0 in stage 21.0 (TID 28)
java.lang.NullPointerException
	at org.apache.spark.mllib.util.Loader$.loadMetadata(modelSaveLoad.scala:125)
	at org.apache.spark.mllib.classification.LogisticRegressionModel$.load(LogisticRegression.scala:171)
*/

val tag_predictions = tags.collect.map(tag => {
    val model_save_location = "hdfs://ambalt1.sum.net:8020/tag_models/models/" + tag.replace(" ", "_") + "_model.model"
    val model = LogisticRegressionModel.load(sc, model_save_location)
    model.clearThreshold()
    val prediction = model.predict(one_song_features)
    
    (tag, prediction)
}).sortBy(_._2).reverse


println("TRACK ID: " + track_info(0))
println("ARTIST: " + track_info(2))
println("SONG TITLE: " + track_info(3))
println("GENRE TAG               | EST. CONFIDENCE")
println("------------------------+----------------------------")

tag_predictions.foreach( prediction => {
    val tag_name = prediction._1
    val probability = prediction._2
    println(f"$tag_name%-23s | $probability%s")
})

