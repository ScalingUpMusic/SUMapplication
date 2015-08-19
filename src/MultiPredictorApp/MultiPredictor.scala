import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.classification.{LogisticRegressionWithSGD, LogisticRegressionWithLBFGS, LogisticRegressionModel}
import org.apache.spark.mllib.classification.{SVMModel,SVMWithSGD}

import scala.util.Random


object MultiPredictorApp {
    
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("Multi Predictor")
        val sc = new SparkContext(conf)
        
        val tag_popularity_cutoff = 1000
        val hdfs_path = "hdfs://ambalt1.sum.net:8020/"
        val track_info_path = "file:///gpfs/AdditionalFiles/unique_tracks.txt"
        
        
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
        val tag_list = sc.textFile(hdfs_path + "tags")
        val clean_tag_list = tag_list.map(tag_count => "['()']".r.replaceAllIn(tag_count, ""))
        val tags = clean_tag_list.map(tag_count => tag_count.split(",")).filter(tag_count => tag_count(1).toInt >= tag_popularity_cutoff).map(tag_count => tag_count(0))
        
        
        
        // Evaluate the audio features against each model and generate a list of predictions
        val tag_predictions = tags.collect.map(tag => {
            val model_save_location = "hdfs://ambalt1.sum.net:8020/tag_models/models/" + tag.replace(" ", "_").replace("\\", "") + "_model.model"
            
	    println(tag)
            val model = SVMModel.load(sc, model_save_location)
            model.clearThreshold()
            val prediction = model.predict(one_song_features)
            
            (tag, prediction)
        }).sortBy(_._2).reverse
        
        
        println("TRACK ID: " + track_info(0))
        println("ARTIST: " + track_info(2))
        println("SONG TITLE: " + track_info(3))
        println("GENRE TAG                    | SVM MARGIN")
        println("-----------------------------+-----------------------------")
        
        tag_predictions.foreach( prediction => {
            val tag_name = prediction._1
            val probability = prediction._2
            println(f"$tag_name%-28s | $probability%s")
        })
    }
    
    
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
}

