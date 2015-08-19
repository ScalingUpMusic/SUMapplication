// to run as of Aug 8
// ssh to spark master (ambari2)
// cd to SUMapplication/src/OneTagApp
// sbt package (to build)
// find . -name \*.jar (find the jar file)
// find . -name \*one-tag-model*.jar
// /usr/hdp/current/spark-client/bin/spark-submit --class "OneTagModel" $(find . -name \*one-tag-model*.jar) <tag>

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.classification.{LogisticRegressionWithSGD, LogisticRegressionWithLBFGS, LogisticRegressionModel}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics

object MultiTagModel {
    val tagPopularityCutoff = 1000
    val verbose = true
    val hdfspath = "hdfs://ambalt1.sum.net:8020/"
    val datapath = hdfspath + "h52text/"
    val time_dir = "run_times/"
    val dir_start = "A"
    val dir_end = "Z"
    val model_dir = "tag_models/"
    val model_subdir = "models/"
    val rslt_dir = "results/"
    val loadpath = datapath + "[" + dir_start + "-" + dir_end + "]"
    // "svm" or "logistic"
    val model_type = "svm"
    val model_intercept = true

    // get data
    def extractData(line:String, tag:String) : (Double, Vector) = {
        val array_string = "\\[.*?\\]".r
        val ars = (array_string findAllIn line).toList
        val hastag = if (ars(0) contains tag) 1.0 else 0.0
        val data = (ars(1) replaceAll ("[\\[\\]\\s]", "")).split(",").map(x => x.toDouble)
        return (hastag, Vectors.dense(data))
    }

    def trainModel(tag: String, sc: SparkContext) {
        
        val sub_start_time = System.currentTimeMillis()/1000
        val hadoopConf = new org.apache.hadoop.conf.Configuration()
        val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(hdfspath), hadoopConf)
        val cleanTag = tag.replace(" ", "_").replace("\\", "")
        val model_save_path = hdfspath + model_dir + model_subdir + cleanTag + "_model.model"

        if(!(hdfs.exists(new org.apache.hadoop.fs.Path(model_save_path)))){
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
                println("VERBOSE - Number of Tracks:\n" + n.toString)
                println("VERBOSE - Percent Have Tag '" + tag +  "':\n" + (ntag/n).toString)
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
                    new LogisticRegressionWithLBFGS().setIntercept(model_intercept).run(train)  
                }

            // Compute raw scores on the test set.
            val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
                    val prediction = model.predict(features)
                    (prediction, label)
                }

            val metrics = new BinaryClassificationMetrics(predictionAndLabels)
            val auroc = metrics.areaUnderROC
            val aupr = metrics.areaUnderPR
            val auroc_string = "Area Under ROC = " + auroc
            val aupr_string = "Area Under Precision-Recall = " +  aupr
            val tot_tm = "Total time: " + (System.currentTimeMillis()/1000 - sub_start_time)
            val rsltStringArr = Array[String](auroc_string, aupr_string, tot_tm)
            val distRsltStringArr = sc.parallelize(rsltStringArr)
            val rslt_save_path = hdfspath + model_dir + rslt_dir + cleanTag
            try { hdfs.delete(new org.apache.hadoop.fs.Path(rslt_save_path), true) } catch { case _ : Throwable => { } }
            distRsltStringArr.saveAsTextFile(rslt_save_path)
            try { hdfs.delete(new org.apache.hadoop.fs.Path(model_save_path), true) } catch { case _ : Throwable => { } }
            model.save(sc, model_save_path)
        }
    }

    def main(args: Array[String]){
        val start_time = System.currentTimeMillis()/1000
        val conf = new SparkConf().setAppName("MultiTagModel")
        val sc = new SparkContext(conf)
        val tagList = sc.textFile(hdfspath + "tags")
        val cleanTagList = tagList.map(tagCnt => "['()']".r.replaceAllIn(tagCnt, ""))
        val topTagsList = cleanTagList.map(tagCnt => tagCnt.split(",")).filter(tagCnt => tagCnt(1).toInt >= tagPopularityCutoff).map(tagCnt => tagCnt(0))
        topTagsList.collect().foreach(tag => trainModel(tag, sc))
        val time_save_path = hdfspath + time_dir + "multiTagTime_start_at_" + start_time.toString
        val tot_time = sc.parallelize(Array((System.currentTimeMillis()/1000 - start_time).toString))
        tot_time.saveAsTextFile(time_save_path)
    }

}