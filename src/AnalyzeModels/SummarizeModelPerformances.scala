import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io._

object SummarizeModelPerformance {
    def mean(listie: Iterable[Double])(implicit num: Numeric[Double]) = {
        num.toDouble(listie.sum) / listie.size
    }

    def tagStats(tagAndCnt: Array[String], rsltDir: String, sc: SparkContext) : Array[String] = {
        var rtnArray = tagAndCnt
        try {
            val tag = tagAndCnt(0)
            val cleanTag = tag.replace(" ", "_").replace("\\", "")
            val rsltPath = rsltDir + cleanTag
            val rsltFile = sc.textFile(rsltPath).collect()
            val auroc = rsltFile(0).split(" = ")(1)
            val auprc = rsltFile(1).split(" = ")(1)
            val trainTime = rsltFile(2).split(": ")(1)
            rtnArray = rtnArray :+ auroc
            rtnArray = rtnArray :+ auprc
            rtnArray = rtnArray :+ trainTime
        }
        catch { case _ : Throwable => { } }
        rtnArray
    }

    def writeToFile(pw: PrintWriter, rsltArr: Array[String]){
        val rsltString = rsltArr.mkString(",")
        pw.write(rsltString + "\n")
    }

    def main(args: Array[String]){
        val hdfspath = "hdfs://ambalt1.sum.net:8020/"
        val model_dir = "tag_models/"
        val rslt_dir = "results/"
        val rslts_path = hdfspath + model_dir + rslt_dir
        val conf = new SparkConf().setAppName("SummarizeModelPerformance")
        val sc = new SparkContext(conf)
        // val modelResults = sc.wholeTextFiles(rslts_path)
        val modelResults = sc.textFile(rslts_path + "*")
        // val fileNames = modelResults.map(line => line._1)
        val resultsArr = modelResults.map(line => line.split(" = "))
        // val resultsArr = modelResults.map(line => line._2.split(" = "))
        val auroc = resultsArr.filter(line => line(0) == "Area Under ROC")
        val auprc = resultsArr.filter(line => line(0) == "Area Under Precision-Recall")
        val auroc_vals = auroc.map(line => line(1).toDouble).collect()
        val auprc_vals = auprc.map(line => line(1).toDouble).collect()
        val mean_auroc = mean(auroc_vals)
        val mean_auprc = mean(auprc_vals)
        println("Mean AUROC: " + mean_auroc.toString)
        println("Mean AUPRC: " + mean_auprc.toString)
        val tagPopularityCutoff = 1000
        val tagList = sc.textFile(hdfspath + "tags")
        val cleanTagList = tagList.map(tagCnt => "['()']".r.replaceAllIn(tagCnt, ""))
        val topTags = cleanTagList.map(tagCnt => tagCnt.split(",")).filter(tagCnt => tagCnt(1).toInt >= tagPopularityCutoff).collect()
        val rsltsByTag = topTags.map(tagAndCnt => tagStats(tagAndCnt, rslts_path, sc)).filter(arr => arr.length==5)
        println(rsltsByTag.length)
        val header = Array("Genre", "Song count", "AUROC", "AUPRC", "Time to Train")
        val fileContents = header +: rsltsByTag
        val pw = new PrintWriter(new File("/home/spark/result_summary.csv" ))
        fileContents.foreach(fc => writeToFile(pw, fc))
        pw.close()

    }
}