import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SummarizeModelPerformance {
    def mean(listie: Iterable[Double])(implicit num: Numeric[Double]) = {
        num.toDouble(listie.sum) / listie.size
    }

    def main(args: Array[String]){
        val hdfspath = "hdfs://ambalt1.sum.net:8020/"
        val model_dir = "tag_models/"
        val model_subdir = "models/"
        val rslt_dir = "results/*"
        val rslts_path = hdfspath + model_dir + rslt_dir
        val conf = new SparkConf().setAppName("SummarizeModelPerformance")
        val sc = new SparkContext(conf)
        val modelResults = sc.textFile(rslts_path)
        val resultsArr = modelResults.map(line => line.split(" = "))
        val auroc = resultsArr.filter(line => line(0) == "Area Under ROC")
        val auprc = resultsArr.filter(line => line(0) == "Area Under Precision-Recall")
        val auroc_vals = auroc.map(line => line(1).toDouble).collect()
        val auprc_vals = auprc.map(line => line(1).toDouble).collect()
        val mean_auroc = mean(auroc_vals)
        val mean_auprc = mean(auprc_vals)
        println("Mean AUROC: " + mean_auroc.toString)
        println("Mean AUPRC: " + mean_auprc.toString)
    }
}