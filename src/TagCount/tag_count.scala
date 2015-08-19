import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object TagCount {

    def main(args: Array[String]) {
        val start_time = System.currentTimeMillis()/1000
        val conf = new SparkConf().setAppName("TagCount")
        val sc = new SparkContext(conf)
        val hdfspath = "hdfs://ambalt1.sum.net:8020/"
        val hadoopConf = new org.apache.hadoop.conf.Configuration()
        val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(hdfspath), hadoopConf)
        val datapath = hdfspath + "h52text/"
        val dir_start = "A"
        val dir_end = "Z"
        val time_dir = "run_times/"
        val loadpath = datapath + "[" + dir_start + "-" + dir_end + "]"
        val array_string = "\\[.*?\\]".r
        val songData = sc.textFile(loadpath)
        val songTags = songData.map(line => (array_string findAllIn line).toList(0))
        val tagInstances = songTags.flatMap(tags => tags.split(", ")
            .filter(tag => tag != "[]")
            .map(tag => (tag.replace("[", "").replace("]", ""), 1)))
        val tagCounts = tagInstances.reduceByKey(_+_)
        val sortedTagCounts = tagCounts.sortBy(_._2, false)
        val rslt_save_path = hdfspath + "tags"
        try { hdfs.delete(new org.apache.hadoop.fs.Path(rslt_save_path), true) } catch { case _ : Throwable => { } }
        sortedTagCounts.saveAsTextFile(rslt_save_path)
        val time_save_path = hdfspath + time_dir + "tagCountTime_start_at_" + start_time.toString
        val tot_time = sc.parallelize(Array((System.currentTimeMillis()/1000 - start_time).toString))
        tot_time.saveAsTextFile(time_save_path)

    }
}