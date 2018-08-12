package cn.spark.study.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

/**
  * @author Administrator
  */
object HDFSWordCount {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setMaster("local[2]")
            .setAppName("HDFSWordCount")
        val ssc = new StreamingContext(conf, Seconds(2))

        val lines = ssc.textFileStream("hdfs://hadoop.zhengzhou.com:8020/user/spark/data/wordcount_dir")
        val words = lines.flatMap {
            _.split(",")
        }
        val pairs = words.map { word => (word, 1) }
        val wordCounts = pairs.reduceByKey(_ + _)

        wordCounts.print()

        ssc.start()
        ssc.awaitTermination()
    }
}