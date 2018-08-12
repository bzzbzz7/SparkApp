package cn.spark.study.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * @author Administrator
  */
object KafkaWordCount {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setMaster("local[2]")
            .setAppName("KafkaWordCount")
        val ssc = new StreamingContext(conf, Seconds(2))

        var topicThreadMap = Map("WordCount" -> 1)

        val lines = KafkaUtils.createStream(ssc, "hadoop.zhengzhou.com", "DefaultConsumerGroup", topicThreadMap)


        val words = lines.flatMap { line => line._2.split(" ") }
        val pairs = words.map { word => (word, 1) }
        val wordCounts = pairs.reduceByKey(_ + _)

        wordCounts.print()

        ssc.start()
        ssc.awaitTermination()
    }

}