package com.zhengzhou.spark.app

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils

object KafakaTest {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[2]").setAppName("KafkaWordCount")
        val ssc = new StreamingContext(conf, Seconds(5))

        val topicMap = Map("test" -> 1)

        val lines = KafkaUtils.createStream(ssc, "hadoop.zhengzhou.com:2181", "testWordCountGroup", topicMap).map(_._2)
        val words = lines.flatMap(_.split(" "))
        val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
        wordCounts.print()

        ssc.start() // Start the computation
        ssc.awaitTermination() // Wait for the computation to terminate

    }
}
