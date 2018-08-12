package com.zhengzhou.spark.app

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafakaDirectTest {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[2]").setAppName("KafkaDirectWordCount")
        val ssc = new StreamingContext(conf, Seconds(5))

        val topicsSet = Set("test")
        val kafkaParams = Map[String, String]("metadata.broker.list" -> "hadoop.zhengzhou.com:9092")
        val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
        val lines = messages.map(_._2)
        val words = lines.flatMap(_.split(" "))
        val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
        wordCounts.print()

        ssc.start() // Start the computation
        ssc.awaitTermination() // Wait for the computation to terminate

    }


}
