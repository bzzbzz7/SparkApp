package cn.spark.study.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author Administrator
  */
object KafkaDirectWordCount {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setMaster("local[2]")
            .setAppName("KafkaWordCount")
        val ssc = new StreamingContext(conf, Seconds(2))

        val topicThreadMap = Map("WordCount" -> 1)
        val kafkaParams = Map("metadata.broker.list" -> "hadoop.zhengzhou.com:9092")
        val topicSet = Set("WordCount")

        val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)


        val words = lines.flatMap { line => line._2.split(" ") }
        val pairs = words.map { word => (word, 1) }
        val wordCounts = pairs.reduceByKey(_ + _)

        wordCounts.print()

        ssc.start()
        ssc.awaitTermination()
    }

}