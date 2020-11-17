package com.zhengzhou.spark.app

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafakaDirectTest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("KafkaDirectWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))

    val topicsSet = Set("topic-zz-test")

//    "key.deserializer" -> classOf[StringDeserializer],
//    "value.deserializer" -> classOf[StringDeserializer],
//    "group.id" -> "use_a_separate_group_id_for_each_stream",
//    "auto.offset.reset" -> "latest",
//    "enable.auto.commit" -> (false: java.lang.Boolean)

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> "slave1:9092,slave2:9092,slave3:9092,slave4:9092,",
      "group.id" -> "group-zz",
      "auto.offset.reset" -> "smallest")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    val lines = messages.map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate

  }


}
