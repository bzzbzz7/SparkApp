package com.zhengzhou.spark.app

//import com.typesafe.config.ConfigFactory
import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import net.ipip.ipdb.City
import org.apache.avro.generic.GenericRecord
import org.apache.log4j.Logger
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object KafkaAvroPvuvRay {

  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger(KafkaAvroPvuvRay.getClass)

    val conf = new SparkConf().setMaster("local[2]").setAppName("KafkaWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))

    logger.info("================================" )
    val stream = this.getClass().getClassLoader().getResourceAsStream("ipipfree_201901.ipdb")
    val ipdb = new City(stream)
    logger.info("after init City" )
//    val ipdbBroadcast = ssc.sparkContext.broadcast(ipdb)
    logger.info("after broacast City" )



    val topicMap = Map("test" -> 1)

    val lines = KafkaUtils.createStream(ssc, "hadoop.zhengzhou.com:2181", "testWordCountGroup", topicMap).map(_._2)




    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate

  }
}