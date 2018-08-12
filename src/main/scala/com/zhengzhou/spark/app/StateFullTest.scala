package com.zhengzhou.spark.app

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StateFullTest {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[2]").setAppName("StatefulNetworkWordCount")
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        ssc.checkpoint(".")

        val topicsSet = Set("test")
        val kafkaParams = Map[String, String]("metadata.broker.list" -> "hadoop.zhengzhou.com:9092")

        val updateFunc = (values: Seq[Int], state: Option[Int]) => {
            val currentCount = values.sum
            val previousCount = state.getOrElse(0)
            Some(currentCount + previousCount)
        }

        val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

        val lines = messages.map(_._2)
        val words = lines.flatMap(_.split(" "))
        val wordDstream = words.map(x => (x, 1))

        val stateDstream = wordDstream.updateStateByKey[Int](updateFunc)

        stateDstream.print()

        ssc.start()
        ssc.awaitTermination()

    }
}
