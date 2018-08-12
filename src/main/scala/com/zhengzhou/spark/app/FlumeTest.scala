package com.zhengzhou.spark.app

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.flume.FlumeUtils // not necessary in Spark 1.3+

object FlumeTest {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[2]").setAppName("FlumeWordCount")
        val ssc = new StreamingContext(conf, Seconds(1))

        val flumeStream = FlumeUtils.createStream(ssc, "hadoop.zhengzhou.com", 9099)

        flumeStream.count().map(cnt => "Received " + cnt + " flume events.").print()

        ssc.start() // Start the computation
        ssc.awaitTermination() // Wait for the computation to terminate

    }
}
