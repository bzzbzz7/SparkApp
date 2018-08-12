package com.zhengzhou.spark.app

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}

object MyTest {

    def main(args: Array[String]) {
        val logFile = "hdfs://hadoop.zhengzhou.com:8020/user/zhengzhou/mapreduce/wordcount/word.txt"

        val conf = new SparkConf().setAppName("Word Count App").setMaster("local")
        val sc = new SparkContext(conf)

        val rdd = sc.textFile(logFile)
        val wordcount = rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

        wordcount.saveAsTextFile("hdfs://hadoop.zhengzhou.com:8020/user/zhengzhou/mapreduce/wordcount/out/" + new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(new Date()))

        sc.stop()
    }

}
