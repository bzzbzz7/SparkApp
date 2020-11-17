package com.zhengzhou.spark.app

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}

object MyTest {

//inputFile = "hdfs://hadoop.zhengzhou.com:8020/user/zhengzhou/mapreduce/wordcount/word.txt"
//outputDir = "hdfs://hadoop.zhengzhou.com:8020/user/zhengzhou/mapreduce/wordcount/out/"


  def main(args: Array[String]) {

    if(args.length<2){
      return 1;
    }
    val inputFile = args(0);
    val outputDir = args(1);

    val conf = new SparkConf().setAppName("zz Word Count App")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile(inputFile)
    val wordcount = rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    wordcount.foreach(wc => println(wc._1 + " appeared " + wc._2 + " times."))
    //        wordcount.saveAsTextFile("hdfs://hadoop.zhengzhou.com:8020/user/zhengzhou/mapreduce/wordcount/out/" + new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(new Date()))
    wordcount.saveAsTextFile(outputDir + new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(new Date()))

    sc.stop()
  }

}
