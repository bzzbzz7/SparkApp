package com.zhengzhou.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}

object RddTest {
    def main(args: Array[String]) {

        val conf = new SparkConf().setMaster("local").setAppName("rdd")
        val sc = new SparkContext(conf)

        val rddInt: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5), 1)

        val rddAggr1: (Int, Int) = rddInt.aggregate((0, 0))((x, y) => (x._1 + y, x._2 + 1), (x, y) => (x._1 + y._1, x._2 + y._2))
        println("====aggregate 1====:" + rddAggr1.toString()) // (15,5)

        sc.stop()
    }
}
