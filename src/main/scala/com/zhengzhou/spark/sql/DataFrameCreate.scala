package com.zhengzhou.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object DataFrameCreate {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("spark://hadoop.zhengzhou.com:7077").setAppName("DataFrameCreate")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)

        val df = sqlContext.read.json("hdfs://hadoop.zhengzhou.com:8020/user/spark/data/students.json")
        df.show()
    }
}
