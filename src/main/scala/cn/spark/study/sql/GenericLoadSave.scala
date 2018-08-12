package cn.spark.study.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext


/**
  * @author Administrator
  */
object GenericLoadSave {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setAppName("GenericLoadSave")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)

        val usersDF = sqlContext.read.load("hdfs://hadoop.zhengzhou.com:8020/users.parquet")
        usersDF.write.save("hdfs://hadoop.zhengzhou.com:8020/namesAndFavColors_scala")
    }

}