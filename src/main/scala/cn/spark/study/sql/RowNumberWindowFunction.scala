package cn.spark.study.sql

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Administrator
  */
object RowNumberWindowFunction {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setMaster("local")
            .setAppName("RowNumberWindowFunction")
        val sc = new SparkContext(conf)
        val hiveContext = new HiveContext(sc)

        // 这里着重说明一下！！！
        // 要使用Spark SQL的内置函数，就必须在这里导入SQLContext下的隐式转换
        import hiveContext.implicits._

        hiveContext.sql("drop table if exists sales")
        hiveContext.sql("create table if not exists " +
            "sales(product string, category string, revenue bigint) " +
            "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ")

        hiveContext.sql("load data local inpath './data/user/spark/data/sales.txt' into table sales ")
        val top3SalesDF = hiveContext.sql("select  * from (" +
            "select product, category, revenue, row_number() over(partition by category order by revenue desc) rank from sales " +
            ") tmp_sales where rank <=3")

        hiveContext.sql("drop table if exists top3_sales")

        top3SalesDF.write.saveAsTable("top3_sales")

        top3SalesDF.show()


    }

}