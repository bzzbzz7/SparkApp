package cn.spark.study.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Administrator
  */
object DailyTop3Keyword {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setMaster("local")
            .setAppName("DailyTop3Keyword")
        val sc = new SparkContext(conf)
        val hiveContext = new HiveContext(sc)


        var queryParaMap: Map[String, List[String]] = Map()
        queryParaMap += ("city" -> List("beijing"))
        queryParaMap += ("platform" -> List("android"))
        queryParaMap += ("version" -> List("1.0", "1.2", "1.5", "2.0"))

        val queryParaMapBroadcast = sc.broadcast(queryParaMap)


        val rawRdd = sc.textFile("./data/user/spark/data/keyword.txt")

        val filterRdd = rawRdd.filter {
            line => {
                val queryParaMap = queryParaMapBroadcast.value

                val arr = line.split('\t')
                if (!queryParaMap.get("city").get.contains(arr(3))) {
                    false
                }
                if (!queryParaMap.get("platform").get.contains(arr(4))) {
                    false
                }
                if (!queryParaMap.get("version").get.contains(arr(5))) {
                    false
                }
                true
            }
        }
        //映射格式 日期_关键词， 用户
        val mapRdd = filterRdd.map {
            line => {
                val arr = line.split('\t')
                (arr(0) + "_" + arr(2), arr(1))
            }
        }
        //去重
        val dateKeywordUvRdd = mapRdd.distinct().groupByKey().map(keyword => (keyword._1, keyword._2.size))


        //转为DF
        val dateKeywordUvRowRdd = dateKeywordUvRdd.map(keyword => Row(keyword._1.split("_")(0), keyword._1.split("_")(1), keyword._2))
        val schema = StructType(
            List(
                StructField("date", StringType, true),
                StructField("keyword", StringType, true),
                StructField("uv", IntegerType, true)
            )
        )
        val dateKeywordUvDF = hiveContext.createDataFrame(dateKeywordUvRowRdd, schema)

        //sql 开窗函数 查找每日top3uv
        dateKeywordUvDF.registerTempTable("daily_keyword_uv")
        val dailyTop3KeywordDF = hiveContext.sql(""
            + "SELECT date,keyword,uv "
            + "FROM ("
            + "SELECT date,keyword,uv,row_number() OVER (PARTITION BY date ORDER BY uv DESC) rank "
            + "FROM daily_keyword_uv"
            + ") tmp "
            + "WHERE rank<=3")
        dailyTop3KeywordDF.show()

        //top3uv 总数

        //
        //        hiveContext.sql("drop table if exists sales")
        //        hiveContext.sql("create table if not exists " +
        //            "sales(product string, category string, revenue bigint) " +
        //            "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ")
        //
        //        hiveContext.sql("load data local inpath './data/user/spark/data/sales.txt' into table sales ")
        //        val top3SalesDF = hiveContext.sql("select  * from (" +
        //            "select product, category, revenue, row_number() over(partition by category order by revenue desc) rank from sales " +
        //            ") tmp_sales where rank <=3")
        //
        //        hiveContext.sql("drop table if exists top3_sales")
        //
        //        top3SalesDF.write.saveAsTable("top3_sales")
        //
        //        top3SalesDF.show()


    }

}