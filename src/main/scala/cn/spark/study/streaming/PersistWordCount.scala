package cn.spark.study.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author Administrator
  */
object PersistWordCount {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setMaster("local[2]")
            .setAppName("UpdateStateByKeyWordCount")
        val ssc = new StreamingContext(conf, Seconds(5))
        ssc.checkpoint("hdfs://hadoop.zhengzhou.com:8020/user/spark/data/wordcount_checkpoint")

        val lines = ssc.socketTextStream("hadoop.zhengzhou.com", 9999)
        val words = lines.flatMap {
            _.split(" ")
        }
        val pairs = words.map { word => (word, 1) }
        val wordCounts = pairs.updateStateByKey(
            (values: Seq[Int], state: Option[Int]) => {
                var newValue = state.getOrElse(0)
                for (value <- values) {
                    newValue += value
                }
                Option(newValue)
            })

        wordCounts.print()

        //持久化
        ssc.start()
        ssc.awaitTermination()
    }

}