package operate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
 TODO
    distinct算子:存在shuffle有磁盘读写存在 效率较低 数据去重
 */
object Rdd_Distinct {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Map")
    //创建spark上下文对象
    val sc = new SparkContext(config)

    val rdd: RDD[Int] = sc.makeRDD(Array(2, 3, 4, 1, 2, 2, 3, 4, 5, 6, 7, 7, 7), 4)
    val rdd1: RDD[Int] = rdd.distinct()
    rdd1.collect().foreach(println)
  }
}
