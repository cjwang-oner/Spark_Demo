package operate_kv

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * TODO 根据key做聚合 必然存在shuffle
 */
object Rdd_GroupByKey {
  def main(args: Array[String]): Unit = {
    // TODO map算子  对RDD元素进行转换 RDD原结构不变 有多少元素循环多少次 就有多少次网络传输 效率较低
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Map")
    //TODO 创建spark上下文对象
    val sc = new SparkContext(config)
    val rdd: RDD[String] = sc.makeRDD(Array("scala", "spark", "scala", "hbase", "hive", "spark", "hadoop", "hbase","scala","spark"))
    val rdd2: RDD[(String, Int)] = rdd.map((_, 1))
    val rdd3: RDD[(String, Iterable[Int])] = rdd2.groupByKey()
    val rdd4: RDD[(String, Int)] = rdd3.map((x => {
      (x._1, x._2.sum)
    }))
    rdd4.collect().foreach(println)
  }
}
