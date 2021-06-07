package operate

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//TODO 将数据扁平化
object Rdd_FlatMap {
  def main(args: Array[String]): Unit = {
    //map算子  对RDD元素进行转换 RDD原结构不变 有多少元素循环多少次 就有多少次网络传输 效率较低
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Map")
    //创建spark上下文对象
    val sc = new SparkContext(config)
    val rdd: RDD[String] = sc.makeRDD(Array("Scala Spark Hbase", "Hive Sqoop Kafak", "Flink", "Flink Hive Java"))
    print(rdd.collect().mkString(","))
    //传入RDD的每个元素  返回可迭代的集合  扁平化操作
    val rdd1: RDD[String] = rdd.flatMap(_.split(" "))
    print(rdd1.collect().mkString(","))
     util.ArrayList






  }
}
